/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.tracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.lang.reflect.Proxy;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;

/**
 * Utility class to collect all the tracing helper methods.
 */
public final class TracingUtil {

  private static final String NULL_SPAN_AS_STRING = "";

  private static volatile boolean isInit = false;

  private TracingUtil() {
  }

  /**
   * Initialize the tracing with the given service name.
   */
  public static void initTracing(
      String serviceName, ConfigurationSource conf) {
    if (!GlobalTracer.isRegistered() && isTracingEnabled(conf)) {
      Configuration config = Configuration.fromEnv(serviceName);
      JaegerTracer tracer = config.getTracerBuilder()
          .registerExtractor(StringCodec.FORMAT, new StringCodec())
          .registerInjector(StringCodec.FORMAT, new StringCodec())
          .build();
      GlobalTracer.registerIfAbsent(tracer);
      isInit = true;
    }
  }

  /**
   * Export the active tracing span as a string.
   *
   * @return encoded tracing context.
   */
  public static String exportCurrentSpan() {
    return exportSpan(GlobalTracer.get().activeSpan());
  }

  /**
   * Export the specific span as a string.
   *
   * @return encoded tracing context.
   */
  public static String exportSpan(Span span) {
    if (span != null && isInit) {
      StringBuilder builder = new StringBuilder();
      GlobalTracer.get().inject(span.context(), StringCodec.FORMAT, builder);
      return builder.toString();
    }
    return NULL_SPAN_AS_STRING;
  }

  /**
   * Create a new scope and use the imported span as the parent.
   *
   * @param name          name of the newly created scope
   * @param encodedParent Encoded parent span (could be null or empty)
   *
   * @return OpenTracing scope.
   */
  public static Span importAndCreateSpan(String name, String encodedParent) {
    Tracer tracer = GlobalTracer.get();
    return tracer.buildSpan(name)
        .asChildOf(extractParent(encodedParent, tracer))
        .start();
  }

  private static SpanContext extractParent(String parent, Tracer tracer) {
    if (!GlobalTracer.isRegistered()) {
      return null;
    }

    if (parent == null || parent.isEmpty()) {
      return null;
    }

    return tracer.extract(StringCodec.FORMAT, new StringBuilder(parent));
  }

  /**
   * Creates a proxy of the implementation and trace all the method calls.
   *
   * @param delegate the original class instance
   * @param itf the interface which should be implemented by the proxy
   * @param <T> the type of the interface
   * @param conf configuration
   *
   * @return A new interface which implements interface but delegate all the
   * calls to the delegate and also enables tracing.
   */
  public static <T> T createProxy(
      T delegate, Class<T> itf, ConfigurationSource conf) {
    if (!isTracingEnabled(conf)) {
      return delegate;
    }
    Class<?> aClass = delegate.getClass();
    return itf.cast(Proxy.newProxyInstance(aClass.getClassLoader(),
        new Class<?>[] {itf},
        new TraceAllMethod<>(delegate, itf.getSimpleName())));
  }

  public static boolean isTracingEnabled(
      ConfigurationSource conf) {
    return conf.getBoolean(
        ScmConfigKeys.HDDS_TRACING_ENABLED,
        ScmConfigKeys.HDDS_TRACING_ENABLED_DEFAULT);
  }

  /**
   * Execute {@code runnable} inside an activated new span.
   */
  public static <E extends Exception> void executeInNewSpan(String spanName,
      CheckedRunnable<E> runnable) throws E {
    Span span = GlobalTracer.get()
        .buildSpan(spanName).start();
    executeInSpan(span, runnable);
  }

  /**
   * Execute {@code supplier} inside an activated new span.
   */
  public static <R, E extends Exception> R executeInNewSpan(String spanName,
      CheckedSupplier<R, E> supplier) throws E {
    Span span = GlobalTracer.get()
        .buildSpan(spanName).start();
    return executeInSpan(span, supplier);
  }

  /**
   * Execute {@code supplier} in the given {@code span}.
   * @return the value returned by {@code supplier}
   */
  private static <R, E extends Exception> R executeInSpan(Span span,
      CheckedSupplier<R, E> supplier) throws E {
    try (Scope ignored = GlobalTracer.get().activateSpan(span)) {
      return supplier.get();
    } catch (Exception ex) {
      span.setTag("failed", true);
      throw ex;
    } finally {
      span.finish();
    }
  }

  /**
   * Execute {@code runnable} in the given {@code span}.
   */
  private static <E extends Exception> void executeInSpan(Span span,
      CheckedRunnable<E> runnable) throws E {
    try (Scope ignored = GlobalTracer.get().activateSpan(span)) {
      runnable.run();
    } catch (Exception ex) {
      span.setTag("failed", true);
      throw ex;
    } finally {
      span.finish();
    }
  }

  /**
   * Execute a new function as a child span of the parent.
   */
  public static <E extends Exception> void executeAsChildSpan(String spanName,
      String parentName, CheckedRunnable<E> runnable) throws E {
    Span span = TracingUtil.importAndCreateSpan(spanName, parentName);
    executeInSpan(span, runnable);
  }

  /**
   * Create an active span with auto-close at finish.
   * <p>
   * This is a simplified way to use span as there is no way to add any tag
   * in case of Exceptions.
   */
  public static AutoCloseable createActivatedSpan(String spanName) {
    Span span = GlobalTracer.get().buildSpan(spanName).start();
    Scope scope = GlobalTracer.get().activateSpan(span);
    return () -> {
      scope.close();
      span.finish();
    };
  }
}
