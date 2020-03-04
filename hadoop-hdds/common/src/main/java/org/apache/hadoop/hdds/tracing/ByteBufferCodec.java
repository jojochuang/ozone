/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.tracing;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import io.jaegertracing.internal.JaegerObjectFactory;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.exceptions.EmptyTracerStateStringException;
import io.jaegertracing.spi.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A jaeger codec to save the current tracing context as a bytebuffer.
 */
public class ByteBufferCodec implements Codec<ByteBuffer> {

  public static final Logger LOG  = LoggerFactory.getLogger(ByteBufferCodec.class);

  private JaegerObjectFactory objectFactory = new JaegerObjectFactory();

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  @Override
  public JaegerSpanContext extract(ByteBuffer buf) {
    if (buf == null) {
      throw new EmptyTracerStateStringException();
    }

    Map<String, String> baggage = null;

    long traceIdHigh = buf.getLong();
    long traceIdLow = buf.getLong();
    long spanId = buf.getLong();
    long parentId = buf.getLong();
    byte flags = buf.get();
    int count = buf.getInt();

    if (count > 0) {
      baggage = new HashMap<String, String>(count);

      byte[] tmp = new byte[32];

      for (int i = 0; i < count; i++) {
        int len = buf.getInt();
        buf.get(tmp, 0, len);
        final String key = new String(tmp, 0, len, DEFAULT_CHARSET);

        len = buf.getInt();
        buf.get(tmp, 0, len);
        final String value = new String(tmp, 0, len, DEFAULT_CHARSET);

        baggage.put(key, value);
      }
    }

    return objectFactory.createSpanContext(
        traceIdHigh,
        traceIdLow,
        spanId,
        parentId,
        flags,
        baggage,
        null
    );
  }

  @Override
  public void inject(JaegerSpanContext context, ByteBuffer buf) {
    buf.putLong(context.getTraceIdHigh())
        .putLong(context.getTraceIdLow())
        .putLong(context.getSpanId())
        .putLong(context.getParentId())
        .put(context.getFlags());

    int pos = buf.position();
    buf.putInt(0);
    int baggageSize = 0;
    for (Map.Entry<String, String> entry : context.baggageItems()) {
      String key = entry.getKey();
      String item = entry.getValue();

      buf.putInt(key.length());
      buf.put(key.getBytes(DEFAULT_CHARSET));

      buf.putInt(item.length());
      buf.put(item.getBytes(DEFAULT_CHARSET));
      baggageSize++;
    }
    buf.position(pos);
    buf.putInt(baggageSize);
  }
}
