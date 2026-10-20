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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

/** Per-operation metrics for the authenticated NFS backend RPC service. */
final class NfsGatewayMetrics implements MetricsSource, ServerInterceptor, Closeable {
  static final String SOURCE_NAME = "NfsGatewayMetrics";
  private static final Set<String> OPERATIONS = ImmutableSet.of(
      "Capabilities", "Getattr", "Lookup", "Readdir", "Read", "Create", "Write", "Commit", "Mkdir",
      "Remove", "Rename");

  private final String sourceName;
  private final Map<String, OperationMetrics> operationMetrics = new ConcurrentHashMap<>();

  NfsGatewayMetrics() {
    sourceName = DefaultMetricsSystem.inMiniClusterMode()
        ? SOURCE_NAME + '-' + UUID.randomUUID() : SOURCE_NAME;
    OPERATIONS.forEach(name -> operationMetrics.put(name, new OperationMetrics()));
    MetricsSystem system = DefaultMetricsSystem.instance();
    system.register(sourceName, "NFS Gateway backend RPC metrics", this);
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    String operation = call.getMethodDescriptor().getBareMethodName();
    if (!OPERATIONS.contains(operation)) {
      return next.startCall(call, headers);
    }
    long startNanos = System.nanoTime();
    int[] nfsStatus = {-1};
    ServerCall<ReqT, RespT> measuredCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      @Override
      public void sendMessage(RespT message) {
        nfsStatus[0] = getNfsStatus(message);
        super.sendMessage(message);
      }

      @Override
      public void close(Status status, Metadata trailers) {
        operationMetrics.get(operation).record(System.nanoTime() - startNanos, nfsStatus[0], status);
        super.close(status, trailers);
      }
    };
    return next.startCall(measuredCall, headers);
  }

  private static int getNfsStatus(Object response) {
    if (!(response instanceof Message)) {
      return -1;
    }
    Message message = (Message) response;
    FieldDescriptor resultField = message.getDescriptorForType().findFieldByName("result");
    if (resultField == null || !message.hasField(resultField)) {
      return -1;
    }
    Message result = (Message) message.getField(resultField);
    FieldDescriptor statusField = result.getDescriptorForType().findFieldByName("nfs_status");
    return statusField == null || !result.hasField(statusField) ? -1 : (Integer) result.getField(statusField);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder record = collector.addRecord(sourceName);
    operationMetrics.forEach((name, metrics) -> metrics.snapshot(record, name));
  }

  @Override
  public void close() {
    DefaultMetricsSystem.instance().unregisterSource(sourceName);
  }

  String getSourceName() {
    return sourceName;
  }

  private static final class OperationMetrics {
    private final LongAdder requests = new LongAdder();
    private final LongAdder nfsStatusErrors = new LongAdder();
    private final LongAdder transportFailures = new LongAdder();
    private final LongAdder latencyNanos = new LongAdder();

    private void record(long elapsedNanos, int nfsStatus, Status grpcStatus) {
      requests.increment();
      latencyNanos.add(elapsedNanos);
      if (nfsStatus > 0) {
        nfsStatusErrors.increment();
      }
      if (!grpcStatus.isOk()) {
        transportFailures.increment();
      }
    }

    private void snapshot(MetricsRecordBuilder record, String operation) {
      long count = requests.sum();
      String prefix = "Num" + operation;
      record.addCounter(Interns.info(prefix + "Ops", "Number of " + operation + " RPCs"), count);
      record.addCounter(Interns.info(prefix + "NfsStatusErrors", "NFS status errors for " + operation),
          nfsStatusErrors.sum());
      record.addCounter(Interns.info(prefix + "GrpcFailures", "gRPC failures for " + operation),
          transportFailures.sum());
      record.addGauge(Interns.info(operation + "LatencyNsAvgTime", "Average " + operation + " RPC latency"),
          count == 0 ? 0 : latencyNanos.sum() / count);
    }
  }
}
