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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.nfs.NfsStatus;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayGrpc;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Result;
import org.junit.jupiter.api.Test;

class TestNfsGatewayMetrics {

  @Test
  void recordsNfsStatusAndGrpcFailuresForEachOperation() {
    NfsGatewayMetrics metrics = new NfsGatewayMetrics();
    try {
      record(metrics, NfsStatus.ACCESS.getCode(), Status.OK);
      record(metrics, NfsStatus.OK.getCode(), Status.UNAVAILABLE);

      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      metrics.getMetrics(collector, true);
      Map<String, Long> values = new HashMap<>();
      MetricsRecord record = collector.getRecords().get(0);
      for (AbstractMetric metric : record.metrics()) {
        values.put(metric.name(), ((Number) metric.value()).longValue());
      }
      assertEquals(2L, values.get("NumGetattrOps"));
      assertEquals(1L, values.get("NumGetattrNfsStatusErrors"));
      assertEquals(1L, values.get("NumGetattrGrpcFailures"));
      assertTrue(values.get("GetattrLatencyNsAvgTime") > 0);
    } finally {
      metrics.close();
    }
  }

  private static void record(NfsGatewayMetrics metrics, int nfsStatus, Status grpcStatus) {
    AtomicReference<ServerCall<GetattrRequest, GetattrResponse>> intercepted = new AtomicReference<>();
    MethodDescriptor<GetattrRequest, GetattrResponse> method = NfsGatewayGrpc.getGetattrMethod();
    ServerCall<GetattrRequest, GetattrResponse> call = new ServerCall<GetattrRequest, GetattrResponse>() {
      @Override
      public void request(int numMessages) {
      }

      @Override
      public void sendHeaders(Metadata headers) {
      }

      @Override
      public void sendMessage(GetattrResponse message) {
      }

      @Override
      public void close(Status status, Metadata trailers) {
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public MethodDescriptor<GetattrRequest, GetattrResponse> getMethodDescriptor() {
        return method;
      }

      @Override
      public Attributes getAttributes() {
        return Attributes.EMPTY;
      }

      @Override
      public String getAuthority() {
        return "localhost";
      }
    };
    ServerCallHandler<GetattrRequest, GetattrResponse> next = (measuredCall, headers) -> {
      intercepted.set(measuredCall);
      return new ServerCall.Listener<GetattrRequest>() { };
    };

    metrics.interceptCall(call, new Metadata(), next);
    intercepted.get().sendMessage(GetattrResponse.newBuilder()
        .setResult(Result.newBuilder().setNfsStatus(nfsStatus)).build());
    intercepted.get().close(grpcStatus, new Metadata());
  }
}
