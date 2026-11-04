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

package org.apache.hadoop.ozone.freon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

class TestFreonS3TraceContextRequestHandler {

  @Test
  void propagatesCurrentSpanAndPreservesRequest() {
    SdkHttpRequest request = SdkHttpRequest.builder().protocol("http").host("localhost")
        .method(SdkHttpMethod.PUT).putHeader("existing", "value").build();
    Context.ModifyHttpRequest context = mock(Context.ModifyHttpRequest.class);
    when(context.httpRequest()).thenReturn(request);
    SpanContext spanContext = SpanContext.create("0123456789abcdef0123456789abcdef", "0123456789abcdef",
        TraceFlags.getSampled(), TraceState.getDefault());
    try (Scope ignored = Span.wrap(spanContext).makeCurrent()) {
      SdkHttpRequest result = new FreonS3TraceContextRequestHandler()
          .modifyHttpRequest(context, new ExecutionAttributes());
      assertEquals("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
          result.firstMatchingHeader("traceparent").get());
      assertEquals("value", result.firstMatchingHeader("existing").get());
      assertEquals(request.getUri(), result.getUri());
      assertEquals(request.method(), result.method());
    }
  }

  @Test
  void leavesRequestUnchangedWithoutSpan() {
    SdkHttpRequest request = SdkHttpRequest.builder().protocol("http").host("localhost")
        .method(SdkHttpMethod.GET).build();
    Context.ModifyHttpRequest context = mock(Context.ModifyHttpRequest.class);
    when(context.httpRequest()).thenReturn(request);
    assertSame(request, new FreonS3TraceContextRequestHandler().modifyHttpRequest(context, new ExecutionAttributes()));
  }
}
