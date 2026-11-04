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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class TestS3KeyGenerator {

  @Test
  void putsObjectWithConfiguredSize() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), any(RequestBody.class)))
        .thenAnswer(invocation -> {
          PutObjectRequest.Builder request = PutObjectRequest.builder();
          invocation.<Consumer<PutObjectRequest.Builder>>getArgument(0).accept(request);
          assertEquals("text/plain", request.build().contentType());
          return null;
        });
    S3KeyGenerator generator = generator(client, "--size", "128");
    generator.call();
    ArgumentCaptor<RequestBody> body = ArgumentCaptor.forClass(RequestBody.class);
    verify(client).putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(), body.capture());
    assertEquals(128, body.getValue().contentLength());
    // S3 Gateway lowercases Content-Type when validating signatures. Avoid an uppercase charset parameter.
    assertEquals("application/octet-stream", body.getValue().contentType());
  }

  @Test
  void completesMultipartUploadWithReturnedEtags() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.createMultipartUpload(ArgumentMatchers.<Consumer<CreateMultipartUploadRequest.Builder>>any()))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload").build());
    when(client.uploadPart(ArgumentMatchers.<Consumer<UploadPartRequest.Builder>>any(), any(RequestBody.class)))
        .thenAnswer(invocation -> {
          Consumer<UploadPartRequest.Builder> consumer = invocation.getArgument(0);
          UploadPartRequest.Builder builder = UploadPartRequest.builder();
          consumer.accept(builder);
          UploadPartRequest request = builder.build();
          assertEquals("upload", request.uploadId());
          assertEquals(5242880, invocation.<RequestBody>getArgument(1).contentLength());
          assertEquals("application/octet-stream", invocation.<RequestBody>getArgument(1).contentType());
          return UploadPartResponse.builder().eTag("etag-" + request.partNumber()).build();
        });
    when(client.completeMultipartUpload(
        ArgumentMatchers.<Consumer<CompleteMultipartUploadRequest.Builder>>any()))
        .thenAnswer(invocation -> {
          Consumer<CompleteMultipartUploadRequest.Builder> consumer = invocation.getArgument(0);
          CompleteMultipartUploadRequest.Builder builder = CompleteMultipartUploadRequest.builder();
          consumer.accept(builder);
          CompleteMultipartUploadRequest request = builder.build();
          assertEquals("bucket1", request.bucket());
          assertEquals("upload", request.uploadId());
          assertEquals(2, request.multipartUpload().parts().size());
          for (int i = 0; i < 2; i++) {
            assertEquals(i + 1, request.multipartUpload().parts().get(i).partNumber());
            assertEquals("etag-" + (i + 1), request.multipartUpload().parts().get(i).eTag());
          }
          return null;
        });
    generator(client, "--size", "5242880", "--multi-part-upload", "--parts", "2").call();
    verify(client).completeMultipartUpload(
        ArgumentMatchers.<Consumer<CompleteMultipartUploadRequest.Builder>>any());
  }

  @Test
  void rejectsSmallMultipartParts() {
    S3KeyGenerator generator = generator(mock(S3Client.class), "--size", "128", "--multi-part-upload");
    assertThrows(IllegalArgumentException.class, generator::call);
  }

  private static S3KeyGenerator generator(S3Client client, String... options) {
    S3KeyGenerator generator = new S3KeyGenerator() {
      @Override
      protected void s3ClientInit() {
        init();
      }

      @Override
      public S3Client getS3() {
        return client;
      }
    };
    CommandLine command = new CommandLine(generator);
    command.parseArgs(Stream.concat(Stream.of("-n", "1", "-t", "1"), Arrays.stream(options)).toArray(String[]::new));
    return generator;
  }
}
