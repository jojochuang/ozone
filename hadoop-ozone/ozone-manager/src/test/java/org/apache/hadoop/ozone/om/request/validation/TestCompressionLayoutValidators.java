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

package org.apache.hadoop.ozone.om.request.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.jupiter.api.Test;

/**
 * Tests layout-finalization validators for compression features.
 */
public class TestCompressionLayoutValidators {

  @Test
  public void rejectBucketCreateWithCompressionBeforeFinalization()
      throws Exception {
    OMRequest request = baseRequest(Type.CreateBucket)
        .setCreateBucketRequest(CreateBucketRequest.newBuilder()
            .setBucketInfo(BucketInfo.newBuilder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setIsVersionEnabled(false)
                .setStorageType(StorageTypeProto.DISK)
                .setCompressionCodec(
                    OzoneManagerProtocolProtos.CompressionCodecProto
                        .COMPRESSION_ZSTD)))
        .build();

    ValidationContext ctx = mockContext(false);
    assertThrows(OMException.class, () ->
        OMBucketCreateRequest.disallowCreateBucketWithCompressionCodec(
            request, ctx));
  }

  @Test
  public void rejectSetBucketPropertyWithCompressionBeforeFinalization()
      throws Exception {
    OMRequest request = baseRequest(Type.SetBucketProperty)
        .setSetBucketPropertyRequest(SetBucketPropertyRequest.newBuilder()
            .setBucketArgs(BucketArgs.newBuilder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setCompressionCodec(
                    OzoneManagerProtocolProtos.CompressionCodecProto
                        .COMPRESSION_ZSTD)))
        .build();

    ValidationContext ctx = mockContext(false);
    assertThrows(OMException.class, () ->
        OMBucketSetPropertyRequest.disallowSetBucketPropertyWithCompressionCodec(
            request, ctx));
  }

  @Test
  public void rejectCreateKeyInCompressionBucketBeforeFinalization()
      throws Exception {
    OMRequest request = baseRequest(Type.CreateKey)
        .setCreateKeyRequest(CreateKeyRequest.newBuilder()
            .setKeyArgs(KeyArgs.newBuilder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName("data.csv")))
        .build();

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    ValidationContext ctx = mockContext(false);
    when(ctx.getBucketInfo("vol", "bucket")).thenReturn(bucketInfo);

    assertThrows(OMException.class, () ->
        OMKeyCreateRequest.disallowCreateKeyInCompressionBucket(request, ctx));
  }

  @Test
  public void rejectInitiateMpuInCompressionBucketBeforeFinalization()
      throws Exception {
    OMRequest request = baseRequest(Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(
            MultipartInfoInitiateRequest.newBuilder()
                .setKeyArgs(KeyArgs.newBuilder()
                    .setVolumeName("vol")
                    .setBucketName("bucket")
                    .setKeyName("data.csv")))
        .build();

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    ValidationContext ctx = mockContext(false);
    when(ctx.getBucketInfo("vol", "bucket")).thenReturn(bucketInfo);

    assertThrows(OMException.class, () ->
        S3InitiateMultipartUploadRequest
            .disallowInitiateMultiPartUploadInCompressionBucket(request, ctx));
  }

  @Test
  public void allowCreateKeyWhenCompressionFeatureFinalized()
      throws Exception {
    OMRequest request = baseRequest(Type.CreateKey)
        .setCreateKeyRequest(CreateKeyRequest.newBuilder()
            .setKeyArgs(KeyArgs.newBuilder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName("data.csv")))
        .build();

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    ValidationContext ctx = mockContext(true);
    when(ctx.getBucketInfo("vol", "bucket")).thenReturn(bucketInfo);

    assertEquals(request,
        OMKeyCreateRequest.disallowCreateKeyInCompressionBucket(request, ctx));
  }

  private static OMRequest.Builder baseRequest(Type type) {
    return OMRequest.newBuilder()
        .setCmdType(type)
        .setClientId(UUID.randomUUID().toString());
  }

  private static ValidationContext mockContext(boolean compressionAllowed) {
    LayoutVersionManager versionManager = mock(LayoutVersionManager.class);
    when(versionManager.isAllowed(OMLayoutFeature.COMPRESSION_SUPPORT))
        .thenReturn(compressionAllowed);
    ValidationContext ctx = mock(ValidationContext.class);
    when(ctx.versionManager()).thenReturn(versionManager);
    return ctx;
  }
}
