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

package org.apache.hadoop.ozone.compression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CompressionPolicy}.
 */
public class TestCompressionPolicy {

  @Test
  public void resolveKeyCodecUsesBucketDefault() {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    OmConfig omConfig = new OmConfig();

    assertEquals(CompressionCodec.ZSTD,
        CompressionPolicy.resolveKeyCodec(bucketInfo, "data.csv", null,
            omConfig));
  }

  @Test
  public void resolveKeyCodecSkipsDeniedExtensions() {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    OmConfig omConfig = new OmConfig();

    assertEquals(CompressionCodec.NONE,
        CompressionPolicy.resolveKeyCodec(bucketInfo, "data.parquet", null,
            omConfig));
  }

  @Test
  public void resolveKeyCodecDisabledForEncryption() {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build();
    OmConfig omConfig = new OmConfig();
    FileEncryptionInfo encInfo = new FileEncryptionInfo(
        CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES,
        new byte[32],
        new byte[16],
        "testkey",
        "testkey@0");

    assertEquals(CompressionCodec.NONE,
        CompressionPolicy.resolveKeyCodec(bucketInfo, "data.csv", encInfo,
            omConfig));
  }

  @Test
  public void isExtensionDeniedMatchesLongestSuffix() {
    OmConfig omConfig = new OmConfig();
    omConfig.setCompressionSkipExtensions(".tar.gz,.gz");

    assertTrue(CompressionPolicy.isExtensionDenied("archive.tar.gz", omConfig));
    assertFalse(CompressionPolicy.isExtensionDenied("archive.csv", omConfig));
  }
}
