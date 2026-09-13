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

package org.apache.hadoop.ozone.debug.replicas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChecksumVerifier}.
 */
public class TestChecksumVerifier {

  @Test
  public void verifyBlockPassesWhenStreamReadsSuccessfully() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    BlockInputStreamFactory factory = mock(BlockInputStreamFactory.class);
    BlockExtendedInputStream stream = mockEmptyStream();
    when(factory.create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.ZSTD)))
        .thenReturn(stream);

    ChecksumVerifier verifier = new ChecksumVerifier(conf, factory, null);
    OmKeyLocationInfo keyLocation = createKeyLocation();

    BlockVerificationResult result = verifier.verifyBlock(
        keyLocation.getPipeline().getFirstNode(), keyLocation,
        CompressionCodec.ZSTD);

    assertTrue(result.isCompleted());
    assertTrue(result.passed());
    verify(factory).create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.ZSTD));
  }

  @Test
  public void verifyBlockUsesNoneCodecWhenUncompressed() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    BlockInputStreamFactory factory = mock(BlockInputStreamFactory.class);
    BlockExtendedInputStream stream = mockEmptyStream();
    when(factory.create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.NONE)))
        .thenReturn(stream);

    ChecksumVerifier verifier = new ChecksumVerifier(conf, factory, null);
    OmKeyLocationInfo keyLocation = createKeyLocation();

    BlockVerificationResult result = verifier.verifyBlock(
        keyLocation.getPipeline().getFirstNode(), keyLocation,
        CompressionCodec.NONE);

    assertTrue(result.passed());
    verify(factory).create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.NONE));
  }

  @Test
  public void verifyBlockFailsOnChecksumException() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    BlockInputStreamFactory factory = mock(BlockInputStreamFactory.class);
    BlockExtendedInputStream stream = mock(BlockExtendedInputStream.class);
    when(stream.read(any(byte[].class)))
        .thenThrow(new IOException(new OzoneChecksumException("checksum mismatch")));
    when(factory.create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.ZSTD)))
        .thenReturn(stream);

    ChecksumVerifier verifier = new ChecksumVerifier(conf, factory, null);
    OmKeyLocationInfo keyLocation = createKeyLocation();

    BlockVerificationResult result = verifier.verifyBlock(
        keyLocation.getPipeline().getFirstNode(), keyLocation,
        CompressionCodec.ZSTD);

    assertTrue(result.isCompleted());
    assertFalse(result.passed());
    assertEquals("checksum mismatch", result.getFailures().get(0));
  }

  @Test
  public void verifyBlockFailsIncompleteOnIoException() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    BlockInputStreamFactory factory = mock(BlockInputStreamFactory.class);
    when(factory.create(any(), any(), any(), any(), any(), isNull(),
        any(OzoneClientConfig.class), eq(CompressionCodec.ZSTD)))
        .thenThrow(new IOException("dn unreachable"));

    ChecksumVerifier verifier = new ChecksumVerifier(conf, factory, null);
    OmKeyLocationInfo keyLocation = createKeyLocation();

    BlockVerificationResult result = verifier.verifyBlock(
        keyLocation.getPipeline().getFirstNode(), keyLocation,
        CompressionCodec.ZSTD);

    assertFalse(result.isCompleted());
    assertFalse(result.passed());
    assertEquals("dn unreachable", result.getFailures().get(0));
  }

  @Test
  public void getTypeReturnsChecksum() throws Exception {
    ChecksumVerifier verifier = new ChecksumVerifier(new OzoneConfiguration(),
        mock(BlockInputStreamFactory.class), mock(XceiverClientManager.class));
    assertEquals("checksum", verifier.getType());
  }

  private static BlockExtendedInputStream mockEmptyStream() throws IOException {
    BlockExtendedInputStream stream = mock(BlockExtendedInputStream.class);
    when(stream.read(any(byte[].class))).thenReturn(-1);
    return stream;
  }

  private static OmKeyLocationInfo createKeyLocation() {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setNodes(Collections.singletonList(datanode))
        .build();
    BlockID blockID = new BlockID(1L, 2L);
    return new OmKeyLocationInfo.Builder()
        .setPipeline(pipeline)
        .setBlockID(blockID)
        .setLength(1024)
        .build();
  }
}
