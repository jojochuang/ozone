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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.ChunkBufferToByteString;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.compression.CompressionStreams;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.CompressedChunkLayout;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.api.Test;

/**
 * Tests compressed chunk write/read with multiple segments in a block file.
 */
public class TestCompressedFilePerBlockStrategy extends TestFilePerBlockStrategy {

  @Test
  public void testCompressionStreamsRoundTrip() throws Exception {
    byte[] input = new byte[2048];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) (i % 251);
    }
    byte[] compressed = CompressionStreams.compress(CompressionCodec.ZSTD,
        input);
    byte[] restored = CompressionStreams.decompress(CompressionCodec.ZSTD,
        compressed);
    assertArrayEquals(input, restored);
  }

  @Test
  public void testMultipleCompressedChunksRoundTrip() throws Exception {
    final int chunkCount = 4;
    final int logicalLen = 256;

    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    ChunkManager subject = createTestSubject();
    long physicalOffset = 0;

    for (int i = 0; i < chunkCount; i++) {
      byte[] logicalData = ("chunk-" + i + "-payload-").getBytes();
      logicalData = padToLength(logicalData, logicalLen);
      byte[] compressed = CompressionStreams.compress(
          CompressionCodec.ZSTD, logicalData);

      ChunkInfo info = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), i),
          physicalOffset, compressed.length);
      info.setLogicalLen(logicalLen);
      ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(compressed));
      setDataChecksum(info, data);
      subject.writeChunk(container, blockID, info, data, WRITE_STAGE);
      physicalOffset += CompressedChunkLayout.segmentPhysicalLen(
          compressed.length);
    }

    BlockData blockData = new BlockData(blockID);
    blockData.setCompressionCodec(CompressionCodec.ZSTD.toDnProto());
    subject.finishWriteChunks(container, blockData);

    KeyValueContainerData containerData = container.getContainerData();
    Path blockFile = getStrategy().getLayout()
        .getChunkFile(containerData, blockID, null).toPath();
    try (FileChannel channel = FileChannel.open(blockFile,
        StandardOpenOption.READ)) {
      assertEquals(chunkCount,
          CompressedChunkLayout.parseOzciFooter(channel).size());
    }

    physicalOffset = 0;
    for (int i = 0; i < chunkCount; i++) {
      byte[] logicalData = ("chunk-" + i + "-payload-").getBytes();
      logicalData = padToLength(logicalData, logicalLen);
      byte[] compressed = CompressionStreams.compress(
          CompressionCodec.ZSTD, logicalData);

      ChunkInfo readInfo = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), i),
          physicalOffset, compressed.length);
      readInfo.setLogicalLen(logicalLen);
      ChunkBufferToByteString read = subject.readChunk(
          container, blockID, readInfo, null);
      assertArrayEquals(compressed, read.toByteString().toByteArray());
      physicalOffset += CompressedChunkLayout.segmentPhysicalLen(
          compressed.length);
    }
  }

  private static byte[] padToLength(byte[] data, int length) {
    byte[] padded = new byte[length];
    System.arraycopy(data, 0, padded, 0, Math.min(data.length, length));
    return padded;
  }
}
