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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.compression.CompressionStreams;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.CompressedChunkLayout;
import org.apache.hadoop.ozone.container.keyvalue.impl.TestCompressedFilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Verifies container scrub accepts compressed chunk layout.
 */
public class TestCompressedKeyValueContainerCheck
    extends TestCompressedFilePerBlockStrategy {

  @Test
  public void compressedBlockPassesFullCheck() throws Exception {
    final int chunkCount = 2;
    final int logicalLen = 512;

    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    ChunkManager subject = createTestSubject();
    List<ChunkInfo> chunkInfos = new ArrayList<>();
    long physicalOffset = 0;

    for (int i = 0; i < chunkCount; i++) {
      byte[] logicalData = padToLength(("chunk-" + i).getBytes(), logicalLen);
      byte[] compressed = CompressionStreams.compress(
          CompressionCodec.ZSTD, logicalData);

      ChunkInfo info = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), i),
          physicalOffset, compressed.length);
      info.setLogicalLen(logicalLen);
      ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(compressed));
      setDataChecksum(info, data);
      subject.writeChunk(container, blockID, info, data, WRITE_STAGE);
      chunkInfos.add(info);
      physicalOffset += CompressedChunkLayout.segmentPhysicalLen(
          compressed.length);
    }

    BlockData blockData = new BlockData(blockID);
    blockData.setCompressionCodec(CompressionCodec.ZSTD.toDnProto());
    List<org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo>
        chunkProtos = new ArrayList<>();
    for (ChunkInfo chunkInfo : chunkInfos) {
      chunkProtos.add(chunkInfo.getProtoBufMessage());
    }
    blockData.setChunks(chunkProtos);
    subject.finishWriteChunks(container, blockData);

    OzoneConfiguration conf = new OzoneConfiguration();
    try (DBHandle dbHandle = BlockUtils.getDB(container.getContainerData(),
        conf)) {
      dbHandle.getStore().getBlockDataTable().put(
          container.getContainerData().getBlockKey(blockID.getLocalID()),
          blockData);
    }

    container.close();
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);
    ContainerScannerConfiguration scannerConfig =
        conf.getObject(ContainerScannerConfiguration.class);
    assertFalse(kvCheck.fullCheck(new DataTransferThrottler(
        scannerConfig.getBandwidthPerVolume()), new Canceler()).hasErrors());
    assertEquals(ContainerLayoutVersion.FILE_PER_BLOCK,
        container.getContainerData().getLayoutVersion());
  }

  private static byte[] padToLength(byte[] data, int length) {
    byte[] padded = new byte[length];
    System.arraycopy(data, 0, padded, 0, Math.min(data.length, length));
    return padded;
  }
}
