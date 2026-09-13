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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link CompressedChunkLayout}.
 */
public class TestCompressedChunkLayout {

  @TempDir
  private Path tempDir;

  @Test
  public void roundTripHeaderFooterAndPayloadOffset() throws IOException {
    Path file = tempDir.resolve("block");
    try (FileChannel channel = FileChannel.open(file,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.READ)) {
      byte[] payload = "compressed".getBytes();
      CompressedChunkLayout.writeChunkHeader(channel, 0, 100, payload.length);
      channel.write(ByteBuffer.wrap(payload),
          CompressedChunkLayout.readChunkPayloadOffset(0));

      List<CompressedChunkLayout.OzciEntry> entries = Arrays.asList(
          new CompressedChunkLayout.OzciEntry(0, 0, 100));
      CompressedChunkLayout.writeOzciFooter(channel, entries);
    }

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      List<CompressedChunkLayout.OzciEntry> parsed =
          CompressedChunkLayout.parseOzciFooter(channel);
      assertEquals(1, parsed.size());
      assertEquals(0, parsed.get(0).getLogicalOffset());
      assertEquals(0, parsed.get(0).getPhysicalOffset());
      assertEquals(100, parsed.get(0).getUncompressedLen());

      ByteBuffer payload = ByteBuffer.allocate("compressed".length());
      channel.read(payload,
          CompressedChunkLayout.readChunkPayloadOffset(0));
      assertArrayEquals("compressed".getBytes(), payload.array());
    }
  }

  @Test
  public void nextSegmentOffsetAccountsForHeader() {
    long firstSegment = 0;
    long compressedLen = 50;
    long logicalLen = 200;
    long secondSegment = CompressedChunkLayout.nextSegmentOffset(
        firstSegment, compressedLen, logicalLen);
    assertEquals(60, secondSegment);

    long uncompressedNext = CompressedChunkLayout.nextSegmentOffset(
        100, 80, 0);
    assertEquals(180, uncompressedNext);
  }

  @Test
  public void toLogicalBlockPositionForMultiChunkBlock() throws Exception {
    long[] segmentOffsets = {0, 60, 130};
    long[] compressedLens = {50, 60, 40};
    long[] logicalLens = {200, 300, 100};

    assertEquals(0, CompressedChunkLayout.toLogicalBlockPosition(0,
        segmentOffsets, compressedLens, logicalLens));
    assertEquals(200, CompressedChunkLayout.toLogicalBlockPosition(60,
        segmentOffsets, compressedLens, logicalLens));
    assertEquals(500, CompressedChunkLayout.toLogicalBlockPosition(130,
        segmentOffsets, compressedLens, logicalLens));

    assertThrows(IOException.class, () ->
        CompressedChunkLayout.toLogicalBlockPosition(999, segmentOffsets,
            compressedLens, logicalLens));
  }

  @Test
  public void populateOzciIndexFromBlockFile() throws IOException {
    Path file = tempDir.resolve("multi-chunk-block");
    int logicalLen = 128;
    byte[] payload1 = "compressed-1".getBytes();
    byte[] payload2 = "compressed-2".getBytes();

    try (FileChannel channel = FileChannel.open(file,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.READ)) {
      CompressedChunkLayout.writeChunkHeader(channel, 0, logicalLen,
          payload1.length);
      channel.write(ByteBuffer.wrap(payload1),
          CompressedChunkLayout.readChunkPayloadOffset(0));

      long secondSegment = CompressedChunkLayout.nextSegmentOffset(0,
          payload1.length, logicalLen);
      CompressedChunkLayout.writeChunkHeader(channel, secondSegment, logicalLen,
          payload2.length);
      channel.write(ByteBuffer.wrap(payload2),
          CompressedChunkLayout.readChunkPayloadOffset(secondSegment));
    }

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      CompressedChunkLayout.OzciIndex index =
          new CompressedChunkLayout.OzciIndex();
      CompressedChunkLayout.populateOzciIndexFromBlockFile(channel, index);
      long expectedSecondSegment = CompressedChunkLayout.nextSegmentOffset(0,
          payload1.length, logicalLen);
      assertEquals(2, index.getEntries().size());
      assertEquals(0, index.getEntries().get(0).getPhysicalOffset());
      assertEquals(logicalLen, index.getEntries().get(0).getUncompressedLen());
      assertEquals(expectedSecondSegment,
          index.getEntries().get(1).getPhysicalOffset());
    }
  }
}
