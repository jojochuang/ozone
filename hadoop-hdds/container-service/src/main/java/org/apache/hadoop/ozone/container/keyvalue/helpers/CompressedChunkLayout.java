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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * On-disk layout for client-compressed chunks in file-per-block storage.
 *
 * <p>Each compressed segment is stored as a 10-byte header followed by the
 * compressed payload. A block file may end with an OZCI footer for scrub and
 * recovery.
 */
public final class CompressedChunkLayout {

  public static final int CHUNK_HEADER_SIZE = 10;
  public static final byte[] CHUNK_MAGIC = {'O', 'C'};
  public static final byte[] OZCI_MAGIC = {'O', 'Z', 'C', 'I'};

  private static final int OZCI_FOOTER_HEADER_SIZE = 8;
  private static final int OZCI_ENTRY_SIZE = 24;

  private CompressedChunkLayout() {
  }

  public static void writeChunkHeader(FileChannel channel, long offset,
      long logicalLen, long compressedLen) throws IOException {
    if (logicalLen < 0 || logicalLen > Integer.MAX_VALUE) {
      throw new IOException("Invalid logical chunk length: " + logicalLen);
    }
    if (compressedLen < 0 || compressedLen > Integer.MAX_VALUE) {
      throw new IOException("Invalid compressed chunk length: " + compressedLen);
    }

    ByteBuffer header = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
    header.put(CHUNK_MAGIC);
    header.putInt((int) logicalLen);
    header.putInt((int) compressedLen);
    header.flip();
    channel.write(header, offset);
  }

  public static long readChunkPayloadOffset(long segmentOffset) {
    return segmentOffset + CHUNK_HEADER_SIZE;
  }

  public static long segmentPhysicalLen(long compressedLen) {
    return CHUNK_HEADER_SIZE + compressedLen;
  }

  /**
   * Returns the on-disk offset of the chunk segment that immediately follows
   * {@code segmentOffset}.
   */
  public static long nextSegmentOffset(long segmentOffset, long compressedLen,
      long logicalLen) {
    if (logicalLen > 0) {
      return segmentOffset + segmentPhysicalLen(compressedLen);
    }
    return segmentOffset + compressedLen;
  }

  public static long chunkLogicalLen(long compressedLen, long logicalLen) {
    return logicalLen > 0 ? logicalLen : compressedLen;
  }

  /**
   * Maps a physical segment offset to the logical block position used by
   * {@code BlockInputStream}.
   */
  public static long toLogicalBlockPosition(long targetSegmentOffset,
      long[] segmentOffsets, long[] compressedLens, long[] logicalLens)
      throws IOException {
    long logicalPos = 0;
    for (int i = 0; i < segmentOffsets.length; i++) {
      if (segmentOffsets[i] == targetSegmentOffset) {
        return logicalPos;
      }
      logicalPos += chunkLogicalLen(compressedLens[i], logicalLens[i]);
    }
    throw new IOException("No chunk at segment offset " + targetSegmentOffset);
  }

  /**
   * Rebuilds the OZCI index by scanning OC headers from the block file.
   */
  public static void populateOzciIndexFromBlockFile(FileChannel channel,
      OzciIndex index) throws IOException {
    long scanEnd = dataRegionEnd(channel);
    long physicalOffset = 0;
    while (physicalOffset + CHUNK_HEADER_SIZE <= scanEnd) {
      ByteBuffer header = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
      channel.read(header, physicalOffset);
      header.flip();
      byte[] magic = new byte[CHUNK_MAGIC.length];
      header.get(magic);
      if (!Arrays.equals(magic, CHUNK_MAGIC)) {
        break;
      }
      int logicalLen = header.getInt();
      int compressedLen = header.getInt();
      index.addEntry(physicalOffset, logicalLen);
      physicalOffset += segmentPhysicalLen(compressedLen);
    }
  }

  private static long dataRegionEnd(FileChannel channel) throws IOException {
    long fileLen = channel.size();
    List<OzciEntry> footer = parseOzciFooter(channel);
    if (footer.isEmpty()) {
      return fileLen;
    }
    int entryCount = footer.size();
    return fileLen - OZCI_FOOTER_HEADER_SIZE - entryCount * OZCI_ENTRY_SIZE;
  }

  public static void writeOzciFooter(FileChannel channel,
      List<OzciEntry> entries) throws IOException {
    int count = entries.size();
    long footerSize = OZCI_FOOTER_HEADER_SIZE + (long) count * OZCI_ENTRY_SIZE;
    long footerStart = channel.size();

    ByteBuffer footer = ByteBuffer.allocate((int) footerSize);
    footer.put(OZCI_MAGIC);
    footer.putInt(count);
    for (OzciEntry entry : entries) {
      footer.putLong(entry.getLogicalOffset());
      footer.putLong(entry.getPhysicalOffset());
      footer.putLong(entry.getUncompressedLen());
    }
    footer.flip();
    channel.write(footer, footerStart);
  }

  public static List<OzciEntry> parseOzciFooter(FileChannel channel)
      throws IOException {
    long fileLen = channel.size();
    if (fileLen < OZCI_FOOTER_HEADER_SIZE) {
      return Collections.emptyList();
    }

    long maxScan = Math.min(fileLen / OZCI_ENTRY_SIZE, 100_000);
    for (long entryCount = 0; entryCount <= maxScan; entryCount++) {
      long footerStart = fileLen - OZCI_FOOTER_HEADER_SIZE
          - entryCount * OZCI_ENTRY_SIZE;
      if (footerStart < 0) {
        break;
      }

      ByteBuffer header = ByteBuffer.allocate(OZCI_FOOTER_HEADER_SIZE);
      channel.position(footerStart);
      channel.read(header);
      header.flip();

      byte[] magic = new byte[OZCI_MAGIC.length];
      header.get(magic);
      if (!Arrays.equals(magic, OZCI_MAGIC)) {
        continue;
      }

      int count = header.getInt();
      if (count != entryCount) {
        continue;
      }

      List<OzciEntry> entries = new ArrayList<>(count);
      ByteBuffer entryBuf = ByteBuffer.allocate(OZCI_ENTRY_SIZE);
      for (int i = 0; i < count; i++) {
        entryBuf.clear();
        channel.read(entryBuf);
        entryBuf.flip();
        entries.add(new OzciEntry(entryBuf.getLong(), entryBuf.getLong(),
            entryBuf.getLong()));
      }
      return entries;
    }
    return Collections.emptyList();
  }

  /** One entry in the on-disk compressed chunk index. */
  /** One entry in the on-disk compressed chunk index. */
  public static final class OzciEntry {
    private final long logicalOffset;
    private final long physicalOffset;
    private final long uncompressedLen;

    public OzciEntry(long logicalOffset, long physicalOffset,
        long uncompressedLen) {
      this.logicalOffset = logicalOffset;
      this.physicalOffset = physicalOffset;
      this.uncompressedLen = uncompressedLen;
    }

    public long getLogicalOffset() {
      return logicalOffset;
    }

    public long getPhysicalOffset() {
      return physicalOffset;
    }

    public long getUncompressedLen() {
      return uncompressedLen;
    }
  }

  /** In-memory builder for a compressed chunk index. */
  /** In-memory builder for a compressed chunk index. */
  public static final class OzciIndex {
    private long logicalOffset;
    private final List<OzciEntry> entries = new ArrayList<>();

    public void addEntry(long physicalOffset, long uncompressedLen) {
      entries.add(new OzciEntry(logicalOffset, physicalOffset, uncompressedLen));
      logicalOffset += uncompressedLen;
    }

    public List<OzciEntry> getEntries() {
      return entries;
    }
  }
}
