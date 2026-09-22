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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CompressionCodecProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CompressionCodec} proto conversions.
 */
public class TestCompressionCodec {

  @Test
  public void omProtoRoundTrip() {
    for (CompressionCodec codec : CompressionCodec.values()) {
      assertEquals(codec,
          CompressionCodec.fromOmProto(codec.toOmProto()));
    }
  }

  @Test
  public void dnProtoRoundTrip() {
    for (CompressionCodec codec : CompressionCodec.values()) {
      assertEquals(codec,
          CompressionCodec.fromDnProto(codec.toDnProto()));
    }
  }

  @Test
  public void enabledOnlyForNonNone() {
    assertFalse(CompressionCodec.NONE.isEnabled());
    assertTrue(CompressionCodec.ZSTD.isEnabled());
    assertEquals(CompressionCodecProto.COMPRESSION_ZSTD,
        CompressionCodec.ZSTD.toDnProto());
    assertEquals(OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_ZSTD,
        CompressionCodec.ZSTD.toOmProto());
  }
}
