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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CompressionCodecProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Transparent compression codec for Ozone keys and blocks.
 */
public enum CompressionCodec {
  NONE,
  ZSTD,
  SNAPPY,
  LZ4,
  GZIP;

  public boolean isEnabled() {
    return this != NONE;
  }

  public static CompressionCodec fromOmProto(
      OzoneManagerProtocolProtos.CompressionCodecProto proto) {
    if (proto == null) {
      return NONE;
    }
    switch (proto) {
    case COMPRESSION_ZSTD:
      return ZSTD;
    case COMPRESSION_SNAPPY:
      return SNAPPY;
    case COMPRESSION_LZ4:
      return LZ4;
    case COMPRESSION_GZIP:
      return GZIP;
    case COMPRESSION_NONE:
    default:
      return NONE;
    }
  }

  public OzoneManagerProtocolProtos.CompressionCodecProto toOmProto() {
    switch (this) {
    case ZSTD:
      return OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_ZSTD;
    case SNAPPY:
      return OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_SNAPPY;
    case LZ4:
      return OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_LZ4;
    case GZIP:
      return OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_GZIP;
    case NONE:
    default:
      return OzoneManagerProtocolProtos.CompressionCodecProto.COMPRESSION_NONE;
    }
  }

  public static CompressionCodec fromDnProto(CompressionCodecProto proto) {
    if (proto == null) {
      return NONE;
    }
    switch (proto) {
    case COMPRESSION_ZSTD:
      return ZSTD;
    case COMPRESSION_SNAPPY:
      return SNAPPY;
    case COMPRESSION_LZ4:
      return LZ4;
    case COMPRESSION_GZIP:
      return GZIP;
    case COMPRESSION_NONE:
    default:
      return NONE;
    }
  }

  public CompressionCodecProto toDnProto() {
    switch (this) {
    case ZSTD:
      return CompressionCodecProto.COMPRESSION_ZSTD;
    case SNAPPY:
      return CompressionCodecProto.COMPRESSION_SNAPPY;
    case LZ4:
      return CompressionCodecProto.COMPRESSION_LZ4;
    case GZIP:
      return CompressionCodecProto.COMPRESSION_GZIP;
    case NONE:
    default:
      return CompressionCodecProto.COMPRESSION_NONE;
    }
  }
}
