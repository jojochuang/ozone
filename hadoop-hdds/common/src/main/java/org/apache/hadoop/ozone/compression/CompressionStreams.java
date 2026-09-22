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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

/**
 * Compresses and decompresses byte arrays for transparent Ozone compression.
 */
public final class CompressionStreams {

  /** On-disk compressed chunk header size written by the datanode. */
  public static final int CHUNK_HEADER_SIZE = 10;

  private static final CompressorStreamFactory FACTORY =
      new CompressorStreamFactory();

  private CompressionStreams() {
  }

  public static byte[] compress(CompressionCodec codec, byte[] data)
      throws IOException {
    if (codec == null || !codec.isEnabled()) {
      return data;
    }
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (OutputStream compressor = wrapOutput(codec, output)) {
      compressor.write(data);
    }
    return output.toByteArray();
  }

  public static byte[] decompress(CompressionCodec codec, byte[] data)
      throws IOException {
    if (codec == null || !codec.isEnabled()) {
      return data;
    }
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (InputStream decompressor = wrapInput(codec, input)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = decompressor.read(buffer)) >= 0) {
        output.write(buffer, 0, read);
      }
    }
    return output.toByteArray();
  }

  private static OutputStream wrapOutput(CompressionCodec codec,
      OutputStream output) throws IOException {
    try {
      return FACTORY.createCompressorOutputStream(
          getFactoryName(codec), output);
    } catch (CompressorException e) {
      throw toIOException(e);
    }
  }

  private static InputStream wrapInput(CompressionCodec codec,
      InputStream input) throws IOException {
    try {
      return FACTORY.createCompressorInputStream(
          getFactoryName(codec), input);
    } catch (CompressorException e) {
      throw toIOException(e);
    }
  }

  private static String getFactoryName(CompressionCodec codec) {
    switch (codec) {
    case GZIP:
      return CompressorStreamFactory.GZIP;
    case LZ4:
      return CompressorStreamFactory.LZ4_FRAMED;
    case SNAPPY:
      return CompressorStreamFactory.SNAPPY_FRAMED;
    case ZSTD:
      return CompressorStreamFactory.ZSTANDARD;
    case NONE:
    default:
      throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
  }

  private static IOException toIOException(CompressorException e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException(e);
  }
}
