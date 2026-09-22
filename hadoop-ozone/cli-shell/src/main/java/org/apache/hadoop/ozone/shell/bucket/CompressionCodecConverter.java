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

package org.apache.hadoop.ozone.shell.bucket;

import org.apache.hadoop.ozone.compression.CompressionCodec;
import picocli.CommandLine;

/**
 * Picocli converter for {@link CompressionCodec}.
 */
public class CompressionCodecConverter
    implements CommandLine.ITypeConverter<CompressionCodec> {

  @Override
  public CompressionCodec convert(String value) {
    if (value == null) {
      return null;
    }
    for (CompressionCodec candidate : CompressionCodec.values()) {
      if (candidate.name().equalsIgnoreCase(value)) {
        return candidate;
      }
    }
    throw new IllegalArgumentException(
        "Unknown compression codec: " + value
            + ". Allowed values: NONE, ZSTD, SNAPPY, LZ4, GZIP");
  }
}
