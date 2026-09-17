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

package org.apache.hadoop.ozone.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.client.OzoneBucket;

/**
 * Shared write/read timing helpers for compression benchmarks.
 */
public final class WriteReadBenchmarkSupport {

  public static final long[] DEFAULT_SIZES = {
      1024,
      1024L * 1024,
      10L * 1024 * 1024,
      100L * 1024 * 1024
  };

  private WriteReadBenchmarkSupport() {
  }

  public static int filesPerRound() {
    return Integer.getInteger("ozone.benchmark.files", 5);
  }

  public static int measuredRounds() {
    return Integer.getInteger("ozone.benchmark.rounds", 3);
  }

  public static int warmupRounds() {
    return Integer.getInteger("ozone.benchmark.warmup.rounds", 1);
  }

  public static long[] fileSizes() {
    String prop = System.getProperty("ozone.benchmark.sizes");
    if (prop == null || prop.isEmpty()) {
      return DEFAULT_SIZES;
    }
    String[] parts = prop.split(",");
    List<Long> sizes = new ArrayList<>();
    for (String part : parts) {
      sizes.add(Long.parseLong(part.trim()));
    }
    return sizes.stream().mapToLong(Long::longValue).toArray();
  }

  public static ReplicationConfig replication() {
    return RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  }

  public static void writeKey(OzoneBucket bucket, String keyName, byte[] data)
      throws IOException {
    try (OutputStream out = bucket.createKey(keyName, data.length,
        replication(), Collections.emptyMap())) {
      out.write(data);
    }
  }

  public static void readKey(OzoneBucket bucket, String keyName, int length)
      throws IOException {
    try (InputStream in = bucket.readKey(keyName)) {
      IOUtils.readFully(in, length);
    }
  }

  public static String keyName(String prefix, long size, int round, int index) {
    return String.format("%s-%d-%d-%d-%s", prefix, size, round, index,
        UUID.randomUUID());
  }

  public static String gitSha() {
    String sha = System.getProperty("ozone.benchmark.git.sha");
    if (sha != null && !sha.isEmpty()) {
      return sha;
    }
    return "unknown";
  }
}
