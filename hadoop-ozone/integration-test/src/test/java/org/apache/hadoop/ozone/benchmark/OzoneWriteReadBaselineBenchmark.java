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

import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.fileSizes;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.filesPerRound;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.gitSha;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.keyName;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.measuredRounds;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.readKey;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.warmupRounds;
import static org.apache.hadoop.ozone.benchmark.WriteReadBenchmarkSupport.writeKey;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Pre-compression baseline: plain bucket create + put/get without codec APIs.
 *
 * <p>Intended to run from a git worktree at commit before compression landed.
 * Run with {@code -Dozone.run.benchmark=true}.
 */
@Tag("manual")
@EnabledIfSystemProperty(named = "ozone.run.benchmark", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OzoneWriteReadBaselineBenchmark {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String volumeName;

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferSize(1024);
    conf.setFromObject(clientConfig);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL,
        6, TimeUnit.SECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    volumeName = "baseline-vol-" + System.nanoTime();
    client.getObjectStore().createVolume(volumeName);
  }

  @AfterAll
  void shutdown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void benchmarkBaseline() throws Exception {
    BenchmarkResultWriter writer = new BenchmarkResultWriter(
        "pre-compression-baseline", gitSha());
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    String bucketName = "baseline-bucket";
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (long size : fileSizes()) {
      byte[] payload = CompressiblePayload.create((int) size);
      BenchmarkStats writeStats = new BenchmarkStats();
      BenchmarkStats readStats = new BenchmarkStats();

      for (int w = 0; w < warmupRounds(); w++) {
        runRound(bucket, payload, size, w, true);
      }
      for (int round = 0; round < measuredRounds(); round++) {
        runRound(bucket, payload, size, round, false, writeStats, readStats);
      }

      writer.addRow("BASELINE", size, "write", writeStats, null);
      writer.addRow("BASELINE", size, "read", readStats, null);
    }
    writer.write();
  }

  private void runRound(OzoneBucket bucket, byte[] payload, long size,
      int round, boolean warmup) throws IOException {
    runRound(bucket, payload, size, round, warmup, null, null);
  }

  private void runRound(OzoneBucket bucket, byte[] payload, long size,
      int round, boolean warmup, BenchmarkStats writeStats,
      BenchmarkStats readStats) throws IOException {
    int files = filesPerRound();
    for (int i = 0; i < files; i++) {
      String key = keyName("baseline", size, round, i);
      long t0 = System.nanoTime();
      writeKey(bucket, key, payload);
      long writeNanos = System.nanoTime() - t0;
      if (!warmup && writeStats != null) {
        writeStats.addSampleNanos(writeNanos);
      }

      t0 = System.nanoTime();
      readKey(bucket, key, payload.length);
      long readNanos = System.nanoTime() - t0;
      if (!warmup && readStats != null) {
        readStats.addSampleNanos(readNanos);
      }
    }
  }
}
