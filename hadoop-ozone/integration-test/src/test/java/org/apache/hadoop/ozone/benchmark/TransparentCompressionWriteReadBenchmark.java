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
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Manual benchmark: write/read many keys per compression codec and file size.
 *
 * <p>Run with {@code -Dozone.run.benchmark=true}.
 */
@Tag("manual")
@EnabledIfSystemProperty(named = "ozone.run.benchmark", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TransparentCompressionWriteReadBenchmark {

  private static final CompressionCodec[] CODECS = {
      CompressionCodec.NONE,
      CompressionCodec.ZSTD,
      CompressionCodec.SNAPPY,
      CompressionCodec.LZ4,
      CompressionCodec.GZIP
  };

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String volumeName;
  private OzoneManagerProtocol omClient;

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
    volumeName = "bench-vol-" + System.nanoTime();
    client.getObjectStore().createVolume(volumeName);
    omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
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
  void benchmarkAllCodecs() throws Exception {
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);

    for (CompressionCodec codec : codecsToRun()) {
      String bucketName = "bench-" + codec.name().toLowerCase() + "-"
          + System.nanoTime();
      BenchmarkResultWriter writer = new BenchmarkResultWriter(
          "compression-codec-" + codec.name().toLowerCase(), gitSha());
      try {
        volume.createBucket(bucketName, BucketArgs.newBuilder()
            .setCompressionCodec(codec)
            .build());
      } catch (IOException e) {
        System.err.println("Skipping codec " + codec + ": " + e.getMessage());
        continue;
      }
      OzoneBucket bucket = volume.getBucket(bucketName);
      runCodec(writer, codec, bucket);
      writer.write();
    }
  }

  private static CompressionCodec[] codecsToRun() {
    String prop = System.getProperty("ozone.benchmark.codecs");
    if (prop == null || prop.trim().isEmpty()) {
      return CODECS;
    }
    String[] names = prop.split(",");
    CompressionCodec[] codecs = new CompressionCodec[names.length];
    for (int i = 0; i < names.length; i++) {
      codecs[i] = CompressionCodec.valueOf(names[i].trim().toUpperCase());
    }
    return codecs;
  }

  private void runCodec(BenchmarkResultWriter writer, CompressionCodec codec,
      OzoneBucket bucket) throws Exception {
    String codecLabel = codec.name();
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

      Double ratio = null;
      if (codec.isEnabled()) {
        ratio = sampleStoredToLogicalRatio(bucket, payload.length);
      }
      writer.addRow(codecLabel, size, "write", writeStats, ratio);
      writer.addRow(codecLabel, size, "read", readStats, null);
    }
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
      String key = keyName("bench", size, round, i);
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

  private Double sampleStoredToLogicalRatio(OzoneBucket bucket, int logicalLen)
      throws Exception {
    String probeKey = keyName("ratio", logicalLen, 0, 0);
    writeKey(bucket, probeKey, CompressiblePayload.create(logicalLen));
    OmKeyInfo keyInfo = omClient.getKeyInfo(new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(probeKey)
        .build(), false).getKeyInfo();

    BlockData blockData = readBlockDataFromDatanode(keyInfo);
    long stored = 0;
    long logical = 0;
    for (ContainerProtos.ChunkInfo chunkProto : blockData.getChunks()) {
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkProto);
      stored += chunkInfo.getLen();
      logical += chunkInfo.getLogicalLen() > 0
          ? chunkInfo.getLogicalLen() : chunkInfo.getLen();
    }
    if (logical == 0) {
      return null;
    }
    return stored / (double) logical;
  }

  private BlockData readBlockDataFromDatanode(OmKeyInfo keyInfo)
      throws IOException {
    OmKeyLocationInfo location = keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly().get(0);
    BlockID blockID = location.getBlockID();
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      OzoneContainer ozoneContainer = dn.getDatanodeStateMachine().getContainer();
      KeyValueHandler keyValueHandler = (KeyValueHandler) ozoneContainer
          .getDispatcher()
          .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
      Container container = ozoneContainer.getContainerSet()
          .getContainer(blockID.getContainerID());
      if (container == null) {
        continue;
      }
      try {
        return keyValueHandler.getBlockManager().getBlock(container, blockID);
      } catch (StorageContainerException e) {
        // try next replica
      }
    }
    throw new IOException("Block not found on any datanode: " + blockID);
  }
}
