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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end tests for transparent client-side compression.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTransparentCompression {

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
    volumeName = "vol-" + UUID.randomUUID();
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
  void testCompressedPutGetRoundTrip() throws Exception {
    String bucketName = "bucket-" + UUID.randomUUID();
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    volume.createBucket(bucketName);

    OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    omClient.setBucketProperty(OmBucketArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build());

    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = "compressed-" + UUID.randomUUID();
    byte[] data = RandomUtils.secure().randomBytes(16 * 1024);
    ReplicationConfig replication = RatisReplicationConfig.getInstance(THREE);
    TestDataUtil.createKey(bucket, keyName, replication, data);

    try (InputStream in = bucket.readKey(keyName)) {
      assertArrayEquals(data, IOUtils.readFully(in, data.length));
    }

    OmKeyInfo keyInfo = omClient.getKeyInfo(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build(), false).getKeyInfo();
    assertEquals(CompressionCodec.ZSTD, keyInfo.getCompressionCodec());
    assertEquals(data.length, keyInfo.getDataSize());
  }

  @Test
  void testDenylistSkipsCompression() throws Exception {
    String bucketName = "bucket-" + UUID.randomUUID();
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    volume.createBucket(bucketName);

    OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    omClient.setBucketProperty(OmBucketArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setCompressionCodec(CompressionCodec.ZSTD)
        .build());

    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = "data.parquet";
    byte[] data = RandomUtils.secure().randomBytes(2048);
    ReplicationConfig replication = RatisReplicationConfig.getInstance(THREE);
    TestDataUtil.createKey(bucket, keyName, replication, data);

    OmKeyInfo keyInfo = omClient.getKeyInfo(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build(), false).getKeyInfo();
    assertEquals(CompressionCodec.NONE, keyInfo.getCompressionCodec());
    assertEquals(data.length, keyInfo.getDataSize());
  }
}
