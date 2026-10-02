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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduces unbounded growth of {@code RaftClientImpl.RepliedCallIds#sent} when
 * read-only Ratis {@code watch} RPCs complete without {@code repliedCallIds.add()}.
 * See CDPD-123869 / RATIS-872 client bookkeeping.
 */
public class TestRepliedCallIdsSentGrowth {
  private static final int WATCH_ITERATIONS = 80;
  private static final int CHUNK_SIZE = 1024;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerLocationProtocolClientSideTranslatorPB scmClient;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 600, TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig = conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(30));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(30));
    conf.setFromObject(ratisServerConfig);
    RatisClientConfig.RaftConfig raftClientConfig = conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(30));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(raftClientConfig);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    scmClient = cluster.getStorageContainerLocationClient();
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void watchRpcsGrowRepliedCallIdsSentMapOnLongLivedClient() throws Exception {
    ContainerWithPipeline container = scmClient.allocateContainer(
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
    Pipeline pipeline = container.getPipeline();
    long containerId = container.getContainerInfo().getContainerID();
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerId);

    try (XceiverClientManager mgr = new XceiverClientManager(conf);
         XceiverClientSpi xceiver = mgr.acquireClient(pipeline)) {
      XceiverClientRatis ratisXceiver = (XceiverClientRatis) xceiver;
      RaftClient raftClient = getRaftClient(ratisXceiver);

      ContainerCommandRequestProto createContainer = ContainerTestHelper.getCreateContainerRequest(
          containerId, pipeline);
      XceiverClientReply createReply = ratisXceiver.sendCommandAsync(createContainer);
      createReply.getResponse().get();
      raftClient.async().watch(createReply.getLogIndex(), ReplicationLevel.MAJORITY_COMMITTED).get();

      int sentAfterCreate = repliedCallIdsSentSize(raftClient);

      for (int i = 0; i < WATCH_ITERATIONS; i++) {
        ContainerCommandRequestProto writeChunk = ContainerTestHelper.getWriteChunkRequest(
            pipeline, blockID, CHUNK_SIZE);
        XceiverClientReply writeReply = ratisXceiver.sendCommandAsync(writeChunk);
        writeReply.getResponse().get();
        long index = writeReply.getLogIndex();
        RaftClientReply watchReply = raftClient.async()
            .watch(index, ReplicationLevel.MAJORITY_COMMITTED).get();
        assertThat(watchReply.isSuccess()).isTrue();
      }

      int sentAfterLoop = repliedCallIdsSentSize(raftClient);
      assertThat(sentAfterLoop)
          .as("RepliedCallIds#sent must not grow with each completed read-only watch")
          .isLessThanOrEqualTo(sentAfterCreate + 2);
      assertEquals(sentAfterLoop, repliedCallIdsSentSize(raftClient),
          "sent size should be stable once watches complete");
    }
  }

  private static RaftClient getRaftClient(XceiverClientRatis client) throws Exception {
    Field clientField = XceiverClientRatis.class.getDeclaredField("client");
    clientField.setAccessible(true);
    @SuppressWarnings("unchecked")
    AtomicReference<RaftClient> ref = (AtomicReference<RaftClient>) clientField.get(client);
    RaftClient raftClient = ref.get();
    if (raftClient == null) {
      client.connect();
      raftClient = ref.get();
    }
    return raftClient;
  }

  private static int repliedCallIdsSentSize(RaftClient raftClient) throws Exception {
    RaftClientImpl impl = (RaftClientImpl) raftClient;
    Field repliedCallIdsField = RaftClientImpl.class.getDeclaredField("repliedCallIds");
    repliedCallIdsField.setAccessible(true);
    Object repliedCallIds = repliedCallIdsField.get(impl);
    Field sentField = repliedCallIds.getClass().getDeclaredField("sent");
    sentField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<Long, ?> sent = (ConcurrentMap<Long, ?>) sentField.get(repliedCallIds);
    return sent.size();
  }
}
