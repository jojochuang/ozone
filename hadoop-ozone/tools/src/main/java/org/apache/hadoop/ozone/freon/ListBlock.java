/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Ozone Debug Command line tool.
 */
@CommandLine.Command(name = "dbl",
    aliases = "datanode-block-listing",
    description = "List blocks for a datanode",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ListBlock  extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ListBlock.class);

  @CommandLine.Option(names = {"-d", "--datanode"},
      description = "Datanode to use.",
      defaultValue = "")
  private String datanodeId;

  @CommandLine.Option(names = {"-nc", "--numContainers"},
      description = "Number of containers to list",
      defaultValue = "1024")
  private int numContainers;

  @CommandLine.Option(names = {"-nb", "--numBlocksPerContainer"},
      description = "Number of blocks to list per container",
      defaultValue = "1024")
  private int numBlocksPerContainer;

  @CommandLine.Option(names = {"-sc", "--startContainerId"},
      description = "ContainerId to list from",
      defaultValue = "0")
  private int startContainerId;

  @CommandLine.Option(names = {"-v", "--verifyChecksum"},
      description = "whether to verify checksum when reading or not",
      defaultValue = "false")
  private boolean verifyChecksum;

  private XceiverClientSpi xceiverClientSpi;


  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
          "Block listing is not supported in secure environment"
      );
    }

    if (Strings.isNullOrEmpty(datanodeId)) {
      throw new IOException("DatanodeID is not provided");
    }

    int containersListed = 0;
    int containersPerCall = 10000;
    while (containersListed < numContainers) {
      try (
          StorageContainerLocationProtocol scmLocationClient = createStorageContainerLocationClient(
              ozoneConf)) {
        List<ContainerInfo> containers =
            scmLocationClient.listContainer(startContainerId, startContainerId + containersPerCall);
        LOG.info("{}", String.format("Received %d containers from SCM", containers.size()));
        System.out.println(String.format("Received %d containers from SCM", containers.size()));
        if (containers.isEmpty()) {
          LOG.info("No more containers found, totally listed " + containersListed);
          System.out.println("No more containers found, totally listed " + containersListed);
          break;
        }
        for (ContainerInfo containerInfo : containers) {
          ContainerWithPipeline containerWithPipeline = scmLocationClient
              .getContainerWithPipeline(containerInfo.getContainerID());
          LOG.info("{}", String
              .format("Received pipeline info %s for container %s",
                  containerWithPipeline, containerInfo));
          System.out.println(String
              .format("Received pipeline info %s for container %s",
                  containerWithPipeline, containerInfo));
          for (DatanodeDetails datanodeDetails : containerWithPipeline
              .getPipeline().getNodes()) {
            if (datanodeId.equals(datanodeDetails.getUuid().toString())) {
              containersListed++;
              listBlocks(containerWithPipeline, datanodeDetails, ozoneConf);
              break;
            }
          }
          if (containersListed == numContainers) {
            break;
          }
        }
        startContainerId += containersPerCall;
      }
    }
    return null;
  }

  // list blocks of a given DN
  private void listBlocks(ContainerWithPipeline containerWithPipeline,
      DatanodeDetails datanodeDetails, OzoneConfiguration ozoneConf) throws IOException {
    try (XceiverClientManager xceiverClientManager = new XceiverClientManager(
        ozoneConf)) {
      Pipeline pipeine = Pipeline.newBuilder(containerWithPipeline.getPipeline())
          .setNodes(Collections.singletonList(datanodeDetails))
          .setId(PipelineID.randomId())
          .setFactor(HddsProtos.ReplicationFactor.ONE)
          .setType(HddsProtos.ReplicationType.STAND_ALONE)
          .build();
      xceiverClientSpi = xceiverClientManager.acquireClientForReadData(pipeine);
      ContainerProtos.ListBlockRequestProto lbrp =
          ContainerProtos.ListBlockRequestProto.newBuilder()
              .setStartLocalID(0)
              .setCount(numBlocksPerContainer)
              .build();
      ContainerProtos.ContainerCommandRequestProto ccrp =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setListBlock(lbrp)
              .setDatanodeUuid(datanodeId)
              .setCmdType(ContainerProtos.Type.ListBlock)
              .setContainerID(containerWithPipeline.getContainerInfo()
                  .getContainerID())
              .build();
      ContainerProtos.ListBlockResponseProto listBlockResponseProto =
          xceiverClientSpi.sendCommand(ccrp).getListBlock();

      BlockReader reader = new BlockReader(pipeine, verifyChecksum);
      for (ContainerProtos.BlockData blockData : listBlockResponseProto.getBlockDataList()) {
        reader.readBlock(blockData);
      }
    } finally {
      if (xceiverClientSpi != null) {
        xceiverClientSpi.close();
      }
    }
  }

  static private class BlockReader {
    private Pipeline pipeline;
    private boolean verifyChecksum;
    private byte[] buf;

    public BlockReader(Pipeline pipeline, boolean verifyChecksum) {
      this.pipeline = pipeline;
      this.verifyChecksum = verifyChecksum;
      buf = new byte[1024*1024];
    }
    private void readBlock(ContainerProtos.BlockData blockData)
        throws IOException {
      ContainerProtos.DatanodeBlockID datanodeBlockID = blockData.getBlockID();
      BlockID blockID = new BlockID(
          datanodeBlockID.getContainerID(),
          datanodeBlockID.getLocalID());

      LOG.info("{}", String.format("Block %s:%s chunksCount:%s",
          blockID.getContainerID(),
          blockID.getLocalID(), blockData.getChunksCount()));
      //System.out.println(String.format("Block %s:%s chunksCount:%s",
      //    blockID.getContainerID(),
      //    blockID.getLocalID(), blockData.getChunksCount()));

      List<ContainerProtos.ChunkInfo> chunks = blockData.getChunksList();
      if (chunks != null && !chunks.isEmpty()) {
        for (ContainerProtos.ChunkInfo chunkInfo : chunks) {
          this.readChunk(chunkInfo, blockID);
        }
      }
    }

    private void readChunk(ContainerProtos.ChunkInfo chunkInfo,
        BlockID blockID) throws IOException {
      // read chunks sequentially
      try (ChunkInputStream is = new ChunkInputStream(chunkInfo, blockID, null,
          () -> pipeline, verifyChecksum, null)) {

        while (is.read(buf) != -1) {
          ; // read till end
        }
      }
    }
  }



}
