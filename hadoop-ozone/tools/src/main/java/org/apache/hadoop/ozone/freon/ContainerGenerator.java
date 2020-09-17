/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo.Builder;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;

import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator to use pure datanode XCeiver interface.
 */
@Command(name = "cg",
    aliases = "container-generator",
    description = "Generates containers",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ContainerGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerGenerator.class);

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"--block-per-container"},
      description = "Number of blocks per container",
      defaultValue = "102400")
  private int blockPerContainer;

  @Option(names = {"--datanode-id"},
      description = "UUID of the datanode",
      required = true)
  private String datanodeId;

  @Option(names = {"--scm-id"},
      description = "UUID of the SCM",
      required = true)
  private String scmId;

  @Option(names = {"--write-scm"},
      description = "Write data to the SCM")
  private boolean writeScm;

  @Option(names = {"--write-om"},
      description = "Write data to the OM db")
  private boolean writeOm;

  @Option(names = {"--write-datanode"},
      description = "Write chunk files.")
  private boolean writeDatanode;

  private boolean generateScm;

  private ChunkManager chunkManager;

  private byte[] data;

  private OzoneConfiguration ozoneConfiguration;

  private Map<Long, KeyValueContainer> containers =
      new ConcurrentHashMap<>();

  private Timer timer;

  private VolumeSet volumeSet;

  private Table<ContainerID, ContainerInfo> containerStore;

  private DBStore omDb;

  private DBStore scmDb;

  private String volumeName = "vol1";

  private String bucketName = "bucket1";


  public ContainerGenerator() {

  }

  @Override
  public Void call() throws Exception {

    try {
      init();
      ozoneConfiguration = createOzoneConfiguration();

      // TODO: enable the configuration to make SCM learn about new containers
      // added to DNs.
      scmDb = DBStoreBuilder.createDBStore(ozoneConfiguration, new SCMDBDefinition());
      containerStore = CONTAINERS.getTable(scmDb);

      // TODO: the default DB profile uses cache size 256MB.
      //  Consider lowering to 10MB to make it consistent with the old code.
      File metaDir = OMStorage.getOmDbDir(ozoneConfiguration);

      RocksDBConfiguration rocksDBConfiguration =
          ozoneConfiguration.getObject(RocksDBConfiguration.class);

      DBStoreBuilder dbStoreBuilder =
          DBStoreBuilder.newBuilder(ozoneConfiguration,
              rocksDBConfiguration).setName(OM_DB_NAME)
              .setPath(Paths.get(metaDir.getPath()));

      OmMetadataManagerImpl.addOMTablesAndCodecs(dbStoreBuilder);

      omDb = dbStoreBuilder.build();

      volumeSet = new MutableVolumeSet(datanodeId, "clusterid",
          ozoneConfiguration);

      data = RandomStringUtils.randomAscii(chunkSize)
          .getBytes(StandardCharsets.UTF_8);

      BlockManager blockManager = new BlockManagerImpl(ozoneConfiguration);

      chunkManager = ChunkManagerFactory.createChunkManager(ozoneConfiguration, blockManager);

      if (writeOm) {
        writeOmBucketVolume();
      }


      timer = getMetrics().timer("chunk-generate");

      runTests(this::writeKey);

      // FIXME: too many open container isn't scalable.
      for (KeyValueContainer container : containers.values()) {
        container.close();
      }

    } finally {
      if (chunkManager != null) {
        chunkManager.shutdown();
      }
      if (this.containerStore != null) {
        this.containerStore.close();
      }
    }
    return null;
  }

  private KeyValueContainer getOrCreateContainer(long containerId)
      throws IOException {

    if (!containers.containsKey(containerId)) {
      ChunkLayOutVersion layoutVersion =
          ChunkLayOutVersion.getConfiguredVersion(ozoneConfiguration);
      KeyValueContainerData keyValueContainerData =
          new KeyValueContainerData(containerId,
              layoutVersion,
              1_000_000L,
              getPrefix(),
              datanodeId);

      KeyValueContainer keyValueContainer =
          new KeyValueContainer(keyValueContainerData, ozoneConfiguration);

      try {
        keyValueContainer
            .create(volumeSet, new RoundRobinVolumeChoosingPolicy(), scmId);
      } catch (StorageContainerException ex) {
        ex.printStackTrace();
      }
      containers.put(containerId, keyValueContainer);

      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(containerId)
          .setState(LifeCycleState.CLOSED)
          .setReplicationFactor(ReplicationFactor.THREE)
          .setReplicationType(ReplicationType.RATIS)
          .setPipelineID(PipelineID.randomId())
          .setOwner("hadoop")
          .build();

      containerStore.put(new ContainerID(containerId), containerInfo);

    }
    return containers.get(containerId);
  }

  private void writeKey(long l) throws Exception {
    //based on the thread naming convention: pool-1-thread-n

    long containerId = l / blockPerContainer + 1;
    KeyValueContainer container = getOrCreateContainer(containerId);

    BlockID blockId = new BlockID(containerId, l);
    String chunkName = "chunk" + l;
    ChunkInfo chunkInfo = new ChunkInfo(chunkName, 0, chunkSize);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);

    timer.time(() -> {
      try {

        if (writeDatanode) {
          writeChunk(l, container, blockId, chunkInfo, byteBuffer);
        }

        if (writeScm) {
          writeScmData(container, blockId, chunkInfo);
        }

        if (writeOm) {
          writeOmData(l, blockId);
        }

      } catch (StorageContainerException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

  }

  private void writeOmBucketVolume() throws IOException {
    Table<String, OmVolumeArgs> volTable =
        omDb.getTable(OmMetadataManagerImpl.VOLUME_TABLE, String.class,
            OmVolumeArgs.class);

    String admin = "weichiu";
    String owner = "weichiu";

    OmVolumeArgs omVolumeArgs = new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(admin).setCreationTime(Time.now()).setOwnerName(owner)
        .setObjectID(1L).setUpdateID(1L).setQuotaInBytes(100L)
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.WORLD, "",
                IAccessAuthorizer.ACLType.ALL, ACCESS)))
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER, "weichiu",
                IAccessAuthorizer.ACLType.ALL, ACCESS))
            ).build();

    volTable.put("/" + volumeName, omVolumeArgs);

    Table<String, OmBucketInfo> bucketTable =
        omDb.getTable(OmMetadataManagerImpl.BUCKET_TABLE, String.class,
            OmBucketInfo.class);

    OmBucketInfo omBucketInfo = new OmBucketInfo.Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName).build();
    bucketTable.put("/" + volumeName + "/" + bucketName, omBucketInfo);

  }

  private void writeOmData(long l, BlockID blockId) throws IOException {
    Table<String, OmKeyInfo> table =
        omDb.getTable(OmMetadataManagerImpl.KEY_TABLE, String.class,
            OmKeyInfo.class);

    DatanodeDetails dn2 = DatanodeDetails.newBuilder()
        .setUuid(UUID.nameUUIDFromBytes(datanodeId.getBytes()))
        .setIpAddress("127.0.0.1")
        .setHostName("localhost")
        .addPort(DatanodeDetails.newPort(Name.RATIS, 9858))
        .build();

    List<DatanodeDetails> locations = new ArrayList<>();
    locations.add(dn2);

    List<OmKeyLocationInfo> omkl = new ArrayList<>();
    omkl.add(new OmKeyLocationInfo.Builder()
        .setBlockID(blockId)
        .setLength(chunkSize)
        .setOffset(0)
        .setPipeline(Pipeline.newBuilder()
            .setId(PipelineID.randomId())
            .setType(ReplicationType.RATIS)
            .setFactor(ReplicationFactor.THREE)
            .setState(PipelineState.CLOSED)
            .setLeaderId(UUID.fromString(datanodeId))
            .setNodes(locations)
            .build())
        .build());

    OmKeyLocationInfoGroup infoGroup = new OmKeyLocationInfoGroup(0, omkl);

    OmKeyInfo keyInfo = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName("key" + l)
        .setDataSize(chunkSize)
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setReplicationFactor(ReplicationFactor.THREE)
        .setReplicationType(ReplicationType.RATIS)
        .addOmKeyLocationInfoGroup(infoGroup)
        .build();
    String level3 = "L3-" + l / 1_000 % 1_000;
    String level2 = "L2-" + l / 1_000_000 % 1_000;
    String level1 = "L1-" + l / 1_000_000_000 % 1_000;
    table.put(
        "/vol1/bucket1/" + level1 + "/" + level2 + "/" + level3 + "/key"
            + l, keyInfo);

    // TODO: add to volume and bucket tables?
  }

  private void writeScmData(KeyValueContainer container, BlockID blockId,
      ChunkInfo chunkInfo) throws IOException {
    BlockData blockData = new BlockData(blockId);
    blockData.addChunk(ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(chunkInfo.getChunkName())
        .setLen(chunkInfo.getLen())
        .setOffset(chunkInfo.getOffset())
        .setChecksumData(ChecksumData.newBuilder().setBytesPerChecksum(1)
            .setType(ChecksumType.NONE).build())
        .build());

    BlockManagerImpl
        .persistPutBlock(container, blockData, ozoneConfiguration, true);
  }

  private void writeChunk(long l, KeyValueContainer container, BlockID blockId,
      ChunkInfo chunkInfo, ByteBuffer byteBuffer) throws IOException {
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setStage(WriteChunkStage.WRITE_DATA)
            .setTerm(1L)
            .setLogIndex(l)
            .setReadFromTmpFile(false)
            .build();
    chunkManager
        .writeChunk(container, blockId, chunkInfo,
            byteBuffer,
            context);

    context =
        new DispatcherContext.Builder()
            .setStage(WriteChunkStage.COMMIT_DATA)
            .setTerm(1L)
            .setLogIndex(l)
            .setReadFromTmpFile(false)
            .build();
    chunkManager
        .writeChunk(container, blockId, chunkInfo,
            byteBuffer,
            context);

    chunkManager.finishWriteChunks(container, new BlockData(blockId));
  }

}
