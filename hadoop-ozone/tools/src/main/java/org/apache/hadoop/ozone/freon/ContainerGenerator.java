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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
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

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;

import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator.
 *
 * Single user.
 */
@Command(name = "cg",
    aliases = "container-generator",
    description = "Generates containers",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ContainerGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerGenerator.class);

  @Option(names = {"-u", "--user-id"},
      description = "Owner of the files",
      defaultValue = "weichiu")
  private static String userId;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"--block-per-container"},
      description = "Number of blocks per container",
      defaultValue = "102400")
  private int blockPerContainer;

  @Option(names = {"--cluster-id"},
      description = "UUID of the Cluster ID",
      required = true)
  private static String clusterId;

  @Option(names = {"--datanode-id"},
      description = "UUID of the datanodes, separated by ,",
      required = true)
  private static String datanodeId;

  @Option(names = {"--key-id-offset"},
      description = "offset of the key ID",
      defaultValue = "0")
  private long keyIdOffset;

  @Option(names = {"--container-id-offset"},
      description = "offset of the container ID",
      defaultValue = "0")
  private long containerIdOffset;

  @Option(names = {"--container-id-increment"},
      description = "increment of the container ID",
      defaultValue = "1")
  private long containerIdOIncrement;

  @Option(names = {"--scm-id"},
      description = "UUID of the SCM",
      required = true)
  private static String scmId;

  @Option(names = {"--write-container"},
      description = "Write to DN container DB")
  private boolean writeContainer;

  @Option(names = {"--write-om"},
      description = "Write data to the OM db")
  private boolean writeOm;

  @Option(names = {"--write-dn"},
      description = "Write chunk files and rocksdb for DataNode.")
  private boolean writeDatanode;

  @Option(names = {"--write-scm"},
      description = "Write data to the SCM db.")
  private boolean writeScm;

  @Option(names = {"--om-key-batch-size"},
      description = "Size of the batch for OM key insertion",
      defaultValue = "1000")
  private long omKeyBatchSize;

  @Option(names = {"--repl"},
      description = "replication factor, either 1 or 3",
      defaultValue = "3")
  private int replicationFactor;

  private ChunkManager chunkManager;

  private static List<Pipeline> pipelines;

  private byte[] data;
  ChecksumData checksumData;
  ContainerProtos.ChecksumData checksumDataProtoBuf;

  private static OzoneConfiguration ozoneConfiguration;
  private static OzoneConfiguration ozoneConfigurationCreateDB;

  // FIXME: close it when the container is full, right away.
  // Keep 1000 containers open, expire after 1 minute.
  private LoadingCache<Long, KeyValueContainer> containersCache =
      CacheBuilder.newBuilder()
      //.expireAfterAccess(1, TimeUnit.MINUTES)
          .maximumSize(100)
          //.removalListener( x -> LOG.info("removing container {} from cache.", x.getKey()))
          .build(new ContainerCreator());

  private Timer timer;

  private static VolumeSet volumeSet;

  private static Table<ContainerID, ContainerInfo> containerStore;
  private static Table<PipelineID, Pipeline> pipelinesStore;

  private DBStore omDb;

  private DBStore scmDb;

  private static String volumeName = "vol1";

  private static String bucketName = "bucket1";

  Table<String, OmKeyInfo> omKeyTable;

  private static VolumeChoosingPolicy volumeChoosingPolicy;

  public ContainerGenerator() {

  }

  public static String getVolumeName() {
    return volumeName;
  }

  public static String getBucketName() {
    return bucketName;
  }

  @Override
  public Void call() throws Exception {
    try {
      init();

      initPipelineList();

      // this is use for operations that opens an existing RocksDB db, and which
      // is not expected to create a new one.
      ozoneConfiguration = createOzoneConfiguration();

      // this is use for operations that may create a new db.
      ozoneConfigurationCreateDB = createOzoneConfiguration();

      if (writeScm) {
        initializeSCM();
      }

      if (writeDatanode) {
        initializeDataNode();
      }

      if (writeOm) {
        initializeOM();
      }

      timer = getMetrics().timer("chunk-generate");

      runTests(this::writeContainer);

    } finally {

      containersCache.asMap().forEach( (k, v) -> {
        try {
          // TODO: lock?
          if (!v.getContainerData().isClosed()) {
            LOG.info("Close container {}.", v.getContainerData().getContainerID());
            v.close();
          }
        } catch (StorageContainerException e) {
          e.printStackTrace();
        }
      });
      containersCache.invalidateAll();

      if (omDb == null) {
        LOG.warn("OM DB object uninitialized. Skip clean up");
      } else {
        omKeyTable.close();

        omDb.close();
      }

      if (chunkManager != null) {
        chunkManager.shutdown();
      }
      if (containerStore != null) {
        containerStore.close();
      }

      if (scmDb != null) {
        scmDb.close();
      }
    }
    return null;
  }

  private void initPipelineList() {
    // assuming the number of datanodes is an exact multiple of replication factor
    // e.g., 6, 9, 12 DNs for rep=3
    String[] dataNodeUuids = StringUtils.split(datanodeId, ',');
    //int rep = replicationFactor.getNumber();
    int numPipeline = dataNodeUuids.length / replicationFactor;

    List<DatanodeDetails> dnDetailsList = new ArrayList<>();

    pipelines = new ArrayList<>();

    for (int i = 0; i < dataNodeUuids.length; i++) {
      DatanodeDetails dnDetails = DatanodeDetails.newBuilder()
          .setUuid(UUID.nameUUIDFromBytes(dataNodeUuids[i].getBytes()))
          .setIpAddress("127.0.0.1")
          .setHostName("localhost")
          //.addPort(DatanodeDetails.newPort(Name.RATIS, 9858))
          .addPort(DatanodeDetails.newPort(Name.STANDALONE, 0))
          .build();
      dnDetailsList.add(dnDetails);
    }

    for (int i=0; i < numPipeline; i++) {
      List<DatanodeDetails> locations = new ArrayList<>();
      for (int j = 0; j < replicationFactor; j++) {
        locations.add(dnDetailsList.get(i * replicationFactor + j));
      }

      Pipeline pipeline = Pipeline.newBuilder()
          .setId(PipelineID.randomId())
          //.setType(ReplicationType.RATIS)
          .setType(ReplicationType.STAND_ALONE)
          .setFactor(ReplicationFactor.valueOf(replicationFactor))
          .setState(PipelineState.CLOSED)
          .setLeaderId(locations.get(0).getUuid())
          .setNodes(locations)
          .build();
      pipelines.add(pipeline);
    }
  }

  static private class ContainerCreator extends CacheLoader<Long, KeyValueContainer> {
    @Override
    public KeyValueContainer load(Long containerId) {
      try {
        return createContainer(containerId);
      } catch (IOException e) {
        LOG.warn("Unable to load container {} into cache.", containerId, e);
        return null;
      }
    }
  }

  /**
   * determine the datanode of the pipeline for the specified container.
   * @param containerId
   * @return
   */
  private static Pipeline getPipelineForContainer(long containerId) {
    return pipelines.get((int)(containerId % pipelines.size()));
  }

  private void initializeDataNode() throws IOException {
    initializeSharedChunkForDataNode();

    BlockManager blockManager = new BlockManagerImpl(ozoneConfiguration);
    chunkManager = ChunkManagerFactory.createChunkManager(ozoneConfiguration, blockManager);

    volumeSet = new MutableVolumeSet(datanodeId, clusterId,
        ozoneConfiguration);

    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
  }

  private void initializeSharedChunkForDataNode() throws OzoneChecksumException {
    //data = RandomStringUtils.randomAscii(chunkSize)
    //    .getBytes(StandardCharsets.UTF_8);
    data = new  byte[chunkSize]; // initialize an array of zero's.
    // FIXME: I'm not sure why the byte buffer can't be shared. (fails to write if I share between threads)

    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    // Use CRC32, 1024x1024 bytes per checksum

    Checksum checksum = new Checksum(ChecksumType.CRC32,
        ozoneConfiguration.getInt(OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT,
            OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES));
    checksumData = checksum.computeChecksum(byteBuffer);
    checksumDataProtoBuf = checksumData.getProtoBufMessage();
  }

  /**
   * initialize data structures required for SCM.
   * @throws IOException
   */
  private void initializeSCM() throws IOException {
    // TODO: enable the configuration to make SCM learn about new containers
    // added to DNs.
    scmDb = DBStoreBuilder.createDBStore(ozoneConfiguration, new SCMDBDefinition());
    containerStore = CONTAINERS.getTable(scmDb);
    writeSCMPipeline();
  }

  /**
   * initialize data structures for OM.
   * @throws IOException
   */
  private void initializeOM() throws IOException {
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

    // initialization: create one bucket and volume in OM.
    if (writeOm) {
      writeOmBucketVolume();
    }

    omKeyTable = omDb.getTable(OmMetadataManagerImpl.KEY_TABLE, String.class,
        OmKeyInfo.class);
  }

  private static KeyValueContainer createContainer(long containerId)
      throws IOException {
    //LOG.info("creating container {}", containerId);
    // DN
    ChunkLayOutVersion layoutVersion = ChunkLayOutVersion.getConfiguredVersion(ozoneConfiguration);
    KeyValueContainerData keyValueContainerData =
        new KeyValueContainerData(containerId, layoutVersion, 1_000_000L,
            getPrefix(), datanodeId);

    KeyValueContainer keyValueContainer = new KeyValueContainer(keyValueContainerData, ozoneConfigurationCreateDB);

    try {
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    } catch (StorageContainerException ex) {
      ex.printStackTrace();
    }

    return keyValueContainer;
  }

  private void writeContainer(long l) throws Exception {
    //based on the thread naming convention: pool-1-thread-n

    //long keyId = l + keyIdOffset;

    long containerId = (l + keyIdOffset / blockPerContainer) + 1;

    if (containerId % containerIdOIncrement != containerIdOffset) {
      // Used for generating DN storage.
      // Skip writing key if the corresponding container does not exist
      // for this particular DN.
      return;
    }

    timer.time(() -> {
      try {

        if (writeDatanode) {
          KeyValueContainer container = containersCache.get(containerId);
          for (long keyId = (containerId-1) * blockPerContainer ;
               keyId< (containerId) * blockPerContainer; keyId++) {
            BlockID blockId = new BlockID(containerId, keyId);
            String chunkName = "chunk" + keyId;
            ChunkInfo chunkInfo = new ChunkInfo(chunkName, 0, chunkSize);

            writeChunk(keyId, container, blockId, chunkInfo);
            writeContainer(container, blockId, chunkInfo);
          }
        }

        if (writeScm) {
          writeScmData(containerId);
        }

        if (writeOm) {
          BatchOperation omKeyTableBatchOperation = omDb.initBatchOperation();
          for (long keyId = (containerId-1) * blockPerContainer ;
               keyId< (containerId) * blockPerContainer ; keyId++) {
            BlockID blockId = new BlockID(containerId, keyId);

            writeOmData(keyId, blockId, omKeyTableBatchOperation);
          }
          commitAndResetOMKeyTableBatchOperation(omKeyTableBatchOperation);
        }

      } catch (StorageContainerException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

  }

  private void writeSCMPipeline() throws IOException {
    // write pipelines to db only once at beginning.
    pipelinesStore = PIPELINES.getTable(scmDb);

    for (Pipeline pipeline : pipelines) {
      pipelinesStore.put(pipeline.getId(), pipeline);
    }
  }

  private void writeOmBucketVolume() throws IOException {
    // TODO: multiple volumes and buckets

    Table<String, OmVolumeArgs> volTable =
        omDb.getTable(OmMetadataManagerImpl.VOLUME_TABLE, String.class,
            OmVolumeArgs.class);

    String admin = userId;
    String owner = userId;

    OmVolumeArgs omVolumeArgs = new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(admin).setCreationTime(Time.now()).setOwnerName(owner)
        .setObjectID(1L).setUpdateID(1L).setQuotaInBytes(100L)
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.WORLD, "",
                IAccessAuthorizer.ACLType.ALL, ACCESS)))
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER, userId,
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


  private void writeScmData(long containerId) throws IOException {
    // SCM
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerId)
            .setState(LifeCycleState.CLOSED)
            .setReplicationFactor(ReplicationFactor.valueOf(replicationFactor))
            //.setReplicationType(ReplicationType.RATIS)
            .setReplicationType(ReplicationType.STAND_ALONE)
            .setPipelineID(getPipelineForContainer(containerId).getId())
            .setOwner(userId)
            .build();

    containerStore.put(new ContainerID(containerId), containerInfo);
  }

  private void writeOmData(long l, BlockID blockId, BatchOperation omKeyTableBatchOperation) throws IOException {
    Pipeline pipeline = getPipelineForContainer(blockId.getContainerID());

    List<OmKeyLocationInfo> omkl = new ArrayList<>();
    omkl.add(new OmKeyLocationInfo.Builder()
        .setBlockID(blockId)
        .setLength(chunkSize)
        .setOffset(0)
        .setPipeline(pipeline)
        .build());

    OmKeyLocationInfoGroup infoGroup = new OmKeyLocationInfoGroup(0, omkl);

    long l4n = l % 1_000;
    long l3n = l / 1_000 % 1_000;
    long l2n = l / 1_000_000 % 1_000;
    long l1n = l / 1_000_000_000 % 1_000;

    String level3 = "L3-" + l3n;
    String level2 = "L2-" + l2n;
    String level1 = "L1-" + l1n;

    if (l2n == 0 && l3n == 0 && l4n == 0) {
      // create l1 directory
      addDirectoryKey(level1 + "/", omKeyTableBatchOperation);
    }

    if (l3n == 0 && l4n == 0) {
      // create l2 directory
      addDirectoryKey(level1 + "/" + level2 + "/", omKeyTableBatchOperation);
    }

    if (l4n == 0) {
      // create l3 directory
      addDirectoryKey(level1 + "/" + level2 + "/" + level3 + "/", omKeyTableBatchOperation);
    }

    String keyName = "/vol1/bucket1/" + level1 + "/" + level2 + "/" + level3 + "/key" + l;

    OmKeyInfo keyInfo = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(level1 + "/" + level2 + "/" + level3 + "/key" + l)
        .setDataSize(chunkSize)
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setReplicationFactor(ReplicationFactor.valueOf(replicationFactor))
        //.setReplicationType(ReplicationType.RATIS)
        .setReplicationType(ReplicationType.STAND_ALONE)
        .addOmKeyLocationInfoGroup(infoGroup)
        .build();

    omKeyTable.putWithBatch(omKeyTableBatchOperation, keyName, keyInfo);
    LOG.debug("Add {} to OM db", keyName);

  }

  private void addDirectoryKey(String keyName, BatchOperation omKeyTableBatchOperation) throws IOException {
    OmKeyInfo l3DirInfo = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(0)
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setReplicationFactor(ReplicationFactor.ONE)
        .setReplicationType(ReplicationType.RATIS)
        .build();
    omKeyTable.putWithBatch(omKeyTableBatchOperation, "/" + volumeName + "/" + bucketName + "/" + keyName, l3DirInfo);

    LOG.debug("add {} to {}:{}", keyName, volumeName, bucketName);
  }

  private void commitAndResetOMKeyTableBatchOperation(BatchOperation omKeyTableBatchOperation) throws IOException {
    LOG.debug("Commit to OM DB key table");

    omDb.commitBatchOperation(omKeyTableBatchOperation);
    omKeyTableBatchOperation.close();
  }

  private void writeContainer(KeyValueContainer container, BlockID blockId,
      ChunkInfo chunkInfo) throws IOException {
    BlockData blockData = new BlockData(blockId);

    blockData.addChunk(ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(chunkInfo.getChunkName())
        .setLen(chunkInfo.getLen())
        .setOffset(chunkInfo.getOffset())
        .setChecksumData(checksumDataProtoBuf)
        .build());

    BlockManagerImpl
        .persistPutBlock(container, blockData, ozoneConfiguration, true);

    if (container.getContainerData().getKeyCount() == blockPerContainer) {
      // the container is full. close it.
      //LOG.info("The container {} is full. Close it.",
      //    container.getContainerData().getContainerID());
      containersCache.invalidate(container.getContainerData().getContainerID());
      container.close();
    }
  }

  private void writeChunk(long l, KeyValueContainer container, BlockID blockId,
      ChunkInfo chunkInfo) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);

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
