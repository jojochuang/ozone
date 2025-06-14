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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.DeleteKeysResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 Background service responsible for purging deleted directories and files
 * in the Ozone Manager (OM) and associated snapshots.
 *
 * <p>
 * This service periodically scans the deleted directory table and submits
 * purge requests for directories and their sub-entries (subdirectories and files).
 * It operates in both the active object store (AOS) and across all deep-clean enabled
 * snapshots. The service supports parallel processing using a thread pool and
 * coordinates exclusive size calculations and cleanup status updates for
 * snapshots.
 * </p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>Processes deleted directories in both the active OM and all snapshots
 *       with deep cleaning enabled.</li>
 *   <li>Uses a thread pool to parallelize deletion tasks within each store or snapshot.</li>
 *   <li>Employs filters to determine reclaimability of directories and files,
 *       ensuring safety with respect to snapshot chains.</li>
 *   <li>Tracks and updates exclusive size and replicated exclusive size for each
 *       snapshot as directories and files are reclaimed.</li>
 *   <li>Updates the "deep cleaned" flag for snapshots after a successful run.</li>
 *   <li>Handles error and race conditions gracefully, deferring work if necessary.</li>
 * </ul>
 *
 * <h2>Constructor Parameters</h2>
 * <ul>
 *   <li><b>interval</b> - How often the service runs.</li>
 *   <li><b>unit</b> - Time unit for the interval.</li>
 *   <li><b>serviceTimeout</b> - Service timeout in the given time unit.</li>
 *   <li><b>ozoneManager</b> - The OzoneManager instance.</li>
 *   <li><b>configuration</b> - Ozone configuration object.</li>
 *   <li><b>dirDeletingServiceCorePoolSize</b> - Number of parallel threads for deletion per store or snapshot.</li>
 *   <li><b>deepCleanSnapshots</b> - Whether to enable deep cleaning for snapshots.</li>
 * </ul>
 *
 * <h2>Threading and Parallelism</h2>
 * <ul>
 *   <li>Uses a configurable thread pool for parallel deletion tasks within each store/snapshot.</li>
 *   <li>Each snapshot and AOS get a separate background task for deletion.</li>
 * </ul>
 *
 * <h2>Snapshot Integration</h2>
 * <ul>
 *   <li>Iterates all snapshots in the chain if deep cleaning is enabled.</li>
 *   <li>Skips snapshots that are already deep-cleaned or not yet flushed to disk.</li>
 *   <li>Updates snapshot metadata to reflect size changes and cleaning status.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Should be scheduled as a background service in OM.</li>
 *   <li>Intended to be run only on the OM leader node.</li>
 * </ul>
 *
 * @see org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter
 * @see org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter
 * @see org.apache.hadoop.ozone.om.SnapshotChainManager
 */
public class DirectoryDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryDeletingService.class);

  // Using multi thread for DirDeletion. Multiple threads would read
  // from parent directory info from deleted directory table concurrently
  // and send deletion requests.
  private int ratisByteLimit;
  private final SnapshotChainManager snapshotChainManager;
  private final boolean deepCleanSnapshots;
  private final ExecutorService deletionThreadPool;
  private final int numberOfParallelThreadsPerStore;
  private final AtomicLong deletedDirsCount;
  private final AtomicLong movedDirsCount;
  private final AtomicLong movedFilesCount;

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager,
      OzoneConfiguration configuration, int dirDeletingServiceCorePoolSize, boolean deepCleanSnapshots) {
    super(DirectoryDeletingService.class.getSimpleName(), interval, unit,
        dirDeletingServiceCorePoolSize, serviceTimeout, ozoneManager);
    int limit = (int) configuration.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    this.numberOfParallelThreadsPerStore = dirDeletingServiceCorePoolSize;
    this.deletionThreadPool = new ThreadPoolExecutor(0, numberOfParallelThreadsPerStore, interval, unit,
        new LinkedBlockingDeque<>(Integer.MAX_VALUE));

    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    registerReconfigCallbacks(ozoneManager.getReconfigurationHandler(), configuration);
    this.snapshotChainManager = ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();
    this.deepCleanSnapshots = deepCleanSnapshots;
    this.deletedDirsCount = new AtomicLong(0);
    this.movedDirsCount = new AtomicLong(0);
    this.movedFilesCount = new AtomicLong(0);
  }

  public void registerReconfigCallbacks(ReconfigurationHandler handler, OzoneConfiguration conf) {
    handler.registerCompleteCallback((changedKeys, newConf) -> {
      if (changedKeys.containsKey(OZONE_DIR_DELETING_SERVICE_INTERVAL)) {
        updateAndRestart(conf);
      }
    });
  }

  private synchronized void updateAndRestart(OzoneConfiguration conf) {
    long newInterval = conf.getTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL,
        OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    LOG.info("Updating and restarting DirectoryDeletingService with interval: {} {}",
        newInterval, TimeUnit.SECONDS.name().toLowerCase());
    shutdown();
    setInterval(newInterval, TimeUnit.SECONDS);
    start();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DirDeletingTask(null));
    if (deepCleanSnapshots) {
      Iterator<UUID> iterator = null;
      try {
        iterator = snapshotChainManager.iterator(true);
      } catch (IOException e) {
        LOG.error("Error while initializing snapshot chain iterator.");
        return queue;
      }
      while (iterator.hasNext()) {
        UUID snapshotId = iterator.next();
        queue.add(new DirDeletingTask(snapshotId));
      }
    }
    return queue;
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  void optimizeDirDeletesAndSubmitRequest(
      long dirNum, long subDirNum, long subFileNum,
      List<Pair<String, OmKeyInfo>> allSubDirList,
      List<PurgePathRequest> purgePathRequestList,
      DeletionContext context) {

    // Optimization to handle delete sub-dir and keys to remove quickly
    // This case will be useful to handle when depth of directory is high
    int subdirDelNum = 0;
    int subDirRecursiveCnt = 0;
    long remainingBufLimit = context.getRemainingBufLimit();
    int consumedSize = 0;
    while (subDirRecursiveCnt < allSubDirList.size() && remainingBufLimit > 0) {
      try {
        Pair<String, OmKeyInfo> stringOmKeyInfoPair = allSubDirList.get(subDirRecursiveCnt++);
        Boolean subDirectoryReclaimable = context.getReclaimableDirChecker().apply(
            Table.newKeyValue(stringOmKeyInfoPair.getKey(), stringOmKeyInfoPair.getValue()));
        Optional<PurgePathRequest> request = prepareDeleteDirRequest(
            stringOmKeyInfoPair.getValue(), stringOmKeyInfoPair.getKey(),
            subDirectoryReclaimable, allSubDirList, context);
        if (!request.isPresent()) {
          continue;
        }
        PurgePathRequest requestVal = request.get();
        consumedSize += requestVal.getSerializedSize();
        remainingBufLimit -= consumedSize;
        purgePathRequestList.add(requestVal);
        // Count up the purgeDeletedDir, subDirs and subFiles
        if (requestVal.hasDeletedDir() && !StringUtils.isBlank(requestVal.getDeletedDir())) {
          subdirDelNum++;
        }
        subDirNum += requestVal.getMarkDeletedSubDirsCount();
        subFileNum += requestVal.getDeletedSubFilesCount();
      } catch (IOException e) {
        LOG.error("Error while running delete directories and files " +
            "background task. Will retry at next run for subset.", e);
        break;
      }
    }
    if (!purgePathRequestList.isEmpty()) {
      submitPurgePaths(purgePathRequestList, context.getSnapshotTableKey(),
          context.getExpectedPreviousSnapshotId());
    }

    updateMetricsAndLogProgress(dirNum, subDirNum, subFileNum, subdirDelNum,
        Time.monotonicNow(), context.getRunCount());
  }

  /**
   * Updates metrics and logs progress of directory deletion operations.
   *
   * @param dirNum Number of directories deleted
   * @param subDirNum Number of subdirectories processed
   * @param subFileNum Number of files processed
   * @param subdirDelNum Number of subdirectories deleted
   * @param startTime Start time of the operation
   * @param runCount Current run count
   */
  private void updateMetricsAndLogProgress(long dirNum, long subDirNum, long subFileNum,
      int subdirDelNum, long startTime, long runCount) {
    if (dirNum != 0 || subDirNum != 0 || subFileNum != 0) {
      long subdirMoved = subDirNum - subdirDelNum;
      deletedDirsCount.addAndGet(dirNum + subdirDelNum);
      movedDirsCount.addAndGet(subdirMoved);
      movedFilesCount.addAndGet(subFileNum);
      long timeTakenInIteration = Time.monotonicNow() - startTime;
      LOG.info("Number of dirs deleted: {}, Number of sub-dir " +
              "deleted: {}, Number of sub-files moved:" +
              " {} to DeletedTable, Number of sub-dirs moved {} to " +
              "DeletedDirectoryTable, iteration elapsed: {}ms, " +
              " totalRunCount: {}",
          dirNum, subdirDelNum, subFileNum, (subDirNum - subdirDelNum),
          timeTakenInIteration, runCount);
      getMetrics().incrementDirectoryDeletionTotalMetrics(dirNum + subdirDelNum, subDirNum, subFileNum);
      getPerfMetrics().setDirectoryDeletingServiceLatencyMs(timeTakenInIteration);
    }
  }

  private OzoneManagerProtocolProtos.OMResponse submitPurgePaths(List<PurgePathRequest> requests,
      String snapTableKey, UUID expectedPreviousSnapshotId) {
    // Use the PurgeRequestBuilder utility to build the request
    OzoneManagerProtocolProtos.OMRequest omRequest =
        PurgeRequestBuilder.buildPurgeDirectoriesRequest(
            requests, snapTableKey, expectedPreviousSnapshotId, getClientId());

    // Submit Purge paths request to OM. Acquire bootstrap lock when processing deletes for snapshots.
    try (BootstrapStateHandler.Lock lock = snapTableKey != null ? getBootstrapStateLock().lock() : null) {
      return submitRequest(omRequest);
    } catch (ServiceException | InterruptedException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.", e);
    }
    return null;
  }

  /**
   * Submits multiple snapshot property update requests as a batch.
   *
   * @param requests List of snapshot property update requests
   * @return OMResponse for the batch request
   */
  private OzoneManagerProtocolProtos.OMResponse submitSetSnapshotRequests(
      List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> requests) {
    if (requests.isEmpty()) {
      return null;
    }

    OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.Builder combinedRequest =
        OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder();

    // Build a batch request containing all individual requests
    for (OzoneManagerProtocolProtos.SetSnapshotPropertyRequest request : requests) {
      if (request.hasDeepCleanedDeletedDir()) {
        combinedRequest.setDeepCleanedDeletedDir(request.getDeepCleanedDeletedDir());
      }
      if (request.hasSnapshotSizeDeltaFromDirDeepCleaning()) {
        combinedRequest.setSnapshotSizeDeltaFromDirDeepCleaning(
            request.getSnapshotSizeDeltaFromDirDeepCleaning());
      }
      combinedRequest.setSnapshotKey(request.getSnapshotKey());
    }

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(combinedRequest)
            .setClientId(getClientId().toString())
            .build();

    try {
      return submitRequest(omRequest);
    } catch (ServiceException e) {
      LOG.error("Setting snapshot property failed. Will retry at next run.", e);
    }
    return null;
  }

  @VisibleForTesting
  final class DirDeletingTask implements BackgroundTask {
    private final UUID snapshotId;

    DirDeletingTask(UUID snapshotId) {
      this.snapshotId = snapshotId;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        long exclusiveSize, long exclusiveReplicatedSize, UUID snapshotID) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(exclusiveSize)
          .setExclusiveReplicatedSize(exclusiveReplicatedSize)
          .build();
      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotChainManager.getTableKey(snapshotID))
          .setSnapshotSizeDeltaFromDirDeepCleaning(snapshotSize)
          .build();
    }

    /**
     * Processes deleted directories for the specified snapshot or AOS.
     *
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     * @param remainingBufLimit Remaining buffer limit for processing.
     * @param runCount Current run count.
     */
    private void processDeletedDirsForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        long remainingBufLimit, long runCount) throws IOException, ExecutionException, InterruptedException {

      String volume = null;
      String bucket = null;
      String snapshotTableKey = null;

      if (currentSnapshotInfo != null) {
        volume = currentSnapshotInfo.getVolumeName();
        bucket = currentSnapshotInfo.getBucketName();
        snapshotTableKey = currentSnapshotInfo.getTableKey();
      }

      // Create supplier for deleted directories
      DeletedDirSupplier dirSupplier = new DeletedDirSupplier(currentSnapshotInfo == null ?
          keyManager.getDeletedDirEntries() : keyManager.getDeletedDirEntries(volume, bucket));

      try {
        // Determine the expected previous snapshot ID
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);

        // Create map to track exclusive size information for snapshots
        Map<UUID, Pair<Long, Long>> exclusiveSizeMap = Maps.newConcurrentMap();

        // Create the deletion context with all required parameters
        DeletionContext context = new DeletionContext.Builder()
            .withCurrentSnapshotInfo(currentSnapshotInfo)
            .withKeyManager(keyManager)
            .withExpectedPreviousSnapshotId(expectedPreviousSnapshotId)
            .withRemainingBufLimit(remainingBufLimit)
            .withRunCount(runCount)
            .withExclusiveSizeMap(exclusiveSizeMap)
            .withDirSupplier(dirSupplier)
            .build();

        // Process deletions in parallel using the thread pool
        CompletableFuture<Boolean> processedAllDeletedDirs = processDeletedDirsInParallel(context);

        // If all directories have been processed successfully, update snapshot properties
        if (processedAllDeletedDirs.get()) {
          updateSnapshotProperties(currentSnapshotInfo, exclusiveSizeMap, snapshotTableKey);
        }
      } finally {
        // Ensure dirSupplier is closed properly
        dirSupplier.close();
      }
    }

    /**
     * Processes deleted directories in parallel using the thread pool.
     *
     * @param context The deletion context
     * @return CompletableFuture indicating whether all directories were processed
     */
    private CompletableFuture<Boolean> processDeletedDirsInParallel(DeletionContext context) {
      CompletableFuture<Boolean> processedAllDeletedDirs = CompletableFuture.completedFuture(true);

      // Create and submit multiple parallel tasks
      for (int i = 0; i < numberOfParallelThreadsPerStore; i++) {
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
          try {
            return processDeletedDirectories(context);
          } catch (Throwable e) {
            LOG.warn("Error processing deleted directories", e);
            return false;
          }
        }, deletionThreadPool);

        // Combine results - only true if all tasks succeed
        processedAllDeletedDirs = processedAllDeletedDirs.thenCombine(future, (a, b) -> a && b);
      }

      return processedAllDeletedDirs;
    }

    /**
     * Updates snapshot properties after processing deleted directories.
     *
     * @param currentSnapshotInfo Current snapshot being processed
     * @param exclusiveSizeMap Map of snapshot IDs to exclusive sizes
     * @param snapshotTableKey Key for the current snapshot
     */
    private void updateSnapshotProperties(SnapshotInfo currentSnapshotInfo,
        Map<UUID, Pair<Long, Long>> exclusiveSizeMap, String snapshotTableKey) {

      List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();

      // Add size update requests for each affected snapshot
      for (Map.Entry<UUID, Pair<Long, Long>> entry : exclusiveSizeMap.entrySet()) {
        UUID snapshotID = entry.getKey();
        long exclusiveSize = entry.getValue().getLeft();
        long exclusiveReplicatedSize = entry.getValue().getRight();
        setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(
            exclusiveSize, exclusiveReplicatedSize, snapshotID));
      }

      // If processing a snapshot, mark it as deep-cleaned
      if (currentSnapshotInfo != null) {
        setSnapshotPropertyRequests.add(OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
            .setSnapshotKey(snapshotTableKey)
            .setDeepCleanedDeletedDir(true)
            .build());
      }

      // Submit all property update requests
      submitSetSnapshotRequests(setSnapshotPropertyRequests);
    }
  }
}

