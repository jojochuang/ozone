/**
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

package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the background service to compact OM rocksdb tables.
 */
public class CompactionService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompactionService.class);

  // Use only a single thread for Compaction.
  private static final int COMPACTOR_THREAD_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;
  private final AtomicLong numCompactions;
  private final AtomicBoolean suspended;
  private Set<String> tablesToCompact = new HashSet<>();
  // this counter is used by OMDirectoriesPurgeResponseWithFSO to track the number of directories deleted
  private final AtomicLong uncompactedDirDeletes = new AtomicLong(0);
  private final AtomicLong uncompactedFileDeletes = new AtomicLong(0);
  private final AtomicLong uncompactedDeletes = new AtomicLong(0);

  public CompactionService(OzoneManager ozoneManager, TimeUnit unit, long interval, long timeout,
      ConfigurationSource conf) {
    super("CompactionService", interval, unit,
        COMPACTOR_THREAD_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = this.ozoneManager.getMetadataManager();

    this.numCompactions = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
  }

  /**
   * Suspend the service (for testing).
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended (for testing).
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  /**
   * Returns the number of manual compactions performed.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getNumCompactions() {
    return numCompactions.get();
  }


  public AtomicLong getUncompactedDirDeletes() {
    return uncompactedDirDeletes;
  }

  public AtomicLong getUncompactedDeletes() {
    return uncompactedDeletes;
  }

  public AtomicLong getUncompactedFileDeletes() {
    return uncompactedFileDeletes;
  }

  public void compactDirectoryTableIfNeeded(long batchSize) throws IOException {

    String columnFamilyName = "directoryTable";
    long compactionThreshold = 10 * 1000;
    if (uncompactedDirDeletes.addAndGet(batchSize) < compactionThreshold) {
      return;
    }
    addTask(columnFamilyName);
    uncompactedDirDeletes.set(0);
  }

  public void compactFileTableIfNeeded(long batchSize) {
    String columnFamilyName = "fileTable";
    long compactionThreshold = 10 * 1000;
    if (uncompactedFileDeletes.addAndGet(batchSize) < compactionThreshold) {
      return;
    }
    addTask(columnFamilyName);
    uncompactedFileDeletes.set(0);
  }

  public void compactDeletedTableIfNeeded(long batchSize) {
    String columnFamilyName = "deletedTable";
    long compactionThreshold = 10 * 1000;
    if (uncompactedDeletes.addAndGet(batchSize) < compactionThreshold) {
      return;
    }
    addTask(columnFamilyName);
    uncompactedDeletes.set(0);
  }

  @Override
  public synchronized BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    for (String table : tablesToCompact) {
      queue.add(new CompactTask(table));
    }
    tablesToCompact.clear();
    return queue;
  }

  public synchronized void addTask(String tableName) {
    tablesToCompact.add(tableName);
  }

  private boolean shouldRun() {
    return !suspended.get();
  }

  private class CompactTask implements BackgroundTask {
    private final String tableName;

    CompactTask(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      // trigger full compaction for the specified table.
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      LOG.debug("Running CompactTask");
      long startTime = Time.monotonicNow();
      LOG.info("Compacting column family: {}", tableName);
      try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
        options.setBottommostLevelCompaction(
            ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
        // Find CF Handler
        RocksDatabase.ColumnFamily columnFamily =
            ((RDBStore) omMetadataManager.getStore()).getDb()
                .getColumnFamily(tableName);
        ((RDBStore) omMetadataManager.getStore()).getDb().compactRange(
            columnFamily, null, null, options);
        LOG.info("Compaction of column family: {} completed in {} ms",
            tableName, Time.monotonicNow() - startTime);
        numCompactions.incrementAndGet();
      }

      return () -> 1;
    }
  }
}
