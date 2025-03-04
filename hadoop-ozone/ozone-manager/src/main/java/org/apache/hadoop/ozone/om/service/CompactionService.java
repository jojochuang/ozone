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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * This is the background service to compact OM rocksdb tables.
 */
public class CompactionService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompactionService.class);

  // Use only a single thread for OpenKeyCleanup. Multiple threads would read
  // from the same table and can send deletion requests for same key multiple
  // times.
  private static final int OPEN_KEY_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;
  private final AtomicLong submittedOpenKeyCount;
  private final AtomicBoolean suspended;

  public CompactionService(OzoneManager ozoneManager,TimeUnit unit, long interval, long timeout,
                           ConfigurationSource conf) {
    super("OpenKeyCleanupService", interval, unit,
        OPEN_KEY_DELETING_CORE_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = this.ozoneManager.getMetadataManager();

    this.submittedOpenKeyCount = new AtomicLong(0);
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
   * Returns the number of open keys that were submitted for deletion by this
   * service. If these keys were committed from the open key table between
   * being submitted for deletion and the actual delete operation, they will
   * not be deleted.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getSubmittedOpenKeyCount() {
    return submittedOpenKeyCount.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new CompactTask(FILE_TABLE));
    queue.add(new CompactTask(KEY_TABLE));
    queue.add(new CompactTask(DIRECTORY_TABLE));
    queue.add(new CompactTask(DELETED_TABLE));
    queue.add(new CompactTask(DELETED_DIR_TABLE));
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get();
  }

  private class CompactTask implements BackgroundTask {
    String tableName;

    CompactTask(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      LOG.debug("Running CompactTask");
      long startTime = Time.monotonicNow();
      LOG.info("Compacting column family: {}",
          tableName);
      ManagedCompactRangeOptions options =
          new ManagedCompactRangeOptions();
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

      return () -> 1;
    }
  }
}
