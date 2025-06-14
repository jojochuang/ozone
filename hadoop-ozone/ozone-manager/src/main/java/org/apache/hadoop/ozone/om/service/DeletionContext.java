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

import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.ratis.util.function.CheckedFunction;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Context object for directory deletion operations, encapsulating parameters
 * needed for deletion tasks.
 */
public class DeletionContext {
  private final SnapshotInfo currentSnapshotInfo;
  private final KeyManager keyManager;
  private final String snapshotTableKey;
  private final UUID expectedPreviousSnapshotId;
  private long remainingBufLimit;
  private final long runCount;
  private final Map<UUID, Pair<Long, Long>> exclusiveSizeMap;
  private final CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> reclaimableDirChecker;
  private final CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> reclaimableFileChecker;
  private final DeletedDirSupplier dirSupplier;

  /**
   * Constructor for DeletionContext.
   */
  private DeletionContext(Builder builder) {
    this.currentSnapshotInfo = builder.currentSnapshotInfo;
    this.keyManager = builder.keyManager;
    this.snapshotTableKey = builder.currentSnapshotInfo == null ?
        null : builder.currentSnapshotInfo.getTableKey();
    this.expectedPreviousSnapshotId = builder.expectedPreviousSnapshotId;
    this.remainingBufLimit = builder.remainingBufLimit;
    this.runCount = builder.runCount;
    this.exclusiveSizeMap = builder.exclusiveSizeMap;
    this.reclaimableDirChecker = builder.reclaimableDirChecker;
    this.reclaimableFileChecker = builder.reclaimableFileChecker;
    this.dirSupplier = builder.dirSupplier;
  }

  public SnapshotInfo getCurrentSnapshotInfo() {
    return currentSnapshotInfo;
  }

  public KeyManager getKeyManager() {
    return keyManager;
  }

  public String getSnapshotTableKey() {
    return snapshotTableKey;
  }

  public UUID getExpectedPreviousSnapshotId() {
    return expectedPreviousSnapshotId;
  }

  public long getRemainingBufLimit() {
    return remainingBufLimit;
  }

  /**
   * Decrements the remaining buffer limit by the specified amount.
   *
   * @param amount Amount to decrement by
   */
  public void decrementRemainingBufLimit(long amount) {
    this.remainingBufLimit -= amount;
  }

  public long getRunCount() {
    return runCount;
  }

  public Map<UUID, Pair<Long, Long>> getExclusiveSizeMap() {
    return exclusiveSizeMap;
  }

  public CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> getReclaimableDirChecker() {
    return reclaimableDirChecker;
  }

  public CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> getReclaimableFileChecker() {
    return reclaimableFileChecker;
  }

  /**
   * Gets the supplier of deleted directories.
   *
   * @return The deleted directory supplier
   */
  public DeletedDirSupplier getDirSupplier() {
    return dirSupplier;
  }

  /**
   * Builder for DeletionContext.
   */
  public static class Builder {
    private SnapshotInfo currentSnapshotInfo;
    private KeyManager keyManager;
    private UUID expectedPreviousSnapshotId;
    private long remainingBufLimit;
    private long runCount;
    private Map<UUID, Pair<Long, Long>> exclusiveSizeMap;
    private CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> reclaimableDirChecker;
    private CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> reclaimableFileChecker;
    private DeletedDirSupplier dirSupplier;

    public Builder withCurrentSnapshotInfo(SnapshotInfo info) {
      this.currentSnapshotInfo = info;
      return this;
    }

    public Builder withKeyManager(KeyManager manager) {
      this.keyManager = manager;
      return this;
    }

    public Builder withExpectedPreviousSnapshotId(UUID id) {
      this.expectedPreviousSnapshotId = id;
      return this;
    }

    public Builder withRemainingBufLimit(long limit) {
      this.remainingBufLimit = limit;
      return this;
    }

    public Builder withRunCount(long count) {
      this.runCount = count;
      return this;
    }

    public Builder withExclusiveSizeMap(Map<UUID, Pair<Long, Long>> sizeMap) {
      this.exclusiveSizeMap = sizeMap;
      return this;
    }

    public Builder withReclaimableDirChecker(
        CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> checker) {
      this.reclaimableDirChecker = checker;
      return this;
    }

    public Builder withReclaimableFileChecker(
        CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, Exception> checker) {
      this.reclaimableFileChecker = checker;
      return this;
    }

    public Builder withDirSupplier(DeletedDirSupplier supplier) {
      this.dirSupplier = supplier;
      return this;
    }

    public DeletionContext build() {
      return new DeletionContext(this);
    }
  }
}
