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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CompactTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      CompactTask.class);
  private final OzoneManager om;

  public CompactTask(OzoneManager ozoneManager) {
    this.om = ozoneManager;
  }

  public CompletableFuture<Void> compact(String columnFamily) throws
      IOException {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return compactAsync(columnFamily);
      } catch (IOException e) {
        LOG.warn("Failed to compact column family: {}", columnFamily, e);
      }
      return null;
    });
  }

  private Void compactAsync(String columnFamilyName) throws IOException {
    LOG.info("Compacting column family: {}", columnFamilyName);
    long startTime = Time.monotonicNow();
    ManagedCompactRangeOptions options =
        new ManagedCompactRangeOptions();
    options.setBottommostLevelCompaction(
        ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
    // Find CF Handler
    RocksDatabase.ColumnFamily columnFamily =
        ((RDBStore)om.getMetadataManager().getStore()).getDb().getColumnFamily(columnFamilyName);
    ((RDBStore)om.getMetadataManager().getStore()).getDb().compactRange(
        columnFamily, null, null, options);
    LOG.info("Compaction of column family: {} completed in {} ms",
        columnFamilyName, Time.monotonicNow() - startTime);
    return null;
  }
}
