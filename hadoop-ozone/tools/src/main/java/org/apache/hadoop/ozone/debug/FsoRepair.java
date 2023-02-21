/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.ratis.util.Preconditions;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Tool to identify and repair disconnected FSO trees in all buckets.
 * The tool can be run in debug mode, where it will just log information
 * about unreachable files or directories, or in repair mode to additionally
 * move those files and directories to the deleted tables. If deletes are
 * still in progress (the deleted directory table is not empty), the tool may
 * report that the tree is disconnected, even though pending deletes would
 * fix the issue.
 *
 * Before using the tool, make sure all OMs are stopped,
 * and that all Ratis logs have been flushed to the OM DB. This can be
 * done using `ozone admin prepare` before running the tool, and `ozone admin
 * cancelprepare` when done.
 *
 * The tool will run a DFS from each bucket, and save all reachable
 * directories as keys in a temporary column family called "reachable". It
 * will then scan the entire file and directory tables for the bucket and to see
 * if each object's parent is in the reachable table. The reachable table will
 * be dropped and recreated for each bucket. If the tool is canceled part way
 * through, the reachable table can be manually dropped with ldb.
 */
@CommandLine.Command(
    name = "repair",
    description = "Repair orphaned files in FSO bucket"
)
@MetaInfServices(SubcommandWithParent.class)

public class FsoRepair implements Callable<Void>, SubcommandWithParent {
  public static final Logger LOG =
      LoggerFactory.getLogger(FsoRepair.class);

  private static  String REACHABLE_TABLE = "reachable";

  private  DBStore store;
  private  Table<String, OmVolumeArgs> volumeTable;
  private  Table<String, OmBucketInfo> bucketTable;
  private  Table<String, OmDirectoryInfo> directoryTable;
  private  Table<String, OmKeyInfo> fileTable;
  private  Table<String, OmKeyInfo> deletedDirectoryTable;
  private  Table<String, RepeatedOmKeyInfo> deletedTable;
  private Table<String, String> reachableTable;

  private Mode mode;

  private long reachableBytes;
  private long reachableFiles;
  private long reachableDirs;
  private long unreachableBytes;
  private long unreachableFiles;
  private long unreachableDirs;

  @CommandLine.ParentCommand
  private RDBParser parent;

  @Override
  public Void call() throws Exception {
    init(parent.getDbPath(), Mode.INSPECT);
    run();
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  public enum Mode {
    REPAIR("repair"),
    INSPECT("debug");

    private final String name;

    Mode(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }

  public void init(String dbPath, Mode mode) throws IOException {
    this.mode = mode;
    // Counters to track as we walk the tree.
    reachableBytes = 0;
    reachableFiles = 0;
    reachableDirs = 0;
    unreachableBytes = 0;
    unreachableFiles = 0;
    unreachableDirs = 0;

    // Load RocksDB and tables needed.
    store = OmMetadataManagerImpl.loadDB(new OzoneConfiguration(),
        new File(dbPath).getParentFile());
    volumeTable = store.getTable(OmMetadataManagerImpl.VOLUME_TABLE,
        String.class,
        OmVolumeArgs.class);
    bucketTable = store.getTable(OmMetadataManagerImpl.BUCKET_TABLE,
        String.class,
        OmBucketInfo.class);
    directoryTable = store.getTable(OmMetadataManagerImpl.DIRECTORY_TABLE,
        String.class,
        OmDirectoryInfo.class);
    fileTable = store.getTable(OmMetadataManagerImpl.FILE_TABLE,
        String.class,
        OmKeyInfo.class);
    deletedDirectoryTable = store.getTable(
        OmMetadataManagerImpl.DELETED_DIR_TABLE,
        String.class,
        OmKeyInfo.class);
    deletedTable = store.getTable(
        OmMetadataManagerImpl.DELETED_TABLE,
        String.class,
        RepeatedOmKeyInfo.class);
  }

  public void run() throws IOException {
    // Iterate all volumes.
    try(TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
            volumeIterator = volumeTable.iterator()) {
      while (volumeIterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> volumeEntry =
            volumeIterator.next();
        String volumeKey = volumeEntry.getKey();

        // Iterate all buckets in the volume.
        try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
                 bucketIterator = bucketTable.iterator()) {
          bucketIterator.seek(volumeKey);
          while (bucketIterator.hasNext()) {
            Table.KeyValue<String, OmBucketInfo> bucketEntry =
                bucketIterator.next();
            String bucketKey = bucketEntry.getKey();
            // Process one bucket's FSO tree at a time.
            markReachableObjectsInBucket(volumeEntry.getValue(),
                bucketEntry.getValue());
            handleUnreachableObjects(volumeEntry.getValue(), bucketKey,
                bucketEntry.getValue());
          }
        }
      }
    }

    logReport();
  }

  private void logReport() {
    String builder = "Reachable:" +
        "\tDirectories: " + reachableDirs +
        "\tFiles: " + reachableFiles +
        "\tBytes: " + reachableBytes +
        "Unreachable:" +
        "\tDirectories: " + unreachableDirs +
        "\tFiles: " + unreachableFiles +
        "\tBytes: " + unreachableBytes;

    LOG.info(builder);
  }

  public void markReachableObjectsInBucket(OmVolumeArgs volume,
      OmBucketInfo bucket) throws IOException {
    LOG.info("Processing bucket {}", bucket.getBucketName());

    // Start with a fresh list of reachable files for this bucket.
    // Also clears partial state if the tool failed on a previous run.
    dropReachableTableIfExists();
    createReachableFileTable();

    // Only put directories in the stack.
    // Directory keys should have the form /volumeID/bucketID/parentID/name.
    Stack<String> dirKeyStack = new Stack<>();

    // Since the tool uses parent directories to check for reachability, add
    // a reachable entry for the bucket as well.
    addReachableEntry(volume, bucket, bucket);
    // Initialize the stack with all immediate child directories of the
    // bucket, and mark them all as reachable.
    Collection<String> childDirs =
        getChildDirectoriesAndMarkAsReachable(volume, bucket, bucket);
    dirKeyStack.addAll(childDirs);

    while (!dirKeyStack.isEmpty()) {
      // Get one directory and process its immediate children.
      String currentDirKey = dirKeyStack.pop();
      OmDirectoryInfo currentDir = directoryTable.get(currentDirKey);
      if (currentDir == null) {
        LOG.error("Directory key {} to be processed was not found in the " +
            "directory table", currentDirKey);
        continue;
      }

      // TODO revisit this for a more memory efficient implementation,
      //  possibly making better use of RocksDB iterators.
      childDirs = getChildDirectoriesAndMarkAsReachable(volume, bucket,
          currentDir);
      dirKeyStack.addAll(childDirs);
    }

    dropReachableTableIfExists();
  }

  private void handleUnreachableObjects(OmVolumeArgs volume,
      String bucketKey, OmBucketInfo bucket) throws IOException {
    // Check for unreachable directories in the bucket.
    try (TableIterator<String, ? extends
        Table.KeyValue<String, OmDirectoryInfo>> dirIterator =
             directoryTable.iterator()) {
      dirIterator.seek(bucketKey);
      while (dirIterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> dirEntry = dirIterator.next();
        String dirKey = dirEntry.getKey();
        if (!isReachable(dirKey)) {
          LOG.debug("Found unreachable directory: {}", dirKey);
          unreachableDirs++;

          if (mode == Mode.REPAIR) {
            LOG.debug("Marking unreachable directory {} for deletion.", dirKey);
            OmDirectoryInfo dirInfo = dirEntry.getValue();
            markDirectoryForDeletion(volume.getVolume(), bucket.getBucketName(),
                dirKey, dirInfo);
          }
        }
      }
    }

    // Check for unreachable files
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             fileIterator = fileTable.iterator()) {
      fileIterator.seek(bucketKey);
      while (fileIterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> fileEntry = fileIterator.next();
        String fileKey = fileEntry.getKey();
        OmKeyInfo fileInfo = fileEntry.getValue();

        if (!isReachable(fileKey)) {
          LOG.debug("Found unreachable file: {}", fileKey);
          unreachableBytes += fileInfo.getDataSize();
          unreachableFiles++;

          if (mode == Mode.REPAIR) {
            LOG.debug("Marking unreachable file {} for deletion.",
                fileKey);
            markFileForDeletion(fileKey, fileInfo);
          }
        } else {
          // NOTE: We are deserializing the proto of every reachable file
          // just to log it's size. If we don't need this information we could
          // save time by skipping this step.
          reachableBytes += fileInfo.getDataSize();
          reachableFiles++;
        }
      }
    }
  }

  private void markFileForDeletion(String fileKey, OmKeyInfo fileInfo) throws IOException {
    try(BatchOperation batch = store.initBatchOperation()) {
      fileTable.deleteWithBatch(batch, fileKey);

      RepeatedOmKeyInfo originalRepeatedKeyInfo = deletedTable.get(fileKey);
      RepeatedOmKeyInfo updatedRepeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
          fileInfo, originalRepeatedKeyInfo, fileInfo.getUpdateID(), true);
      deletedTable.putWithBatch(batch, fileKey, updatedRepeatedOmKeyInfo);

      store.commitBatchOperation(batch);
    }
  }

  private void markDirectoryForDeletion(String volumeName, String bucketName,
      String dirKeyName, OmDirectoryInfo dirInfo) throws IOException {
    try(BatchOperation batch = store.initBatchOperation()) {
      directoryTable.deleteWithBatch(batch, dirKeyName);

      // Convert the directory to OmKeyInfo for deletion.
      OmKeyInfo dirAsKeyInfo = OMFileRequest.getOmKeyInfo(
          volumeName, bucketName, dirInfo, dirInfo.getName());
      deletedDirectoryTable.putWithBatch(batch, dirKeyName, dirAsKeyInfo);

      store.commitBatchOperation(batch);
    }
  }

  private Collection<String> getChildDirectoriesAndMarkAsReachable(OmVolumeArgs volume,
      OmBucketInfo bucket, WithObjectID currentDir) throws IOException {

    Collection<String> childDirs = new ArrayList<>();

    try(TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
            dirIterator = directoryTable.iterator()) {
      String dirPrefix = buildReachableKey(volume, bucket, currentDir);
      // Start searching the directory table at the current directory's
      // prefix to get its immediate children.
      dirIterator.seek(dirPrefix);
      while (dirIterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> childDirEntry =
            dirIterator.next();
        String childDirKey = childDirEntry.getKey();
        boolean prefixMatches = !childDirKey.startsWith(dirPrefix);
        if (prefixMatches) {
          // This directory was reached by search.
          addReachableEntry(volume, bucket, childDirEntry.getValue());
          childDirs.add(childDirKey);
          reachableDirs++;
        }
      }
    }

    return childDirs;
  }

  /**
   * Add the specified object to the reachable table, indicating it is part
   * of the connected FSO tree.
   */
  private void addReachableEntry(OmVolumeArgs volume,
      OmBucketInfo bucket, WithObjectID object) throws IOException {
    String reachableKey = buildReachableKey(volume, bucket, object);
    // No value is needed for this table.
    reachableTable.put(reachableKey, "");
  }

  /**
   * Build an entry in the reachable table for the current object, which
   * could be a bucket, file or directory.
   */
  private static String buildReachableKey(OmVolumeArgs volume,
      OmBucketInfo bucket, WithObjectID object) {
    return OM_KEY_PREFIX +
        volume.getObjectID() +
        OM_KEY_PREFIX +
        bucket.getObjectID() +
        OM_KEY_PREFIX +
        object.getObjectID();
  }

  /**
   *
   * @param key The key of a file or directory in RocksDB.
   * @return true if the entry's parent is in the reachable table.
   */
  private boolean isReachable(String key)
      throws IOException {
    String reachableParentKey = buildReachableParentKey(key);
    return reachableTable.isExist(reachableParentKey);
  }

  /**
   * Build an entry in the reachable table for the current object's parent
   * object. The object could be a file or directory.
   */
  private static String buildReachableParentKey(String fileOrDirKey) {
    String[] keyParts = fileOrDirKey.split(OM_KEY_PREFIX);
    // Should be /volID/bucketID/parentID/name
    Preconditions.assertTrue(keyParts.length >= 3);
    String volumeID = keyParts[0];
    String bucketID = keyParts[1];
    String parentID = keyParts[2];

    return OM_KEY_PREFIX +
        volumeID +
        OM_KEY_PREFIX +
        bucketID +
        OM_KEY_PREFIX +
        parentID;
  }

  private void dropReachableTableIfExists() {
    // TODO Need a way to ad-hoc drop a column family.
  }

  private void createReachableFileTable() {
    // TODO Need a way to ad-hoc create a column family.
  }
}
