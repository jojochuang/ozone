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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for OMSnapshotDirectoryMetrics.
 */
public class TestOMSnapshotDirectoryMetrics {

  @TempDir
  private File tempDir;

  private OMMetadataManager metadataManager;
  private RDBStore rdbStore;
  private RocksDBCheckpointDiffer differ;
  private OzoneConfiguration conf;
  private File snapshotsParentDir;
  private File sstBackupDir;
  private OMSnapshotDirectoryMetrics metrics;

  @BeforeEach
  public void setUp() throws IOException {
    metadataManager = mock(OMMetadataManager.class);
    rdbStore = mock(RDBStore.class);
    differ = mock(RocksDBCheckpointDiffer.class);

    when(metadataManager.getStore()).thenReturn(rdbStore);
    when(rdbStore.getRocksDBCheckpointDiffer()).thenReturn(differ);

    // Create temp directories
    snapshotsParentDir = new File(tempDir, "db.snapshots");
    assertTrue(snapshotsParentDir.mkdirs());

    sstBackupDir = new File(tempDir, "sst-backup");
    assertTrue(sstBackupDir.mkdirs());

    when(rdbStore.getSnapshotsParentDir()).thenReturn(snapshotsParentDir.getAbsolutePath());
    when(differ.getSSTBackupDir()).thenReturn(sstBackupDir.getAbsolutePath() + "/");

    conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SNAPSHOT_DIRECTORY_METRICS_UPDATE_INTERVAL, "5m");

    // Create metrics directly without registering with DefaultMetricsSystem
    // to avoid conflicts between tests
    metrics = new OMSnapshotDirectoryMetrics(conf, metadataManager);
  }

  @AfterEach
  public void tearDown() {
    if (metrics != null) {
      metrics.stop();
    }
  }

  @Test
  public void testEmptyDirectories() {
    assertTrue(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getDbSnapshotsDirSize());
    assertEquals(0, metrics.getTotalSstFilesCount());
    assertEquals(0, metrics.getNumSnapshots());
    assertEquals(0, metrics.getSstBackupDirSize());
    assertEquals(0, metrics.getSstBackupFileCount());
  }

  @Test
  public void testSnapshotCheckpointDirectoryMetrics() throws IOException {
    // Create a snapshot checkpoint directory with some SST files
    File checkpointDir1 = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir1.mkdirs());

    byte[] content1 = "sst file content 1".getBytes();
    byte[] content2 = "sst file content 2".getBytes();
    byte[] content3 = "manifest content".getBytes();

    createFile(checkpointDir1, "000001.sst", content1);
    createFile(checkpointDir1, "000002.sst", content2);
    createFile(checkpointDir1, "MANIFEST-000001", content3);

    // Create another snapshot checkpoint directory
    File checkpointDir2 = new File(snapshotsParentDir, "snapshot-2");
    assertTrue(checkpointDir2.mkdirs());

    byte[] content4 = "sst file content 4".getBytes();
    createFile(checkpointDir2, "000003.sst", content4);

    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(2, metrics.getNumSnapshots());
    assertEquals(3, metrics.getTotalSstFilesCount());
    long expectedSize = content1.length + content2.length + content3.length + content4.length;
    assertEquals(expectedSize, metrics.getDbSnapshotsDirSize());
  }

  @Test
  public void testSnapshotCheckpointHardLinkDeduplication() throws IOException {
    // Create a snapshot checkpoint directory with an SST file
    File checkpointDir1 = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir1.mkdirs());

    byte[] content = "sst file content".getBytes();
    File file1 = createFile(checkpointDir1, "000001.sst", content);

    // Create another snapshot that hard-links to the same SST file
    File checkpointDir2 = new File(snapshotsParentDir, "snapshot-2");
    assertTrue(checkpointDir2.mkdirs());

    File link = new File(checkpointDir2, "000001.sst");
    Files.createLink(link.toPath(), file1.toPath());

    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(2, metrics.getNumSnapshots());
    // Both snapshots count the SST file, but size should only count unique inodes once
    assertEquals(2, metrics.getTotalSstFilesCount());
    // Hard-linked files share the same inode, so should only count size once
    assertEquals(content.length, metrics.getDbSnapshotsDirSize());
  }

  @Test
  public void testBackupSSTDirectoryMetrics() throws IOException {
    // Create SST files in backup directory
    byte[] content1 = "backup sst content 1".getBytes();
    byte[] content2 = "backup sst content 2".getBytes();
    byte[] content3 = "other file content".getBytes();

    createFile(sstBackupDir, "000010.sst", content1);
    createFile(sstBackupDir, "000011.sst", content2);
    createFile(sstBackupDir, "pruned.sst.tmp", content3);

    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(2, metrics.getSstBackupFileCount());
    long expectedSize = content1.length + content2.length + content3.length;
    assertEquals(expectedSize, metrics.getSstBackupDirSize());
  }

  @Test
  public void testBackupSSTHardLinkDeduplication() throws IOException {
    // Create a snapshot checkpoint directory with an SST file
    File checkpointDir = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir.mkdirs());

    byte[] content = "sst file content".getBytes();
    File snapshotFile = createFile(checkpointDir, "000001.sst", content);

    // Create a hard link in backup SST dir (simulating what RocksDBCheckpointDiffer does)
    File backupLink = new File(sstBackupDir, "000001.sst");
    Files.createLink(backupLink.toPath(), snapshotFile.toPath());

    assertTrue(metrics.triggerUpdateMetrics());

    // Snapshot directory counts 1 SST, size = content.length
    assertEquals(1, metrics.getNumSnapshots());
    assertEquals(1, metrics.getTotalSstFilesCount());

    // Backup SST dir also has 1 SST file
    assertEquals(1, metrics.getSstBackupFileCount());
    // The backup file is a hard link to the snapshot file (same inode)
    // but backup dir metrics count files independently
    assertEquals(content.length, metrics.getSstBackupDirSize());
  }

  @Test
  public void testResetWhenSnapshotsDirNotExists() {
    when(rdbStore.getSnapshotsParentDir()).thenReturn("/nonexistent/path");

    assertTrue(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getDbSnapshotsDirSize());
    assertEquals(0, metrics.getTotalSstFilesCount());
    assertEquals(0, metrics.getNumSnapshots());
  }

  @Test
  public void testResetWhenBackupDirNotExists() {
    when(differ.getSSTBackupDir()).thenReturn("/nonexistent/backup/path/");

    assertTrue(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getSstBackupDirSize());
    assertEquals(0, metrics.getSstBackupFileCount());
  }

  @Test
  public void testResetWhenSnapshotsDirIsNull() {
    when(rdbStore.getSnapshotsParentDir()).thenReturn(null);

    assertTrue(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getDbSnapshotsDirSize());
    assertEquals(0, metrics.getTotalSstFilesCount());
    assertEquals(0, metrics.getNumSnapshots());
  }

  @Test
  public void testReturnsFalseWhenStoreIsNotRDBStore() {
    // Use a non-RDBStore store
    org.apache.hadoop.hdds.utils.db.DBStore nonRdbStore =
        mock(org.apache.hadoop.hdds.utils.db.DBStore.class);
    when(metadataManager.getStore()).thenReturn(nonRdbStore);

    assertFalse(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getDbSnapshotsDirSize());
    assertEquals(0, metrics.getTotalSstFilesCount());
    assertEquals(0, metrics.getNumSnapshots());
    assertEquals(0, metrics.getSstBackupDirSize());
    assertEquals(0, metrics.getSstBackupFileCount());
  }

  @Test
  public void testMetricsUpdatedAfterDirectoryChanges() throws IOException {
    // Initially empty directories
    assertTrue(metrics.triggerUpdateMetrics());
    assertEquals(0, metrics.getTotalSstFilesCount());
    assertEquals(0, metrics.getSstBackupFileCount());

    // Add files to snapshot dir
    File checkpointDir = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir.mkdirs());
    byte[] content = "content".getBytes();
    createFile(checkpointDir, "000001.sst", content);

    // Add files to backup dir
    createFile(sstBackupDir, "000002.sst", content);

    // Trigger update
    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(1, metrics.getTotalSstFilesCount());
    assertEquals(1, metrics.getSstBackupFileCount());
    assertEquals(1, metrics.getNumSnapshots());
  }

  /**
   * Creates a file with the given content in the specified directory.
   *
   * @return the created file
   */
  private File createFile(File dir, String name, byte[] content) throws IOException {
    File file = new File(dir, name);
    Files.write(file.toPath(), content);
    return file;
  }

  @Test
  public void testOnlyRegularFilesAreCountedInBackupDir() throws IOException {
    // Create a subdirectory in backup dir (should be ignored)
    File subdir = new File(sstBackupDir, "subdir");
    assertTrue(subdir.mkdirs());
    byte[] content = "content".getBytes();
    createFile(subdir, "000001.sst", content);

    // Create regular SST file in backup dir
    createFile(sstBackupDir, "000002.sst", content);

    assertTrue(metrics.triggerUpdateMetrics());

    // Only the regular file in the backup dir root should be counted
    assertEquals(1, metrics.getSstBackupFileCount());
    assertEquals(content.length, metrics.getSstBackupDirSize());
  }

  @Test
  public void testOnlyRegularFilesAreCountedInSnapshotDirs() throws IOException {
    File checkpointDir = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir.mkdirs());

    // Create a subdirectory inside the checkpoint dir (should be ignored for file counting)
    File subdir = new File(checkpointDir, "subdir");
    assertTrue(subdir.mkdirs());

    byte[] content = "content".getBytes();
    createFile(subdir, "000001.sst", content);

    // Create regular file in checkpoint dir
    createFile(checkpointDir, "000002.sst", content);

    assertTrue(metrics.triggerUpdateMetrics());

    // Only regular files in the checkpoint dir directly should be counted
    assertEquals(1, metrics.getTotalSstFilesCount());
  }

  @Test
  public void testNonSSTFilesNotCountedAsSSTFiles() throws IOException {
    File checkpointDir = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir.mkdirs());

    byte[] content = "content".getBytes();
    createFile(checkpointDir, "000001.sst", content);
    createFile(checkpointDir, "MANIFEST-000001", content);
    createFile(checkpointDir, "OPTIONS-000001", content);

    // Add non-SST file to backup dir
    createFile(sstBackupDir, "000002.sst", content);
    createFile(sstBackupDir, "pruned.sst.tmp", content);

    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(1, metrics.getTotalSstFilesCount());
    assertEquals(1, metrics.getSstBackupFileCount());
  }

  @Test
  public void testMultipleSnapshotDirectories() throws IOException {
    byte[] content = "content".getBytes();
    int numSnapshots = 5;
    long expectedTotalSize = 0;

    for (int i = 0; i < numSnapshots; i++) {
      File checkpointDir = new File(snapshotsParentDir, "snapshot-" + i);
      assertTrue(checkpointDir.mkdirs());
      createFile(checkpointDir, String.format("%06d.sst", i), content);
      expectedTotalSize += content.length;
    }

    assertTrue(metrics.triggerUpdateMetrics());

    assertEquals(numSnapshots, metrics.getNumSnapshots());
    assertEquals(numSnapshots, metrics.getTotalSstFilesCount());
    assertEquals(expectedTotalSize, metrics.getDbSnapshotsDirSize());
  }

  @Test
  public void testGetMetricsReturnsLatestValues() throws IOException {
    // Add files
    File checkpointDir = new File(snapshotsParentDir, "snapshot-1");
    assertTrue(checkpointDir.mkdirs());
    byte[] content = "content".getBytes();
    createFile(checkpointDir, "000001.sst", content);

    createFile(sstBackupDir, "000002.sst", content);

    assertTrue(metrics.triggerUpdateMetrics());

    // Verify getMetrics returns correct values
    org.apache.hadoop.metrics2.MetricsCollector collector =
        mock(org.apache.hadoop.metrics2.MetricsCollector.class);
    org.apache.hadoop.metrics2.MetricsRecordBuilder recordBuilder =
        mock(org.apache.hadoop.metrics2.MetricsRecordBuilder.class);
    when(collector.addRecord(OMSnapshotDirectoryMetrics.class.getSimpleName()))
        .thenReturn(recordBuilder);
    when(recordBuilder.setContext(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(recordBuilder);
    when(recordBuilder.addGauge(org.mockito.ArgumentMatchers.any(
        org.apache.hadoop.metrics2.MetricsInfo.class),
        org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(recordBuilder);

    metrics.getMetrics(collector, true);

    org.mockito.Mockito.verify(collector).addRecord(
        OMSnapshotDirectoryMetrics.class.getSimpleName());
  }
}
