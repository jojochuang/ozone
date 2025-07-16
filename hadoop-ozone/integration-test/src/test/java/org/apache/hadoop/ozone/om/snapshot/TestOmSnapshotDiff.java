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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract class to test OmSnapshot.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOmSnapshotDiff {
  static {
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String volumeName;
  private String bucketName;
  private OzoneManagerProtocol writeClient;
  private ObjectStore store;
  private OzoneManager ozoneManager;
  private OzoneBucket ozoneBucket;
  private OzoneConfiguration conf;

  private final BucketLayout bucketLayout;
  private final boolean enabledFileSystemPaths;
  private final boolean forceFullSnapshotDiff;
  private final boolean disableNativeDiff;
  private final AtomicInteger counter;
  private final boolean createLinkedBucket;
  private final Map<String, String> linkedBuckets = new HashMap<>();

  public TestOmSnapshotDiff(BucketLayout newBucketLayout,
                            boolean newEnableFileSystemPaths,
                            boolean forceFullSnapDiff,
                            boolean disableNativeDiff,
                            boolean createLinkedBucket)
      throws Exception {
    this.enabledFileSystemPaths = newEnableFileSystemPaths;
    this.bucketLayout = newBucketLayout;
    this.forceFullSnapshotDiff = forceFullSnapDiff;
    this.disableNativeDiff = disableNativeDiff;
    this.counter = new AtomicInteger();
    this.createLinkedBucket = createLinkedBucket;
    init();

    if (!disableNativeDiff) {
      assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary());
    }
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  private void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, enabledFileSystemPaths);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    conf.setBoolean(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF, forceFullSnapshotDiff);
    conf.setBoolean(OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS, disableNativeDiff);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    //conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.BUCKET_LAYOUT_SUPPORT.layoutVersion());
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, KeyManagerImpl.DISABLE_VALUE);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil.createVolumeAndBucket(client, bucketLayout, createLinkedBucket);
    if (createLinkedBucket) {
      this.linkedBuckets.put(ozoneBucket.getName(), ozoneBucket.getSourceBucket());
    }
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();
    ozoneManager = cluster.getOzoneManager();

    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();

    // stop the deletion services so that keys can still be read
    //stopKeyManager();
    //preFinalizationChecks();
    //finalizeOMUpgrade();
  }

  private void createBucket(OzoneVolume volume, String bucketVal) throws IOException {
    if (createLinkedBucket) {
      String sourceBucketName = linkedBuckets.computeIfAbsent(bucketVal, (k) -> bucketVal + counter.incrementAndGet());
      volume.createBucket(sourceBucketName);
      TestDataUtil.createLinkedBucket(client, volume.getName(), sourceBucketName, bucketVal);
      this.linkedBuckets.put(bucketVal, sourceBucketName);
    } else {
      volume.createBucket(bucketVal);
    }
  }

  private void stopKeyManager() throws IOException {
    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    keyManager.stop();
  }

  private void startKeyManager() throws IOException {
    KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    keyManager.start(conf);
  }

  private RDBStore getRdbStore() {
    return (RDBStore) ozoneManager.getMetadataManager().getStore();
  }

  @AfterAll
  void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private int keyCount(OzoneBucket bucket, String keyPrefix)
      throws IOException {
    Iterator<? extends OzoneKey> iterator = bucket.listKeys(keyPrefix);
    int keyCount = 0;
    while (iterator.hasNext()) {
      iterator.next();
      keyCount++;
    }
    return keyCount;
  }

  private void getOmKeyInfo(String volume, String bucket,
                            String key) throws IOException {
    ResolvedBucket resolvedBucket = new ResolvedBucket(volume, bucket,
        volume, this.linkedBuckets.getOrDefault(bucket, bucket), "", bucketLayout);
    cluster.getOzoneManager().getKeyManager()
        .getKeyInfo(new OmKeyArgs.Builder()
                .setVolumeName(volume)
                .setBucketName(this.linkedBuckets.getOrDefault(bucket, bucket))
                .setKeyName(key).build(),
            resolvedBucket, null);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Snapshot snap2 is created
   * 4) Key k1 is deleted.
   * 5) Snapshot snap3 is created.
   * 6) Snapdiff b/w snap3 & snap2 taken to assert difference of 1 key
   */
  @Test
  public void testSnapDiffHandlingReclaimWithLatestUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff =
        getSnapDiffReport(testVolumeName, testBucketName, snap1, snap2);
    assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is deleted.
   * 4) Snapshot snap2 is created.
   * 5) Snapshot snap3 is created.
   * 6) Snap diff b/w snap3 & snap1 taken to assert difference of 1 key.
   * 7) Snap diff b/w snap3 & snap2 taken to assert difference of 0 key.
   */
  @Test
  public void testSnapDiffHandlingReclaimWithPreviousUse() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is deleted.
   * 4) Key k1 is recreated.
   * 5) Snapshot snap2 is created.
   * 6) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 2 keys.
   * 7) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *    of 0 key.
   * 8) Checking rocks db to ensure the object created shouldn't be reclaimed
   *    as it is used by snapshot.
   * 9) Key k1 is deleted.
   * 10) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *     of 1 key.
   * 11) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *     of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRecreation() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.deleteKey(key1);
    key1 = createFileKey(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap3);
    assertEquals(diff.getDiffList().size(), 2);
    assertEquals(diff.getDiffList(), Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap3);
    assertEquals(diff.getDiffList().size(), 0);
    bucket.deleteKey(key1);
    String snap4 = "snap4";
    createSnapshot(testVolumeName, testBucketName, snap4);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap1, snap4);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, snap4);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key renamed-k1 is deleted.
   * 5) Snapshot snap2 created.
   * 4) Snap diff b/w snap2 & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffReclaimWithKeyRename() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 10000);
    getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
    bucket.deleteKey(renamedKey);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)
    ));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key renamed-k1 is renamed to renamed-renamed-k1.
   * 5) Key renamed-renamed-k1 is deleted.
   * 6) Snapshot snap2 is created.
   * 7) Snap diff b/w Active FS & snap1 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWith2RenamesAndDelete() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 120000);
    String renamedRenamedKey = "renamed-" + renamedKey;
    bucket.renameKey(renamedKey, renamedRenamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(testVolumeName, testBucketName, renamedRenamedKey);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 120000);
    getOmKeyInfo(testVolumeName, testBucketName, renamedRenamedKey);
    bucket.deleteKey(renamedRenamedKey);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName,
            snap3);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap3);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1)
    ));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to renamed-k1.
   * 4) Key k1 is recreated.
   * 5) Key k1 is deleted.
   * 6) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 key.
   */
  @Test
  public void testSnapDiffWithKeyRenamesRecreationAndDelete()
          throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String renamedKey = "renamed-" + key1;
    bucket.renameKey(key1, renamedKey);
    key1 =  createFileKey(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap3);
    bucket.deleteKey(key1);
    String activeSnap = "activefs";
    createSnapshot(testVolumeName, testBucketName, activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, activeSnap);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME,
            key1, renamedKey)
    ));
  }

  /**
   * Testing scenario:
   * 1) Snapshot snap1 created.
   * 2) Key k1 is created.
   * 3) Key k1 is deleted.
   * 4) Snapshot s2 is created before key k1 is reclaimed.
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 0 keys.
   * 6) Snap diff b/w snapshot of Active FS & snap2 taken to assert difference
   *    of 0 keys.
   */
  @Test
  public void testSnapDiffReclaimWithDeferredKeyDeletion() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    bucket.deleteKey(key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    String activeSnap = "activefs";
    createSnapshot(testVolumeName, testBucketName, activeSnap);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, activeSnap);
    assertEquals(diff.getDiffList().size(), 0);
    diff = getSnapDiffReport(testVolumeName, testBucketName, snap2, activeSnap);
    assertEquals(diff.getDiffList().size(), 0);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is renamed to key k1_renamed
   * 4) Key k1_renamed is renamed to key k1
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 key with 1 Modified entry.
   */
  @Test
  public void testSnapDiffWithNoEffectiveRename() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    String key1Renamed = key1 + "_renamed";

    bucket.renameKey(key1, key1Renamed);
    bucket.renameKey(key1Renamed, key1);


    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    assertEquals(diff.getDiffList(), Collections.singletonList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, key1, key1)));
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Dir dir1/dir2 is created.
   * 4) Key k1 is renamed to key dir1/dir2/k1_renamed
   * 5) Snap diff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 3 key
   *    with 1 rename entry & 2 dirs create entry.
   */
  @Test
  public void testSnapDiffWithDirectory() throws Exception {

    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(bucket.getVolumeName(), bucket.getName(),
        key1);
    bucket.createDirectory("dir1/dir2");
    String key1Renamed = "dir1/dir2/" + key1 + "_renamed";
    bucket.renameKey(key1, key1Renamed);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<DiffReportEntry> diffEntries;
    if (bucketLayout.isFileSystemOptimized()) {
      diffEntries = Lists.newArrayList(
          SnapshotDiffReportOzone.getDiffReportEntry(
          SnapshotDiffReport.DiffType.RENAME, key1,
              "dir1/dir2/" + key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1/dir2"));
    } else {
      diffEntries = Lists.newArrayList(
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.RENAME,
              key1, "dir1/dir2/" + key1 + "_renamed"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1/dir2"),
          SnapshotDiffReportOzone.getDiffReportEntry(
              SnapshotDiffReport.DiffType.CREATE, "dir1"));
    }
    assertEquals(diff.getDiffList(), diffEntries);
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Dir dir1/dir2 is created.
   * 4) Key k1 is renamed to key dir1/dir2/k1_renamed
   * 5) Dir dir1 is deleted.
   * 6) Snapshot snap2 created.
   * 5) Snapdiff b/w snapshot of Active FS & snap1 taken to assert difference
   *    of 1 delete key entry.
   */
  @Test
  public void testSnapDiffWithDirectoryDelete() throws Exception {
    assumeTrue(bucketLayout.isFileSystemOptimized());
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    bucket.createDirectory("dir1/dir2");
    String key1Renamed = "dir1/dir2/" + key1 + "_renamed";
    bucket.renameKey(key1, key1Renamed);
    bucket.deleteDirectory("dir1", true);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);

    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1));
    assertEquals(diff.getDiffList(), diffEntries);
  }

  private OzoneObj buildKeyObj(OzoneBucket bucket, String key) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key).build();
  }

  @Test
  public void testSnapdiffWithObjectMetaModification() throws Exception {
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    createSnapshot(testVolumeName, testBucketName, snap1);
    OzoneObj keyObj = buildKeyObj(bucket, key1);
    OzoneAcl userAcl = OzoneAcl.of(USER, "user",
        DEFAULT, WRITE);
    store.addAcl(keyObj, userAcl);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReport diff = getSnapDiffReport(testVolumeName, testBucketName,
        snap1, snap2);
    List<DiffReportEntry> diffEntries = Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY,
            key1));
    assertEquals(diffEntries, diff.getDiffList());
  }

  @Test
  public void testSnapdiffWithFilesystemCreate()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {

    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createSnapshot(testVolumeName, testBucketName, snap1);
      createFileKey(fs, key);
      String snap2 = "snap2";
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      int idx = bucketLayout.isFileSystemOptimized() ? 0 :
          diff.getDiffList().size() - 1;
      Path p = new Path("/");
      while (true) {
        FileStatus[] fileStatuses = fs.listStatus(p);
        assertEquals(fileStatuses[0].isDirectory(),
            bucketLayout.isFileSystemOptimized() &&
                idx < diff.getDiffList().size() - 1 ||
                !bucketLayout.isFileSystemOptimized() && idx > 0);
        p = fileStatuses[0].getPath();
        assertEquals(diff.getDiffList().get(idx),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, p.toUri().getPath()
                    .substring(1)));
        if (fileStatuses[0].isFile()) {
          break;
        }
        idx += bucketLayout.isFileSystemOptimized() ? 1 : -1;
      }
    }
  }

  @Test
  public void testSnapDiffWithFilesystemDirectoryRenameOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createFileKey(fs, key);
      createSnapshot(testVolumeName, testBucketName, snap1);
      String snap2 = "snap2";
      fs.rename(new Path("/dir1/dir2"), new Path("/dir1/dir3"));
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      if (bucketLayout.isFileSystemOptimized()) {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir1/dir3"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1")),
            diff.getDiffList());
      } else {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1",
                "dir1/dir3/key1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2",
                "dir1/dir3")),
            diff.getDiffList());
      }

    }
  }

  @Test
  public void testSnapDiffWithFilesystemDirectoryMoveOperation()
      throws IOException, URISyntaxException, InterruptedException,
      TimeoutException {
    assumeTrue(!bucketLayout.isObjectStore(enabledFileSystemPaths));
    String testVolumeName = "vol" + counter.incrementAndGet();
    String testBucketName = "bucket" + counter.incrementAndGet();
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, testBucketName, testVolumeName);
    try (FileSystem fs = FileSystem.get(new URI(rootPath), cluster.getConf())) {
      String snap1 = "snap1";
      String key = "/dir1/dir2/key1";
      createFileKey(fs, key);
      fs.mkdirs(new Path("/dir3"));
      createSnapshot(testVolumeName, testBucketName, snap1);
      String snap2 = "snap2";
      fs.rename(new Path("/dir1/dir2"), new Path("/dir3/dir2"));
      createSnapshot(testVolumeName, testBucketName, snap2);
      SnapshotDiffReport diff = getSnapDiffReport(testVolumeName,
          testBucketName, snap1, snap2);
      if (bucketLayout.isFileSystemOptimized()) {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2", "dir3/dir2"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, "dir3")),
            diff.getDiffList());
      } else {
        assertEquals(Arrays.asList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1",
                "dir3/dir2/key1"),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1/dir2",
                "dir3/dir2")),
            diff.getDiffList());
      }
    }
  }

  @Test
  public void testSnapDiffWithDirRename() throws Exception {
    assumeTrue(bucketLayout.isFileSystemOptimized());
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    bucket1.createDirectory("dir1");
    String snap1 = "snap1";
    createSnapshot(volume, bucket, snap1);
    bucket1.renameKey("dir1", "dir1_rename");
    String snap2 = "snap2";
    createSnapshot(volume, bucket, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(volume, bucket,
        snap1, snap2);
    assertEquals(Collections.singletonList(
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.RENAME, "dir1", "dir1_rename")),
        diff.getDiffList());
  }

  @Test
  public void testSnapDiff() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    key1 = createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    // When from and to snapshots are same, it returns empty response.
    SnapshotDiffReportOzone
        diff0 = getSnapDiffReport(volume, bucket, snap1, snap1);
    assertTrue(diff0.getDiffList().isEmpty());

    // Do nothing, take another snapshot
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);

    SnapshotDiffReportOzone
        diff1 = getSnapDiffReport(volume, bucket, snap1, snap2);
    assertTrue(diff1.getDiffList().isEmpty());
    // Create Key2 and delete Key1, take snapshot
    String key2 = "key-2-";
    key2 = createFileKeyWithPrefix(bucket1, key2);
    bucket1.deleteKey(key1);
    String snap3 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap3);

    // Diff should have 2 entries
    SnapshotDiffReportOzone
        diff2 = getSnapDiffReport(volume, bucket, snap2, snap3);
    assertEquals(2, diff2.getDiffList().size());
    assertEquals(
        Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.DELETE, key1),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.CREATE, key2)),
        diff2.getDiffList());

    // Rename Key2
    String key2Renamed = key2 + "_renamed";
    bucket1.renameKey(key2, key2Renamed);
    String snap4 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap4);

    SnapshotDiffReportOzone
        diff3 = getSnapDiffReport(volume, bucket, snap3, snap4);
    assertEquals(1, diff3.getDiffList().size());
    assertThat(diff3.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.RENAME, key2,
             key2Renamed));


    // Create a directory
    String dir1 = "dir-1" +  counter.incrementAndGet();
    bucket1.createDirectory(dir1);
    String snap5 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap5);
    SnapshotDiffReportOzone
        diff4 = getSnapDiffReport(volume, bucket, snap4, snap5);
    assertEquals(1, diff4.getDiffList().size());
    assertThat(diff4.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.CREATE, dir1));

    String key3 = createFileKeyWithPrefix(bucket1, "key-3-");
    String snap6 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap6);
    createFileKey(bucket1, key3);
    String renamedKey3 = key3 + "_renamed";
    bucket1.renameKey(key3, renamedKey3);

    String snap7 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap7);
    SnapshotDiffReportOzone
        diff5 = getSnapDiffReport(volume, bucket, snap6, snap7);
    List<DiffReportEntry> expectedDiffList =
        Arrays.asList(SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, key3,
             renamedKey3),
            SnapshotDiffReportOzone.getDiffReportEntry(
                SnapshotDiffReport.DiffType.MODIFY, key3)
        );
    assertEquals(expectedDiffList, diff5.getDiffList());

    IOException ioException = assertThrows(IOException.class,
        () -> store.snapshotDiff(volume, bucket, snap6,
            snap7, "3", 0, forceFullSnapshotDiff, disableNativeDiff));
    assertThat(ioException.getMessage()).contains("Index (given: 3) " +
        "should be a number >= 0 and < totalDiffEntries: 2. Page size " +
        "(given: 1000) should be a positive number > 0.");

    deleteKeys(bucket1);
    String snap8 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap8);

    SnapshotDiffReportOzone
        diff6 = getSnapDiffReport(volume, bucket, snap7, snap8);
    assertEquals(3, diff6.getDiffList().size());
    assertThat(diff6.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.DELETE, key2Renamed));
    assertThat(diff6.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.DELETE, renamedKey3));
    assertThat(diff6.getDiffList()).contains(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReportOzone.DiffType.DELETE, dir1));
  }

  private SnapshotDiffReportOzone getSnapDiffReport(String volume,
                                                    String bucket,
                                                    String fromSnapshot,
                                                    String toSnapshot)
      throws InterruptedException, IOException {
    SnapshotDiffResponse response;
    do {
      response = store.snapshotDiff(volume, bucket, fromSnapshot,
          toSnapshot, null, 0, forceFullSnapshotDiff,
          disableNativeDiff);
      Thread.sleep(response.getWaitTimeInMs());
    } while (response.getJobStatus() != DONE);

    return response.getSnapshotDiffReport();
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is recreated.
   * 4) Snapshot s2 is created.
   * 5) Snapdiff b/w Snap1 & Snap2 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWithKeyOverwrite() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    createFileKey(bucket, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    getOmKeyInfo(testVolumeName, testBucketName, key1);
    assertEquals(diff.getDiffList().size(), 1);
    assertEquals(diff.getDiffList(), Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));
  }

  @Test
  public void testSnapDiffMultipleBuckets() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucketName1 = "buck-" + counter.incrementAndGet();
    String bucketName2 = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucketName1);
    createBucket(volume1, bucketName2);
    OzoneBucket bucket1 = volume1.getBucket(bucketName1);
    OzoneBucket bucket2 = volume1.getBucket(bucketName2);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    key1 = createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap1);
    // Create key in bucket2 and bucket1 and calculate diff
    // Diff should not contain bucket2's key
    createFileKeyWithPrefix(bucket1, key1);
    createFileKeyWithPrefix(bucket2, key1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucketName1, snap2);
    SnapshotDiffReportOzone diff1 =
        getSnapDiffReport(volume, bucketName1, snap1, snap2);
    assertEquals(1, diff1.getDiffList().size());
  }

  /**
   * Testing scenario:
   * 1) Key k1 is created.
   * 2) Snapshot snap1 created.
   * 3) Key k1 is updated with object tagging.
   * 4) Snapshot s2 is created.
   * 5) Snapdiff b/w Snap1 & Snap2 taken to assert difference of 1 key.
   * 6) Delete k1's tag.
   * 7) Snapshot s3 is created.
   * 8) Snapdiff b/w Snap2 & Snap3 taken to assert difference of 1 key.
   */
  @Test
  public void testSnapDiffWithTag() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key-1", "tag-value-1");
    tags.put("tag-key-2", "tag-value-2");

    bucket.putObjectTagging(key1, tags);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(diff.getDiffList(), Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));

    bucket.deleteObjectTagging(key1);
    String snap3 = "snap3";
    createSnapshot(testVolumeName, testBucketName, snap3);
    SnapshotDiffReportOzone diff2 = getSnapDiffReport(testVolumeName,
        testBucketName, snap2, snap3);
    assertEquals(1, diff2.getDiffList().size());
    assertEquals(diff2.getDiffList(), Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)));
  }

  @Test
  public void testSnapDiffWithTime() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    getOmKeyInfo(testVolumeName, testBucketName, key1);

    bucket.setTimes(key1, 1, 1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, key1, key1)), diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithStreamKey() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);

    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String key1 = "k1";
    key1 = createStreamFileKeyWithPrefix(bucket, key1);
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.CREATE, key1)), diff.getDiffList());
  }

  @Test
  public void testSnapDiffWithRewrite() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);
    OzoneKeyDetails keyDetails = bucket.getKey(key1);

    OzoneOutputStream out = null;
    try {
      out = bucket.rewriteKey(keyDetails.getName(), keyDetails.getDataSize(),
          keyDetails.getGeneration(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
          keyDetails.getMetadata());
      out.write("rewrite".getBytes(UTF_8));
    } finally {
      if (out != null) {
        out.close();
      }
    }
    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)), diff.getDiffList());
  }

  // hsync is problematic for snapshot. Ignore it for now.
  /*@Test
  public void testSnapDiffWithHsync() throws Exception {
    String testVolumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String testBucketName = "bucket1";
    store.createVolume(testVolumeName);
    OzoneVolume volume = store.getVolume(testVolumeName);
    createBucket(volume, testBucketName);
    OzoneBucket bucket = volume.getBucket(testBucketName);
    String snap1 = "snap1";
    createSnapshot(testVolumeName, testBucketName, snap1);

    String key1 = "k1";
    key1 = createFileKeyWithPrefix(bucket, key1);

    String snap2 = "snap2";
    createSnapshot(testVolumeName, testBucketName, snap2);
    SnapshotDiffReportOzone diff = getSnapDiffReport(testVolumeName,
        testBucketName, snap1, snap2);
    assertEquals(1, diff.getDiffList().size());
    assertEquals(Lists.newArrayList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.MODIFY, key1)), diff.getDiffList());
  }*/

  private String createSnapshot(String volName, String buckName)
      throws IOException, InterruptedException, TimeoutException {
    return createSnapshot(volName, buckName, UUID.randomUUID().toString());
  }

  private String createSnapshot(String volName, String buckName,
      String snapshotName)
      throws IOException, InterruptedException, TimeoutException {
    store.createSnapshot(volName, buckName, snapshotName);
    String snapshotKeyPrefix =
        OmSnapshotManager.getSnapshotPrefix(snapshotName);
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volName, linkedBuckets.getOrDefault(buckName, buckName), snapshotName));
    String snapshotDirName =
        OmSnapshotManager.getSnapshotPath(ozoneManager.getConfiguration(),
            snapshotInfo) + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils
        .waitFor(() -> new File(snapshotDirName).exists(), 1000, 120000);
    return snapshotKeyPrefix;
  }

  private void deleteKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> bucketIterator = bucket.listKeys(null);
    // turn bucketIterator into a list
    List<String> keys = new ArrayList<>();
    while (bucketIterator.hasNext()) {
      keys.add(bucketIterator.next().getName());
    }
    bucket.deleteKeys(keys);
    /*while (bucketIterator.hasNext()) {
      OzoneKey key = bucketIterator.next();
      bucket.deleteKey(key.getName());
    }*/
  }

  private String createFileKeyWithPrefix(OzoneBucket bucket, String keyPrefix)
      throws Exception {
    String key = keyPrefix + counter.incrementAndGet();
    return createFileKey(bucket, key);
  }

  private String createFileKey(OzoneBucket bucket, String key)
          throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneOutputStream fileKey = bucket.createKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(bucket.getVolumeName(), bucket.getName(), key);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 300, 10000);
    return key;
  }

  private String createStreamFileKeyWithPrefix(OzoneBucket bucket, String keyPrefix)
      throws Exception {
    String key = keyPrefix + counter.incrementAndGet();
    return createStreamFileKey(bucket, key);
  }

  private String createStreamFileKey(OzoneBucket bucket, String key)
      throws Exception {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    OzoneDataStreamOutput fileKey = bucket.createStreamKey(key, value.length);
    fileKey.write(value);
    fileKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        getOmKeyInfo(bucket.getVolumeName(), bucket.getName(), key);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 300, 10000);
    return key;
  }

  private void createFileKey(FileSystem fs,
                             String path)
      throws IOException, InterruptedException, TimeoutException {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    Path pathVal = new Path(path);
    FSDataOutputStream fileKey = fs.create(pathVal);
    fileKey.write(value);
    fileKey.close();
    GenericTestUtils.waitFor(() -> {
      try {
        fs.getFileStatus(pathVal);
      } catch (IOException e) {
        return false;
      }
      return true;
    }, 1000, 30000);
  }

}
