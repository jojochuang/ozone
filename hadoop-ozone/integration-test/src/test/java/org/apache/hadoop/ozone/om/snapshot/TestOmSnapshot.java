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
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.leftPad;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;


/**
 * Abstract class to test OmSnapshot.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOmSnapshot {
  static {
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  private static final String SNAPSHOT_DAY_PREFIX = "snap-day-";
  private static final String SNAPSHOT_WEEK_PREFIX = "snap-week-";
  private static final String SNAPSHOT_MONTH_PREFIX = "snap-month-";
  private static final String KEY_PREFIX = "key-";
  private static final String SNAPSHOT_KEY_PATTERN_STRING = "(.+)/(.+)/(.+)";
  private static final Pattern SNAPSHOT_KEY_PATTERN =
      Pattern.compile(SNAPSHOT_KEY_PATTERN_STRING);
  private static final int POLL_INTERVAL_MILLIS = 500;
  private static final int POLL_MAX_WAIT_MILLIS = 120_000;

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

  public TestOmSnapshot(BucketLayout newBucketLayout,
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
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.BUCKET_LAYOUT_SUPPORT.layoutVersion());
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
    stopKeyManager();
    preFinalizationChecks();
    finalizeOMUpgrade();
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

  private void preFinalizationChecks() {
    OMException omException  = assertThrows(OMException.class,
        () -> store.createSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.listSnapshot(volumeName, bucketName, null, null));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.snapshotDiff(volumeName, bucketName,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
          "", 1000, false, disableNativeDiff));
    assertFinalizationException(omException);
    omException  = assertThrows(OMException.class,
        () -> store.deleteSnapshot(volumeName, bucketName,
            UUID.randomUUID().toString()));
    assertFinalizationException(omException);

  }

  private static void assertFinalizationException(OMException omException) {
    assertEquals(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omException.getResult());
    assertThat(omException.getMessage())
        .contains("cannot be invoked before finalization.");
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private void finalizeOMUpgrade() throws Exception {
    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalization.StatusAndMessages finalizationResponse =
        omClient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    assertTrue(isStarting(finalizationResponse.status()));
    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS, () -> {
      final UpgradeFinalization.StatusAndMessages progress =
          omClient.queryUpgradeFinalizationProgress(
              upgradeClientID, false, false);
      return isDone(progress.status());
    });
  }

  @AfterAll
  void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKey
  public void testListKey() throws Exception {
    String volumeA = "vol-a-" + counter.incrementAndGet();
    String volumeB = "vol-b-" + counter.incrementAndGet();
    String bucketA = "buc-a-" + counter.incrementAndGet();
    String bucketB = "buc-b-" + counter.incrementAndGet();
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    createBucket(volA, bucketA);
    createBucket(volA, bucketB);
    createBucket(volB, bucketA);
    createBucket(volB, bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      createFileKeyWithPrefix(volAbucketA, keyBaseA + i + "-");
      createFileKeyWithPrefix(volAbucketB, keyBaseA + i + "-");
      createFileKeyWithPrefix(volBbucketA, keyBaseA + i + "-");
      createFileKeyWithPrefix(volBbucketB, keyBaseA + i + "-");
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      createFileKeyWithPrefix(volAbucketA, keyBaseB + i + "-");
      createFileKeyWithPrefix(volAbucketB, keyBaseB + i + "-");
      createFileKeyWithPrefix(volBbucketA, keyBaseB + i + "-");
      createFileKeyWithPrefix(volBbucketB, keyBaseB + i + "-");
    }

    String snapshotKeyPrefix = createSnapshot(volumeA, bucketA);

    int volABucketAKeyCount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volABucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketB);
    deleteKeys(volAbucketB);

    int volABucketBKeyCount = keyCount(volAbucketB,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volABucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketA);
    deleteKeys(volBbucketA);

    int volBBucketAKeyCount = keyCount(volBbucketA,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volBBucketAKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeB, bucketB);
    deleteKeys(volBbucketB);

    int volBBucketBKeyCount = keyCount(volBbucketB,
        snapshotKeyPrefix + "key-");
    assertEquals(20, volBBucketBKeyCount);

    snapshotKeyPrefix = createSnapshot(volumeA, bucketA);
    deleteKeys(volAbucketA);

    int volABucketAKeyACount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-a-");
    assertEquals(10, volABucketAKeyACount);


    int volABucketAKeyBCount = keyCount(volAbucketA,
        snapshotKeyPrefix + "key-b-");
    assertEquals(10, volABucketAKeyBCount);
  }

  @Test
  // based on TestOzoneRpcClientAbstract:testListKeyOnEmptyBucket
  public void testListKeyOnEmptyBucket()
      throws IOException, InterruptedException, TimeoutException {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    createBucket(vol, bucket);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys(snapshotKeyPrefix);
    while (keys.hasNext()) {
      fail();
    }
  }

  private OmKeyArgs genKeyArgs(String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setLocationInfoList(new ArrayList<>())
        .build();
  }

  @Test
  public void checkKey() throws Exception {
    String s = "testData";
    String dir1 = "dir1";
    String key1 = dir1 + "/key1";

    // create key1
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key1,
        s.length());
    byte[] input = s.getBytes(StandardCharsets.UTF_8);
    ozoneOutputStream.write(input);
    ozoneOutputStream.close();

    String snapshotKeyPrefix = createSnapshot(volumeName, bucketName);

    GenericTestUtils.waitFor(() -> {
      try {
        int keyCount = keyCount(ozoneBucket, key1);
        if (keyCount == 0) {
          return true;
        }
        ozoneBucket.deleteKey(key1);
        return false;
      } catch (Exception e) {
        return  false;
      }
    }, 1000, 10000);

    try {
      ozoneBucket.deleteKey(dir1);
    } catch (OMException e) {
      // OBJECT_STORE won't have directory entry so ignore KEY_NOT_FOUND
      if (e.getResult() != KEY_NOT_FOUND) {
        fail("got exception on cleanup: " + e.getMessage());
      }
    }

    OmKeyArgs keyArgs = genKeyArgs(snapshotKeyPrefix + key1);

    KeyInfoWithVolumeContext omKeyInfo = writeClient.getKeyInfo(keyArgs, false);
    assertEquals(omKeyInfo.getKeyInfo().getKeyName(), snapshotKeyPrefix + key1);

    KeyInfoWithVolumeContext fileInfo = writeClient.getKeyInfo(keyArgs, false);
    assertEquals(fileInfo.getKeyInfo().getKeyName(), snapshotKeyPrefix + key1);

    OzoneFileStatus ozoneFileStatus = writeClient.getFileStatus(keyArgs);
    assertEquals(ozoneFileStatus.getKeyInfo().getKeyName(),
        snapshotKeyPrefix + key1);
  }

  @Test
  public void testListDeleteKey() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    createBucket(vol, bucket);
    OzoneBucket volBucket = vol.getBucket(bucket);

    String key = "key-";
    createFileKeyWithPrefix(volBucket, key);
    String snapshotKeyPrefix = createSnapshot(volume, bucket);
    deleteKeys(volBucket);

    int volBucketKeyCount = keyCount(volBucket, snapshotKeyPrefix + "key-");
    assertEquals(1, volBucketKeyCount);

    snapshotKeyPrefix = createSnapshot(volume, bucket);
    Iterator<? extends OzoneKey> volBucketIter2 =
            volBucket.listKeys(snapshotKeyPrefix);
    while (volBucketIter2.hasNext()) {
      fail("The last snapshot should not have any keys in it!");
    }
  }

  @Test
  public void testListAddNewKey() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    createBucket(vol, bucket);
    OzoneBucket bucket1 = vol.getBucket(bucket);

    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snapshotKeyPrefix1 = createSnapshot(volume, bucket);

    String key2 = "key-2-";
    createFileKeyWithPrefix(bucket1, key2);
    String snapshotKeyPrefix2 = createSnapshot(volume, bucket);

    int volBucketKeyCount = keyCount(bucket1, snapshotKeyPrefix1 + "key-");
    assertEquals(1, volBucketKeyCount);


    int volBucketKeyCount2 = keyCount(bucket1, snapshotKeyPrefix2 + "key-");
    assertEquals(2, volBucketKeyCount2);

    deleteKeys(bucket1);
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

  @Test
  public void testNonExistentBucket() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    //create volume but not bucket
    store.createVolume(volume);

    OMException omException = assertThrows(OMException.class,
        () -> createSnapshot(volume, bucket));
    assertEquals(BUCKET_NOT_FOUND, omException.getResult());
  }

  @Test
  public void testCreateSnapshotMissingMandatoryParams() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap1);

    String nullstr = "";
    // Bucket is empty
    assertThrows(IllegalArgumentException.class,
            () -> createSnapshot(volume, nullstr));
    // Volume is empty
    assertThrows(IllegalArgumentException.class,
            () -> createSnapshot(nullstr, bucket));
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

  @Test
  public void testSnapshotOpensWithDisabledAutoCompaction() throws Exception {
    String snapPrefix = createSnapshot(volumeName, bucketName);
    try (RDBStore snapshotDBStore = (RDBStore)
        ((OmSnapshot) cluster.getOzoneManager().getOmSnapshotManager()
            .getActiveFsMetadataOrSnapshot(volumeName, bucketName, snapPrefix).get())
            .getMetadataManager().getStore()) {
      for (String table : snapshotDBStore.getTableNames().values()) {
        assertTrue(snapshotDBStore.getDb().getColumnFamily(table)
            .getHandle().getDescriptor()
            .getOptions().disableAutoCompactions());
      }
    }
  }

  // Test snapshot diff when OM restarts in non-HA OM env and diff job is
  // in_progress when it restarts.

  @Test
  public void testSnapshotDiffWhenOmRestart() throws Exception {
    String snapshot1 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffResponse response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        disableNativeDiff);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    // Restart the OM and wait for sometime to make sure that previous snapDiff
    // job finishes.
    cluster.restartOzoneManager();
    stopKeyManager();
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS,
        () -> cluster.getOzoneManager().isRunning());

    response = store.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, forceFullSnapshotDiff,
        disableNativeDiff);

    // If job was IN_PROGRESS or DONE state when OM restarted, it should be
    // DONE by this time.
    // If job FAILED during crash (which mostly happens in the test because
    // of active snapshot checks), it would be removed by clean up service on
    // startup, and request after clean up will be considered a new request
    // and would return IN_PROGRESS. No other state is expected other than
    // IN_PROGRESS and DONE.
    if (response.getJobStatus() == DONE) {
      assertEquals(100, response.getSnapshotDiffReport().getDiffList().size());
    } else if (response.getJobStatus() == IN_PROGRESS) {
      SnapshotDiffReportOzone diffReport = fetchReportPage(volumeName,
          bucketName, snapshot1, snapshot2, null, 0);
      assertEquals(100, diffReport.getDiffList().size());
    } else {
      fail("Unexpected job status for the test.");
    }
  }

  // Test snapshot diff when OM restarts in non-HA OM env and report is
  // partially received.

  @Test
  public void testSnapshotDiffWhenOmRestartAndReportIsPartiallyFetched()
      throws Exception {
    int pageSize = 10;
    String snapshot1 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.secure().nextNumeric(5);
    createSnapshots(snapshot1, snapshot2);

    SnapshotDiffReportOzone diffReport = fetchReportPage(volumeName,
        bucketName, snapshot1, snapshot2, null, pageSize);

    List<DiffReportEntry> diffReportEntries = diffReport.getDiffList();

    // Restart the OM and no need to wait because snapDiff job finished before
    // the restart.
    cluster.restartOzoneManager();
    stopKeyManager();
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS,
        () -> cluster.getOzoneManager().isRunning());

    while (isNotEmpty(diffReport.getToken())) {
      diffReport = fetchReportPage(volumeName, bucketName, snapshot1,
          snapshot2, diffReport.getToken(), pageSize);
      diffReportEntries.addAll(diffReport.getDiffList());
    }
    assertEquals(100, diffReportEntries.size());
  }

  private SnapshotDiffReportOzone fetchReportPage(String volName,
                                                  String buckName,
                                                  String fromSnapshot,
                                                  String toSnapshot,
                                                  String token,
                                                  int pageSize)
      throws IOException, InterruptedException {

    while (true) {
      SnapshotDiffResponse response = store.snapshotDiff(volName, buckName,
          fromSnapshot, toSnapshot, token, pageSize, forceFullSnapshotDiff,
          disableNativeDiff);
      if (response.getJobStatus() == IN_PROGRESS) {
        Thread.sleep(response.getWaitTimeInMs());
      } else if (response.getJobStatus() == DONE) {
        return response.getSnapshotDiffReport();
      } else {
        fail("Unexpected job status for the test.");
      }
    }
  }

  private void createSnapshots(String snapshot1,
                               String snapshot2) throws Exception {
    createFileKeyWithPrefix(ozoneBucket, "key");
    store.createSnapshot(volumeName, bucketName, snapshot1);

    for (int i = 0; i < 100; i++) {
      createFileKeyWithPrefix(ozoneBucket, "key-" + i);
    }

    store.createSnapshot(volumeName, bucketName, snapshot2);
  }

  @Test
  public void testCompactionDagDisableForSnapshotMetadata() throws Exception {
    String snapshotName = createSnapshot(volumeName, bucketName);

    RDBStore activeDbStore = getRdbStore();
    // RocksDBCheckpointDiffer should be not null for active DB store.
    assertNotNull(activeDbStore.getRocksDBCheckpointDiffer());
    assertEquals(2,  activeDbStore.getDbOptions().listeners().size());

    OmSnapshot omSnapshot = (OmSnapshot) cluster.getOzoneManager()
        .getOmSnapshotManager()
        .getActiveFsMetadataOrSnapshot(volumeName, bucketName, snapshotName).get();

    RDBStore snapshotDbStore =
        (RDBStore) omSnapshot.getMetadataManager().getStore();
    // RocksDBCheckpointDiffer should be null for snapshot DB store.
    assertNull(snapshotDbStore.getRocksDBCheckpointDiffer());
    assertEquals(0, snapshotDbStore.getDbOptions().listeners().size());
  }

  @Test
  @Slow("HDDS-9299")
  public void testDayWeekMonthSnapshotCreationAndExpiration() throws Exception {
    String volumeA = "vol-a-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.secure().nextNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    createBucket(volA, bucketA);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);

    int latestDayIndex = 0;
    int latestWeekIndex = 0;
    int latestMonthIndex = 0;
    int oldestDayIndex = latestDayIndex;
    int oldestWeekIndex = latestWeekIndex;
    int oldestMonthIndex = latestMonthIndex;
    int daySnapshotRetentionPeriodDays = 7;
    int weekSnapshotRetentionPeriodWeek = 1;
    int monthSnapshotRetentionPeriodMonth = 1;
    int[] updatedDayIndexArr;
    int[] updatedWeekIndexArr;
    int[] updatedMonthIndexArr;
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 4; j++) {
        for (int k = 0; k < 7; k++) {
          // If there are seven day's snapshots in cluster already,
          // remove the oldest day snapshot then create the latest day snapshot
          updatedDayIndexArr = checkSnapshotExpirationThenCreateLatest(
              SNAPSHOT_DAY_PREFIX, oldestDayIndex, latestDayIndex,
              oldestWeekIndex, latestWeekIndex,
              oldestMonthIndex, latestMonthIndex,
              daySnapshotRetentionPeriodDays, volumeA, bucketA, volAbucketA);
          oldestDayIndex = updatedDayIndexArr[0];
          latestDayIndex = updatedDayIndexArr[1];
        }
        // If there is one week's snapshot in cluster already,
        // remove the oldest week snapshot then create the latest week snapshot
        updatedWeekIndexArr = checkSnapshotExpirationThenCreateLatest(
            SNAPSHOT_WEEK_PREFIX, oldestDayIndex, latestDayIndex,
            oldestWeekIndex, latestWeekIndex,
            oldestMonthIndex, latestMonthIndex,
            weekSnapshotRetentionPeriodWeek, volumeA, bucketA, volAbucketA);
        oldestWeekIndex = updatedWeekIndexArr[0];
        latestWeekIndex = updatedWeekIndexArr[1];
      }
      // If there is one month's snapshot in cluster already,
      // remove the oldest month snapshot then create the latest month snapshot
      updatedMonthIndexArr = checkSnapshotExpirationThenCreateLatest(
          SNAPSHOT_MONTH_PREFIX, oldestDayIndex, latestDayIndex,
          oldestWeekIndex, latestWeekIndex, oldestMonthIndex, latestMonthIndex,
          monthSnapshotRetentionPeriodMonth, volumeA, bucketA, volAbucketA);
      oldestMonthIndex = updatedMonthIndexArr[0];
      latestMonthIndex = updatedMonthIndexArr[1];
    }
  }

  @SuppressWarnings("parameternumber")
  private int[] checkSnapshotExpirationThenCreateLatest(String snapshotPrefix,
      int oldestDayIndex, int latestDayIndex,
      int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex,
      int snapshotRetentionPeriod,
      String volumeNameStr, String bucketNameStr, OzoneBucket ozoneBucketClient)
      throws Exception {
    int targetOldestIndex = 0;
    int targetLatestIndex = 0;
    switch (snapshotPrefix) {
    case SNAPSHOT_DAY_PREFIX:
      targetOldestIndex = oldestDayIndex;
      targetLatestIndex = latestDayIndex;
      break;
    case SNAPSHOT_WEEK_PREFIX:
      targetOldestIndex = oldestWeekIndex;
      targetLatestIndex = latestWeekIndex;
      break;
    case SNAPSHOT_MONTH_PREFIX:
      targetOldestIndex = oldestMonthIndex;
      targetLatestIndex = latestMonthIndex;
      break;
    default:
    }
    targetOldestIndex = deleteOldestSnapshot(targetOldestIndex,
        targetLatestIndex, snapshotPrefix, snapshotRetentionPeriod,
        oldestDayIndex, latestDayIndex, oldestWeekIndex, latestWeekIndex,
        oldestMonthIndex, latestMonthIndex, volumeNameStr, bucketNameStr,
        snapshotPrefix + targetOldestIndex, ozoneBucketClient);

    // When it's day, create a new key.
    // Week/month period will only create new snapshot
    if (snapshotPrefix.equals(SNAPSHOT_DAY_PREFIX)) {
      createFileKeyWithData(ozoneBucketClient, KEY_PREFIX + latestDayIndex);
    }
    createSnapshot(volumeNameStr, bucketNameStr,
        snapshotPrefix + targetLatestIndex);
    targetLatestIndex++;
    return new int[]{targetOldestIndex, targetLatestIndex};
  }

  @SuppressWarnings("parameternumber")
  private int deleteOldestSnapshot(int targetOldestIndex, int targetLatestIndex,
      String snapshotPrefix, int snapshotRetentionPeriod, int oldestDayIndex,
      int latestDayIndex, int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex, String volumeNameStr,
      String bucketNameStr, String snapshotName, OzoneBucket ozoneBucketClient)
        throws Exception {
    if (targetLatestIndex - targetOldestIndex >= snapshotRetentionPeriod) {
      store.deleteSnapshot(volumeNameStr, bucketNameStr, snapshotName);
      targetOldestIndex++;

      if (snapshotPrefix.equals(SNAPSHOT_DAY_PREFIX)) {
        oldestDayIndex = targetOldestIndex;
      } else if (snapshotPrefix.equals(SNAPSHOT_WEEK_PREFIX)) {
        oldestWeekIndex = targetOldestIndex;
      } else if (snapshotPrefix.equals(SNAPSHOT_MONTH_PREFIX)) {
        oldestMonthIndex = targetOldestIndex;
      }

      checkDayWeekMonthSnapshotData(ozoneBucketClient,
          oldestDayIndex, latestDayIndex,
          oldestWeekIndex, latestWeekIndex,
          oldestMonthIndex, latestMonthIndex);
    }
    return targetOldestIndex;
  }

  private void checkDayWeekMonthSnapshotData(OzoneBucket ozoneBucketClient,
      int oldestDayIndex, int latestDayIndex,
      int oldestWeekIndex, int latestWeekIndex,
      int oldestMonthIndex, int latestMonthIndex) throws Exception {
    for (int i = 0; i < latestDayIndex; i++) {
      String keyName = KEY_PREFIX + i;
      // Validate keys metadata in active Ozone namespace
      OzoneKeyDetails ozoneKeyDetails = ozoneBucketClient.getKey(keyName);
      assertEquals(keyName, ozoneKeyDetails.getName());
      assertEquals(linkedBuckets.getOrDefault(ozoneBucketClient.getName(), ozoneBucketClient.getName()),
          ozoneKeyDetails.getBucketName());
      assertEquals(ozoneBucketClient.getVolumeName(),
          ozoneKeyDetails.getVolumeName());

      // Validate keys data in active Ozone namespace
      try (OzoneInputStream ozoneInputStream =
               ozoneBucketClient.readKey(keyName)) {
        byte[] fileContent = new byte[keyName.length()];
        IOUtils.readFully(ozoneInputStream, fileContent);
        assertEquals(keyName, new String(fileContent, UTF_8));
      }
    }
    // Validate history day snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_DAY_PREFIX, oldestDayIndex,
        latestDayIndex, ozoneBucketClient);
    // Validate history week snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_WEEK_PREFIX, oldestWeekIndex,
        latestWeekIndex, ozoneBucketClient);
    // Validate history month snapshot data integrity
    validateSnapshotDataIntegrity(SNAPSHOT_MONTH_PREFIX, oldestMonthIndex,
        latestMonthIndex, ozoneBucketClient);
  }

  private void validateSnapshotDataIntegrity(String snapshotPrefix,
      int oldestIndex, int latestIndex, OzoneBucket ozoneBucketClient)
      throws Exception {
    if (latestIndex == 0) {
      return;
    }
    // Verify key exists from the oldest day/week/month snapshot
    // to latest snapshot
    for (int i = oldestIndex; i < latestIndex; i++) {
      Iterator<? extends OzoneKey> iterator =
          ozoneBucketClient.listKeys(
              OmSnapshotManager.getSnapshotPrefix(snapshotPrefix + i));
      while (iterator.hasNext()) {
        String keyName = iterator.next().getName();
        try (OzoneInputStream ozoneInputStream =
                 ozoneBucketClient.readKey(keyName)) {
          // We previously created key content as only "key-{index}"
          // Thus need to remove snapshot related prefix from key name by regex
          // before use key name to compare key content
          // e.g.,".snapshot/snap-day-1/key-0" -> "key-0"
          Matcher snapKeyNameMatcher = SNAPSHOT_KEY_PATTERN.matcher(keyName);
          if (snapKeyNameMatcher.matches()) {
            String truncatedSnapshotKeyName = snapKeyNameMatcher.group(3);
            byte[] fileContent = new byte[truncatedSnapshotKeyName.length()];
            IOUtils.readFully(ozoneInputStream, fileContent);
            assertEquals(truncatedSnapshotKeyName,
                new String(fileContent, UTF_8));
          }
        }
      }
    }
  }

  private void createFileKeyWithData(OzoneBucket bucket, String keyName)
      throws IOException {
    OzoneOutputStream fileKey = bucket.createKey(keyName,
        keyName.length());
    // Use key name as key content
    fileKey.write(keyName.getBytes(UTF_8));
    fileKey.close();
  }

  private String getKeySuffix(int index) {
    return leftPad(Integer.toString(index), 10, "0");
  }

  // End-to-end test to verify that compaction DAG only tracks 'keyTable',
  // 'directoryTable' and 'fileTable' column families. And only these
  // column families are used in SST diff calculation.

  @Test
  public void testSnapshotCompactionDag() throws Exception {
    String volume1 = "volume-1-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket1 = "bucket-1-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket2 = "bucket-2-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket3 = "bucket-3-" + RandomStringUtils.secure().nextNumeric(5);

    store.createVolume(volume1);
    OzoneVolume ozoneVolume = store.getVolume(volume1);
    createBucket(ozoneVolume, bucket1);
    OzoneBucket ozoneBucket1 = ozoneVolume.getBucket(bucket1);

    DBStore activeDbStore = ozoneManager.getMetadataManager().getStore();

    for (int i = 0; i < 100; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap1");
    activeDbStore.compactDB();

    createBucket(ozoneVolume, bucket2);
    OzoneBucket ozoneBucket2 = ozoneVolume.getBucket(bucket2);

    for (int i = 100; i < 200; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
      createFileKey(ozoneBucket2, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap2");
    createSnapshot(volume1, bucket2, "bucket2-snap1");
    activeDbStore.compactDB();

    createBucket(ozoneVolume, bucket3);
    OzoneBucket ozoneBucket3 = ozoneVolume.getBucket(bucket3);

    for (int i = 200; i < 300; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket1, keyName);
      createFileKey(ozoneBucket2, keyName);
      createFileKey(ozoneBucket3, keyName);
    }

    createSnapshot(volume1, bucket1, "bucket1-snap3");
    createSnapshot(volume1, bucket2, "bucket2-snap2");
    createSnapshot(volume1, bucket3, "bucket3-snap1");
    activeDbStore.compactDB();

    for (int i = 300; i < 400; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket3, keyName);
      createFileKey(ozoneBucket2, keyName);
    }

    createSnapshot(volume1, bucket2, "bucket2-snap3");
    createSnapshot(volume1, bucket3, "bucket3-snap2");
    activeDbStore.compactDB();

    for (int i = 400; i < 500; i++) {
      String keyName = "/dir1/dir2/dir3/key-" + getKeySuffix(i);
      createFileKey(ozoneBucket3, keyName);
    }

    createSnapshot(volume1, bucket3, "bucket3-snap3");

    List<CompactionNode> filteredNodes = ozoneManager.getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer()
        .getCompactionNodeMap().values().stream()
        .filter(node ->
            !COLUMN_FAMILIES_TO_TRACK_IN_DAG.contains(node.getColumnFamily()))
        .collect(Collectors.toList());

    assertEquals(0, filteredNodes.size());

    assertEquals(100,
        fetchReportPage(volume1, bucket1, "bucket1-snap1", "bucket1-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket1, "bucket1-snap2", "bucket1-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket1, "bucket1-snap1", "bucket1-snap3",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket2, "bucket2-snap1", "bucket2-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket2, "bucket2-snap2", "bucket2-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket2, "bucket2-snap1", "bucket2-snap3",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket3, "bucket3-snap1", "bucket3-snap2",
            null, 0).getDiffList().size());
    assertEquals(100,
        fetchReportPage(volume1, bucket3, "bucket3-snap2", "bucket3-snap3",
            null, 0).getDiffList().size());
    assertEquals(200,
        fetchReportPage(volume1, bucket3, "bucket3-snap1", "bucket3-snap3",
            null, 0).getDiffList().size());
  }

  @Test
  public void testSnapshotReuseSnapName() throws Exception {
    // start KeyManager for this test
    startKeyManager();
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buck-" + counter.incrementAndGet();
    store.createVolume(volume);
    OzoneVolume volume1 = store.getVolume(volume);
    createBucket(volume1, bucket);
    OzoneBucket bucket1 = volume1.getBucket(bucket);
    // Create Key1 and take snapshot
    String key1 = "key-1-";
    createFileKeyWithPrefix(bucket1, key1);
    String snap1 = "snap" + counter.incrementAndGet();
    String snapshotKeyPrefix = createSnapshot(volume, bucket, snap1);

    int keyCount1 = keyCount(bucket1, snapshotKeyPrefix + "key-");
    assertEquals(1, keyCount1);

    store.deleteSnapshot(volume, bucket, snap1);

    GenericTestUtils.waitFor(() -> {
      try {
        return !ozoneManager.getMetadataManager().getSnapshotInfoTable()
            .isExist(SnapshotInfo.getTableKey(volume, bucket, snap1));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 200, 10000);

    createFileKeyWithPrefix(bucket1, key1);
    String snap2 = "snap" + counter.incrementAndGet();
    createSnapshot(volume, bucket, snap2);

    String key2 = "key-2-";
    createFileKeyWithPrefix(bucket1, key2);
    createSnapshot(volume, bucket, snap1);

    int keyCount2 = keyCount(bucket1, snapshotKeyPrefix + "key-");
    assertEquals(3, keyCount2);

    // Stop key manager after testcase executed
    stopKeyManager();
  }
}
