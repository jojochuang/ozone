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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyIdentifier;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.Assert.assertTrue;

public class TestFileLeaseManagerIntegrated {
  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private BucketLayout bucketLayout;
  private String volumeName;
  private Path volumePath;
  private String bucketName;
  private Path bucketPath;

  @Rule
  public Timeout timeout = Timeout.seconds(360);

  @Before
  public void setupClass()
      throws InterruptedException, TimeoutException, IOException {
    bucketLayout = BucketLayout.FILE_SYSTEM_OPTIMIZED;
    conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes()
        //.setClusterId(clusterId).setScmId(scmId).setOmId(omId)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = bucket.getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);
  }

  @After
  public void shutdownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Make sure the lease is restored even if only the inode has the record.
   */
  @Test
  public void testLeaseRestorationOnRestart() throws Exception {
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem dfs = FileSystem.get(conf);

    // Create an empty file
    String path = "testLeaseRestorationOnRestart";
    Path fullPath = new Path(bucketPath, path);
    FSDataOutputStream out = dfs.create(fullPath);
    out.hsync();

    // Remove the lease from the lease manager, but leave it in the inode.
    //FSDirectory dir = cluster.getNamesystem().getFSDirectory();
    //FileLeaseManager.INodeFile file = dir.getINode(path).asFile();
    OMMetadataManager metaMgr =cluster.getOzoneManager().getMetadataManager();
    OzoneFileStatus toKeyParentDirStatus = OMFileRequest.getOMKeyInfoIfExists(
        metaMgr, volumeName, bucketName, path, 0);
    OmKeyInfo keyInfo = toKeyParentDirStatus.getKeyInfo();
    FileLeaseManager.INodeFile iNodeFile =
        new FileLeaseManager.INodeFile(new KeyIdentifier(volumeName, bucketName, path, null));
    String clientId = keyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
    cluster.getOzoneManager().getFileLeaseManager().removeLease(
        clientId, iNodeFile);

    // Save a fsimage.
    /*dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    cluster.getNameNodeRpc().saveNamespace(0,0);
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);*/

    // Restart the OM.
    cluster.restartOzoneManager();

    // Check whether the lease manager has the lease
    //dir = cluster.getNamesystem().getFSDirectory();
    //file = dir.getINode(path).asFile();
    assertTrue("Lease should exist.",
        cluster.getOzoneManager().getFileLeaseManager().getLease(iNodeFile) != null);
  }
}
