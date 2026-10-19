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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequestWithFSO;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test Key ACL requests for prefix layout.
 */
public class TestOMKeyAclRequestWithFSO extends TestOMKeyAclRequest {

  @Test
  public void testKeyAclUpdateRejectsActiveNfsFileLease() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();
    OmKeyInfo keyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    long now = Time.now();
    String exportUuid = UUID.randomUUID().toString();
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, volumeId, bucketId,
        1, now + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, keyInfo.getObjectID(),
        "transfer", "nfs-user@EXAMPLE.COM", ByteString.copyFromUtf8("owner-token"), now, 10_000);
    omMetadataManager.getNfsExportLeaseTable().put(omMetadataManager.getNfsExportLeaseKey(exportUuid), exportLease);
    omMetadataManager.getNfsFileLeaseTable().put(
        omMetadataManager.getNfsFileLeaseKey(volumeId, bucketId, keyInfo.getObjectID()), fileLease);

    OzoneManagerProtocolProtos.OMRequest request = createAddAclkeyRequest(
        org.apache.hadoop.ozone.OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]"));
    OMClientResponse response = getOmKeyAddAclRequest(request).validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(OzoneManagerProtocolProtos.Status.NFS_LEASE_CONFLICT, response.getOMResponse().getStatus());
    OmKeyInfo unchangedKey = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(keyInfo.getAcls(), unchangedKey.getAcls());
    assertEquals(keyInfo.getModificationTime(), unchangedKey.getModificationTime());
  }

  @Override
  protected String addKeyToTable() throws Exception {
    String parentDir = "c/d/e";
    String fileName = "file1";
    keyName = parentDir + "/" + fileName; // updated key name

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils
        .addParentsToDirTable(volumeName, bucketName, parentDir,
            omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, fileName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    OMRequestTestUtils
        .addFileToKeyTable(false, false, fileName, omKeyInfo, -1, 50,
            omMetadataManager);
    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    return omMetadataManager.getOzonePathKey(
        volumeId, bucketId, omKeyInfo.getParentObjectID(),
        fileName);
  }

  @Override
  protected OMKeyAclRequest getOmKeyAddAclRequest(
      OzoneManagerProtocolProtos.OMRequest originalRequest) {
    return new OMKeyAddAclRequestWithFSO(originalRequest, getBucketLayout());
  }

  @Override
  protected OMKeyAclRequest getOmKeyRemoveAclRequest(
      OzoneManagerProtocolProtos.OMRequest removeAclRequest) {
    return new OMKeyRemoveAclRequestWithFSO(removeAclRequest,
        getBucketLayout());
  }

  @Override
  protected OMKeyAclRequest getOmKeySetAclRequest(
      OzoneManagerProtocolProtos.OMRequest setAclRequest) {
    return new OMKeySetAclRequestWithFSO(setAclRequest, getBucketLayout());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
