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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.Test;

/**
 * Tests OMBucketDeleteRequest class which handles DeleteBucket request.
 */
public class TestOMBucketDeleteRequestWithFSO
    extends TestOMBucketDeleteRequest {

  @Test
  public void testValidateAndUpdateCacheWithFSO() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    assertEquals(0, omMetrics.getNumFSOBucketDeletes());

    OzoneManagerProtocolProtos.OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // Create Volume and bucket entries in DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1);

    assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

    assertEquals(1, omMetrics.getNumFSOBucketDeletes());
  }

  @Test
  public void testActiveNfsExportLeaseBlocksBucketDelete() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    String exportUuid = UUID.randomUUID().toString();
    String leaseKey = omMetadataManager.getNfsExportLeaseKey(exportUuid);
    omMetadataManager.getNfsExportLeaseTable().addCacheEntry(leaseKey,
        OmNfsLeaseInfo.createExportLease(exportUuid, volumeId, bucketId, 1, 2000, "gateway", 1), 1);

    OMBucketDeleteRequest request = new OMBucketDeleteRequest(
        createDeleteBucketRequest(volumeName, bucketName).toBuilder()
            .setDeleteBucketRequest(DeleteBucketRequest.newBuilder()
                .setVolumeName(volumeName).setBucketName(bucketName).setLeaderTimeMillis(1000))
            .build());
    assertEquals(Status.NFS_LEASE_CONFLICT,
        request.validateAndUpdateCache(ozoneManager, 2).getOMResponse().getStatus());
    assertNotNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));
  }

  private OMRequest createDeleteBucketRequest(
      String volumeName,
      String bucketName) {
    return OMRequest.newBuilder()
        .setDeleteBucketRequest(
            DeleteBucketRequest.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
