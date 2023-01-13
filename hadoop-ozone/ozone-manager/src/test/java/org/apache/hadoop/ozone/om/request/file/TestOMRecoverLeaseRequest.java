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

package org.apache.hadoop.ozone.om.request.file;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverLeaseRequest;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests OMRecoverLeaseRequest
 */
public class TestOMRecoverLeaseRequest extends TestOMKeyRequest {

    private long parentId = Long.MIN_VALUE;

    @Override
    public BucketLayout getBucketLayout() {
        return BucketLayout.FILE_SYSTEM_OPTIMIZED;
    }

    @NotNull
    protected OMRequest createRecoverLeaseRequest(
            String volumeName, String bucketName, String keyName) {

        RecoverLeaseRequest recoverLeaseRequest = RecoverLeaseRequest.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName).build();

        return OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.RecoverLease)
                .setClientId(UUID.randomUUID().toString())
                .setRecoverLeaseRequest(recoverLeaseRequest).build();

    }

    @Test
    public void testValidateAndUpdateCache() throws Exception {
        String parentDir = "c/d/e";
        String fileName = "f";
        keyName = parentDir + "/" + fileName;
        OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
        // Create parent dirs for the path
        parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, parentDir, omMetadataManager);

        OMRequest modifiedOmRequest = doPreExecute(createRecoverLeaseRequest(
            volumeName, bucketName, keyName));

        OMRecoverLeaseRequest omRecoverLeaseRequest = getOmRecoverLeaseRequest(
            modifiedOmRequest);

        OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, omRecoverLeaseRequest.getBucketLayout());

        // add to both key table and open key table
        List<OmKeyLocationInfo> allocatedLocationList = getKeyLocation(3)
            .stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());
        String openKey = addToOpenFileTable(allocatedLocationList);
        //String ozoneKey = getOzonePathKey();

        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(
                omRecoverLeaseRequest.getBucketLayout()).get(openKey);
        Assert.assertNotNull(omKeyInfo);

        String ozoneKey = addToFileTable(allocatedLocationList);
        omKeyInfo =
            omMetadataManager.getKeyTable(omRecoverLeaseRequest.getBucketLayout())
                .get(ozoneKey);

        Assert.assertNotNull(omKeyInfo);

        OMClientResponse omClientResponse =
            omRecoverLeaseRequest.validateAndUpdateCache(ozoneManager,
                100L, ozoneManagerDoubleBufferHelper);

        Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

        // Entry should be deleted from openKey Table.
        omKeyInfo =
            omMetadataManager.getOpenKeyTable(omRecoverLeaseRequest.getBucketLayout())
                .get(openKey);
        Assert.assertNull(omKeyInfo);

        // Now entry should be created in key Table.
        omKeyInfo =
            omMetadataManager.getKeyTable(omRecoverLeaseRequest.getBucketLayout())
                .get(ozoneKey);

        Assert.assertNotNull(omKeyInfo);
    }

    protected OMRecoverLeaseRequest getOmRecoverLeaseRequest(OMRequest omRequest) {
        return new OMRecoverLeaseRequest(omRequest);
    }

    private List<KeyLocation> getKeyLocation(int count) {
        List<KeyLocation> keyLocations = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            KeyLocation keyLocation =
                KeyLocation.newBuilder()
                    .setBlockID(HddsProtos.BlockID.newBuilder()
                        .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                            .setContainerID(i + 1000).setLocalID(i + 100).build()))
                    .setOffset(0).setLength(200).setCreateVersion(version).build();
            keyLocations.add(keyLocation);
        }
        return keyLocations;
    }

    private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

        OMRecoverLeaseRequest omRecoverLeaseRequest =
            getOmRecoverLeaseRequest(originalOMRequest);

        OMRequest modifiedOmRequest = omRecoverLeaseRequest.preExecute(ozoneManager);

        // assert keyName is normalized
        return modifiedOmRequest;
    }

    String addToOpenFileTable(List<OmKeyLocationInfo> locationList)
        throws Exception {
        OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            replicationType, replicationFactor, 0, parentId, 0, Time.now(), version);
        omKeyInfo.appendNewBlocks(locationList, false);

        OMRequestTestUtils.addFileToKeyTable(
            true, false, omKeyInfo.getFileName(),
            omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

        final long volumeId = omMetadataManager.getVolumeId(
            omKeyInfo.getVolumeName());
        final long bucketId = omMetadataManager.getBucketId(
            omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

        return omMetadataManager.getOpenFileName(volumeId, bucketId,
            omKeyInfo.getParentObjectID(), omKeyInfo.getFileName(), clientID);
    }

    String addToFileTable(List<OmKeyLocationInfo> locationList)
        throws Exception {
        OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            replicationType, replicationFactor, 0, parentId, 0, Time.now(), version);
        omKeyInfo.appendNewBlocks(locationList, false);

        OMRequestTestUtils.addFileToKeyTable(
            false, false, omKeyInfo.getFileName(),
            omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

        final long volumeId = omMetadataManager.getVolumeId(
            omKeyInfo.getVolumeName());
        final long bucketId = omMetadataManager.getBucketId(
            omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

        return omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId, omKeyInfo.getFileName());
    }

    // TODO: verify that recover a closed file should be allowed (essentially no-op)
    @Test
    public void testRecoverClosedFile() throws Exception {

    }

    // TODO: recover an EC file is not supported

    // TODO: verify that recover an open (not yet hsync'ed) file doesn't work.

    // TODO: verify that recover an file that doesn't exist throws exception.
}
