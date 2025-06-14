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

import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;

import java.util.List;

/**
 * Helper class for building purge requests in the directory deletion process.
 */
public class PurgeRequestBuilder {

  private PurgeRequestBuilder() {
    // Utility class, should not be instantiated
  }

  /**
   * Creates a PurgePathRequest from directory and file information.
   *
   * @param volumeId The volume ID
   * @param bucketId The bucket ID
   * @param purgeDeletedDir The directory to be purged, or null if not purging
   * @param purgeDeletedFiles List of files to purge
   * @param markDirsAsDeleted List of directories to mark as deleted
   * @return A PurgePathRequest object
   */
  public static PurgePathRequest buildPurgeRequest(
      final long volumeId,
      final long bucketId,
      final String purgeDeletedDir,
      final List<OmKeyInfo> purgeDeletedFiles,
      final List<OmKeyInfo> markDirsAsDeleted) {

    PurgePathRequest.Builder purgePathsRequest = PurgePathRequest.newBuilder();
    purgePathsRequest.setVolumeId(volumeId);
    purgePathsRequest.setBucketId(bucketId);

    if (purgeDeletedDir != null) {
      purgePathsRequest.setDeletedDir(purgeDeletedDir);
    }

    for (OmKeyInfo purgeFile : purgeDeletedFiles) {
      purgePathsRequest.addDeletedSubFiles(
          purgeFile.getProtobuf(true, ClientVersion.CURRENT_VERSION));
    }

    // Add these directories to deletedDirTable, so that its sub-paths will be
    // traversed in next iteration to ensure cleanup all sub-children.
    for (OmKeyInfo dir : markDirsAsDeleted) {
      purgePathsRequest.addMarkDeletedSubDirs(
          dir.getProtobuf(ClientVersion.CURRENT_VERSION));
    }

    return purgePathsRequest.build();
  }

  /**
   * Creates a PurgeDirectoriesRequest with multiple purge path requests.
   *
   * @param requests List of PurgePathRequest objects
   * @param snapTableKey The snapshot table key, or null for AOS
   * @param expectedPreviousSnapshotId The expected previous snapshot ID
   * @param clientId The client ID
   * @return An OMRequest for purging directories
   */
  public static OzoneManagerProtocolProtos.OMRequest buildPurgeDirectoriesRequest(
      List<PurgePathRequest> requests,
      String snapTableKey,
      org.apache.hadoop.hdds.utils.db.UUID expectedPreviousSnapshotId,
      UUID clientId) {

    OzoneManagerProtocolProtos.PurgeDirectoriesRequest.Builder purgeDirRequest =
        OzoneManagerProtocolProtos.PurgeDirectoriesRequest.newBuilder();

    if (snapTableKey != null) {
      purgeDirRequest.setSnapshotTableKey(snapTableKey);
    }

    OzoneManagerProtocolProtos.NullableUUID.Builder expectedPreviousSnapshotNullableUUID =
        OzoneManagerProtocolProtos.NullableUUID.newBuilder();
    if (expectedPreviousSnapshotId != null) {
      expectedPreviousSnapshotNullableUUID.setUuid(
          org.apache.hadoop.hdds.HddsUtils.toProtobuf(expectedPreviousSnapshotId));
    }
    purgeDirRequest.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID.build());

    purgeDirRequest.addAllDeletedPath(requests);

    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
        .setPurgeDirectoriesRequest(purgeDirRequest)
        .setClientId(clientId.toString())
        .build();
  }

  /**
   * Creates a SetSnapshotPropertyRequest for updating snapshot exclusive size.
   *
   * @param exclusiveSize The exclusive size
   * @param exclusiveReplicatedSize The exclusive replicated size
   * @param snapshotID The snapshot ID
   * @return A SetSnapshotPropertyRequest
   */
  public static OzoneManagerProtocolProtos.SetSnapshotPropertyRequest buildSnapshotSizeUpdateRequest(
      long exclusiveSize, long exclusiveReplicatedSize, UUID snapshotID, String tableKey) {

    OzoneManagerProtocolProtos.SnapshotSize snapshotSize =
        OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
            .setExclusiveSize(exclusiveSize)
            .setExclusiveReplicatedSize(exclusiveReplicatedSize)
            .build();

    return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
        .setSnapshotKey(tableKey)
        .setSnapshotSizeDeltaFromDirDeepCleaning(snapshotSize)
        .build();
  }

  /**
   * Creates a SetSnapshotPropertyRequest for marking a snapshot as deep-cleaned.
   *
   * @param snapshotTableKey The snapshot table key
   * @return A SetSnapshotPropertyRequest
   */
  public static OzoneManagerProtocolProtos.SetSnapshotPropertyRequest buildDeepCleanedFlagRequest(
      String snapshotTableKey) {

    return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
        .setSnapshotKey(snapshotTableKey)
        .setDeepCleanedDeletedDir(true)
        .build();
  }
}
