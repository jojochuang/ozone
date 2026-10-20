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

package org.apache.hadoop.ozone.om.request.file;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

class TestOMFileRequest {

  @Test
  void getKeyPathComponentsPreservesUnicodeNames() {
    assertEquals(Arrays.asList("café 文件", "資料"),
        OMFileRequest.getKeyPathComponents("café 文件/資料"));
  }

  @Test
  void nfsFileLeaseConflictContinuesUntilRecoveryReleasesExpiredLease() throws Exception {
    long leaderTimeMillis = Time.now() - 60_000;
    long objectId = 3;
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease("export", 1, 2, 1,
        leaderTimeMillis + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, objectId, "transfer", "owner",
        ByteString.copyFromUtf8("token"), leaderTimeMillis, 30_000);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    Table<String, OmNfsLeaseInfo> leaseTable = mock(Table.class);
    OmKeyInfo keyInfo = mock(OmKeyInfo.class);
    when(keyInfo.getVolumeName()).thenReturn("volume");
    when(keyInfo.getBucketName()).thenReturn("bucket");
    when(keyInfo.getObjectID()).thenReturn(objectId);
    when(metadataManager.getVolumeId("volume")).thenReturn(1L);
    when(metadataManager.getBucketId("volume", "bucket")).thenReturn(2L);
    when(metadataManager.getNfsFileLeaseKey(1, 2, objectId)).thenReturn("lease");
    when(metadataManager.getNfsFileLeaseTable()).thenReturn(leaseTable);
    when(leaseTable.get("lease")).thenReturn(fileLease);

    assertTrue(fileLease.getProtobuf().getExpiresAtMillis() < Time.now());
    assertThrows(OMException.class, () -> OMFileRequest.validateNfsFileLeaseAbsent(metadataManager, keyInfo));
    when(leaseTable.get("lease")).thenReturn(null);
    assertDoesNotThrow(() -> OMFileRequest.validateNfsFileLeaseAbsent(metadataManager, keyInfo));
  }

  @Test
  void expiredDescendantLeaseBlocksDirectoryMutation() throws Exception {
    long leaderTimeMillis = Time.now() - 60_000;
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease("export", 1, 2, 1,
        leaderTimeMillis + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, 3, "transfer", "owner",
        ByteString.copyFromUtf8("token"), leaderTimeMillis, 30_000);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    Table<String, OmNfsLeaseInfo> leaseTable = mock(Table.class);
    Table.KeyValueIterator<String, OmNfsLeaseInfo> leaseIterator = mock(Table.KeyValueIterator.class);
    Table.KeyValue<String, OmNfsLeaseInfo> leaseEntry = mock(Table.KeyValue.class);
    Table<String, OmNfsObjectInfo> objectTable = mock(Table.class);
    when(metadataManager.isNfsObjectIndexEnabled()).thenReturn(true);
    when(metadataManager.isNfsObjectIndexReady()).thenReturn(true);
    when(metadataManager.getNfsFileLeaseTable()).thenReturn(leaseTable);
    when(metadataManager.getNfsObjectTable()).thenReturn(objectTable);
    when(metadataManager.getNfsObjectKey(1, 2, 0)).thenReturn("1/2/0");
    when(metadataManager.getNfsObjectKey(1, 2, 3)).thenReturn("1/2/3");
    when(objectTable.get("1/2/3")).thenReturn(
        new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 2, "leased"));
    when(leaseTable.iterator("1/2/")).thenReturn(leaseIterator);
    when(leaseTable.cacheIterator()).thenReturn(Collections.emptyIterator());
    when(leaseIterator.hasNext()).thenReturn(true, false);
    when(leaseIterator.next()).thenReturn(leaseEntry);
    when(leaseEntry.getKey()).thenReturn("1/2/3");
    when(leaseEntry.getValue()).thenReturn(fileLease);

    assertTrue(fileLease.getProtobuf().getExpiresAtMillis() < Time.now());
    assertThrows(OMException.class,
        () -> OMFileRequest.validateNfsDirectoryLeasesReleased(metadataManager, 1, 2, 2));
  }
}
