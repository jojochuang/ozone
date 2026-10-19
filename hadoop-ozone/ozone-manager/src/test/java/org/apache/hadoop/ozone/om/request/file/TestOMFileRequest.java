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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

class TestOMFileRequest {

  @Test
  void getKeyPathComponentsPreservesUnicodeNames() {
    assertEquals(Arrays.asList("café 文件", "資料"),
        OMFileRequest.getKeyPathComponents("café 文件/資料"));
  }

  @Test
  void nfsFileLeaseConflictUsesReplicatedLeaderTime() throws Exception {
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

    assertThrows(OMException.class,
        () -> OMFileRequest.validateNfsFileLeaseInactive(metadataManager, keyInfo, leaderTimeMillis));
    assertThrows(OMException.class, () -> OMFileRequest.validateNfsFileLeaseInactive(metadataManager, keyInfo, 0));
    assertDoesNotThrow(() -> OMFileRequest.validateNfsFileLeaseInactive(metadataManager, keyInfo,
        leaderTimeMillis + 30_000));
  }
}
