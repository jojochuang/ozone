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

package org.apache.hadoop.ozone.om.response.nfs;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_FILE_LEASE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_TRANSFER_RANGE_TABLE;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/** Persists a file lease transition in the same OM batch as its Ratis request. */
@CleanupTableInfo(cleanupTables = {NFS_FILE_LEASE_TABLE, NFS_TRANSFER_RANGE_TABLE})
public class OMNfsFileLeaseResponse extends OMClientResponse {
  private final String leaseKey;
  private final OmNfsLeaseInfo lease;
  private final boolean deleteLease;
  private final Map<String, OmNfsTransferRangeInfo> cleanupRanges;

  public OMNfsFileLeaseResponse(OMResponse response, String leaseKey, OmNfsLeaseInfo lease, boolean deleteLease,
      Map<String, OmNfsTransferRangeInfo> cleanupRanges) {
    super(response);
    this.leaseKey = leaseKey;
    this.lease = lease;
    this.deleteLease = deleteLease;
    this.cleanupRanges = cleanupRanges;
  }

  public OMNfsFileLeaseResponse(OMResponse response) {
    super(response);
    checkStatusNotOK();
    leaseKey = null;
    lease = null;
    deleteLease = false;
    cleanupRanges = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager manager, BatchOperation batch) throws IOException {
    if (deleteLease) {
      manager.getNfsFileLeaseTable().deleteWithBatch(batch, leaseKey);
    } else {
      manager.getNfsFileLeaseTable().putWithBatch(batch, leaseKey, lease);
    }
    if (cleanupRanges != null) {
      for (Map.Entry<String, OmNfsTransferRangeInfo> entry : cleanupRanges.entrySet()) {
        manager.getNfsTransferRangeTable().putWithBatch(batch, entry.getKey(), entry.getValue());
      }
    }
  }
}
