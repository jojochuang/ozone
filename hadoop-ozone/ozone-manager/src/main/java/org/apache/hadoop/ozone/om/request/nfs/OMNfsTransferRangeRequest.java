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

package org.apache.hadoop.ozone.om.request.nfs;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NFS_LEASE_CONFLICT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.nfs.OMNfsTransferRangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsTransferRangeInfo.State;
import org.apache.hadoop.util.Time;

/** Applies a fenced, idempotent NFS write-range manifest mutation through Ratis. */
public class OMNfsTransferRangeRequest extends OMClientRequest {
  public OMNfsTransferRangeRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    NfsTransferRangeRequest rangeRequest = request.getNfsTransferRangeRequest();
    validateRequest(rangeRequest);
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE,
          IAccessAuthorizer.ACLType.WRITE, rangeRequest.getVolumeName(), rangeRequest.getBucketName(), null);
    }
    return request.toBuilder().setNfsTransferRangeRequest(
        rangeRequest.toBuilder().setLeaderTimeMillis(Time.now())).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    NfsTransferRangeRequest request = getOmRequest().getNfsTransferRangeRequest();
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("exportUUID", request.getExportUUID());
    auditMap.put("objectID", Long.toUnsignedString(request.getObjectID()));
    auditMap.put("offset", Long.toUnsignedString(request.getOffset()));
    auditMap.put("transferID", request.getTransferID());
    auditMap.put("operation", request.getOperation().name());
    boolean locked = false;
    Exception exception = null;
    OMClientResponse clientResponse;
    try {
      validateRequest(request);
      if (!request.hasLeaderTimeMillis() || request.getLeaderTimeMillis() <= 0) {
        throw new OMException("NFS transfer range is missing the OM leader timestamp", INVALID_REQUEST);
      }
      if (!metadataManager.isNfsDurableWriteRangesEnabled() || !metadataManager.isNfsObjectIndexReady()) {
        throw new OMException("NFS durable write ranges are not enabled", NOT_SUPPORTED_OPERATION);
      }
      if ((request.getOperation() == NfsTransferRangeRequest.Operation.PREPARE
          || request.getOperation() == NfsTransferRangeRequest.Operation.PROMOTE
          || request.getOperation() == NfsTransferRangeRequest.Operation.MARK_CLEANUP)
          && !metadataManager.isNfsStagingIntentsEnabled()) {
        throw new OMException("NFS staging intents are not enabled", NOT_SUPPORTED_OPERATION);
      }
      if (request.getOperation() == NfsTransferRangeRequest.Operation.UPSERT
          && metadataManager.isNfsStagingIntentsEnabled()) {
        throw new OMException("NFS ranges require a prepared staging intent", NOT_SUPPORTED_OPERATION);
      }
      locked = metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, request.getVolumeName(), request.getBucketName()).isLockAcquired();
      if (!locked) {
        throw new OMException("Cannot acquire NFS transfer bucket lock", INVALID_REQUEST);
      }
      OmBucketInfo bucket = metadataManager.getBucketTable().get(
          metadataManager.getBucketKey(request.getVolumeName(), request.getBucketName()));
      if (bucket == null) {
        throw new OMException("NFS transfer bucket does not exist", BUCKET_NOT_FOUND);
      }
      if (bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        throw new OMException("NFS durable writes require a native FSO bucket", NOT_SUPPORTED_OPERATION);
      }

      long volumeId = metadataManager.getVolumeId(request.getVolumeName());
      long bucketId = bucket.getObjectID();
      OmNfsLeaseInfo exportLease = metadataManager.getNfsExportLeaseTable().get(
          metadataManager.getNfsExportLeaseKey(request.getExportUUID()));
      String leaseKey = metadataManager.getNfsFileLeaseKey(volumeId, bucketId, request.getObjectID());
      OmNfsLeaseInfo fileLease = metadataManager.getNfsFileLeaseTable().get(leaseKey);
      if (exportLease == null || fileLease == null) {
        throw new OMException("NFS transfer lease is missing", NFS_LEASE_CONFLICT);
      }
      try {
        fileLease.validateFileCommit(exportLease, request.getExportUUID(), request.getGatewayID(),
            request.getGatewayGeneration(), request.getTransferID(), request.getLeaseGeneration(),
            request.getOwnerToken(), request.getLeaderTimeMillis());
      } catch (IllegalStateException conflict) {
        throw new OMException(conflict.getMessage(), NFS_LEASE_CONFLICT);
      }

      boolean prepare = request.getOperation() == NfsTransferRangeRequest.Operation.PREPARE;
      boolean promote = request.getOperation() == NfsTransferRangeRequest.Operation.PROMOTE;
      boolean markCleanup = request.getOperation() == NfsTransferRangeRequest.Operation.MARK_CLEANUP;
      boolean delete = request.getOperation() == NfsTransferRangeRequest.Operation.DELETE;
      OmNfsTransferRangeInfo range = new OmNfsTransferRangeInfo(request.getExportUUID(), volumeId, bucketId,
          request.getObjectID(), request.getLeaseGeneration(), request.getOffset(), request.getLength(),
          request.getTransferID(), request.getStagingKey(), request.getSha256(), context.getIndex(),
          prepare ? State.PENDING_STAGE : State.RECORDED, 0);
      String rangeKey = metadataManager.getNfsTransferRangeKey(request.getExportUUID(), volumeId, bucketId,
          request.getObjectID(), request.getLeaseGeneration(), request.getOffset(), request.getStagingKey());
      Table<String, OmNfsTransferRangeInfo> rangeTable = metadataManager.getNfsTransferRangeTable();
      OmNfsTransferRangeInfo existing = rangeTable.get(rangeKey);
      if ((delete || markCleanup) && existing == null) {
        Map.Entry<String, OmNfsTransferRangeInfo> recoveredRange = findRecoveredRange(metadataManager, rangeTable,
            volumeId, bucketId, request);
        if (recoveredRange != null) {
          rangeKey = recoveredRange.getKey();
          existing = recoveredRange.getValue();
          range = existing;
        }
      }
      if (markCleanup && (existing == null || existing.getLeaseGeneration() >= request.getLeaseGeneration())) {
        throw new OMException("NFS staging intent is not from a fenced lease generation", NFS_LEASE_CONFLICT);
      }
      boolean newRange = existing == null || !sameManifest(existing, range);
      boolean stateChange = promote && existing != null && existing.getState() == State.PENDING_STAGE
          || markCleanup && existing != null && existing.getState() == State.PENDING_STAGE;
      range = applyIntentTransition(existing, range, request, context.getIndex());
      OmNfsLeaseInfo updatedFileLease = null;
      if (delete || markCleanup) {
        if (existing != null && !sameDeleteManifest(existing, volumeId, bucketId, request)) {
          throw new OMException("NFS transfer range delete does not match the manifest", NFS_LEASE_CONFLICT);
        }
      } else if (existing != null && existing.getStagingKey().equals(range.getStagingKey())
          && !sameManifest(existing, range)) {
        throw new OMException("NFS staging key was reused for different range data", NFS_LEASE_CONFLICT);
      }

      long transactionIndex = existing != null && sameManifest(existing, range) && !stateChange
          ? existing.getTransactionIndex() : context.getIndex();
      if (transactionIndex != range.getTransactionIndex()) {
        range = new OmNfsTransferRangeInfo(request.getExportUUID(), volumeId, bucketId, request.getObjectID(),
            range.getLeaseGeneration(), request.getOffset(), request.getLength(), request.getTransferID(),
            request.getStagingKey(), request.getSha256(), transactionIndex, range.getState(),
            range.getCleanupAfterMillis());
      }
      if (delete) {
        rangeTable.addCacheEntry(rangeKey, context.getIndex());
      } else if (!(prepare && existing != null || promote && existing != null && !stateChange)) {
        rangeTable.addCacheEntry(rangeKey, range, context.getIndex());
        if ((!prepare && newRange || promote && stateChange) && fileLease.getProtobuf().getContentPublished()) {
          updatedFileLease = fileLease.withContentPublished(false);
          metadataManager.getNfsFileLeaseTable().addCacheEntry(leaseKey, updatedFileLease, context.getIndex());
        }
      }
      response.setNfsTransferRangeResponse(NfsTransferRangeResponse.newBuilder().setRecorded(true)
          .setTransactionIndex(transactionIndex).build());
      clientResponse = new OMNfsTransferRangeResponse(response.build(), rangeKey, range, delete,
          updatedFileLease == null ? null : leaseKey, updatedFileLease);
    } catch (IOException | IllegalArgumentException | IllegalStateException | ArithmeticException ex) {
      exception = ex;
      if (ex instanceof IllegalStateException) {
        exception = new OMException(ex.getMessage(), NFS_LEASE_CONFLICT);
      } else if (ex instanceof IllegalArgumentException || ex instanceof ArithmeticException) {
        exception = new OMException(ex.getMessage(), INVALID_REQUEST);
      }
      clientResponse = new OMNfsTransferRangeResponse(createErrorOMResponse(response, exception));
    } finally {
      if (locked) {
        metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, request.getVolumeName(), request.getBucketName());
      }
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.NFS_TRANSFER_RANGE, auditMap, exception, getOmRequest().getUserInfo()));
    }
    return clientResponse;
  }

  private static boolean sameManifest(OmNfsTransferRangeInfo existing, OmNfsTransferRangeInfo incoming) {
    return existing.getExportUUID().equals(incoming.getExportUUID())
        && existing.getVolumeID() == incoming.getVolumeID() && existing.getBucketID() == incoming.getBucketID()
        && existing.getObjectID() == incoming.getObjectID()
        && existing.getLeaseGeneration() == incoming.getLeaseGeneration()
        && existing.getOffset() == incoming.getOffset() && existing.getLength() == incoming.getLength()
        && existing.getTransferID().equals(incoming.getTransferID())
        && existing.getStagingKey().equals(incoming.getStagingKey())
        && existing.getSha256().equals(incoming.getSha256());
  }

  private static OmNfsTransferRangeInfo applyIntentTransition(OmNfsTransferRangeInfo existing,
      OmNfsTransferRangeInfo incoming, NfsTransferRangeRequest request, long transactionIndex)
      throws OMException {
    if (request.getOperation() == NfsTransferRangeRequest.Operation.PREPARE) {
      if (existing != null && (!sameManifest(existing, incoming) || existing.getState() == State.CLEANUP_PENDING)) {
        throw new OMException("NFS staging intent conflicts with its existing manifest", NFS_LEASE_CONFLICT);
      }
      return existing == null ? incoming : existing;
    }
    if (request.getOperation() == NfsTransferRangeRequest.Operation.PROMOTE) {
      if (existing == null || !sameManifest(existing, incoming) || existing.getState() == State.CLEANUP_PENDING) {
        throw new OMException("NFS staging intent cannot be promoted", NFS_LEASE_CONFLICT);
      }
      return existing.getState() == State.PENDING_STAGE
          ? existing.withState(State.RECORDED, 0, transactionIndex) : existing;
    }
    if (request.getOperation() == NfsTransferRangeRequest.Operation.MARK_CLEANUP) {
      if (existing.getState() == State.RECORDED) {
        throw new OMException("Acknowledged NFS ranges must remain recoverable", NFS_LEASE_CONFLICT);
      }
      return existing.getState() == State.CLEANUP_PENDING ? existing
          : existing.withState(State.CLEANUP_PENDING, request.getLeaderTimeMillis(), transactionIndex);
    }
    if (request.getOperation() != NfsTransferRangeRequest.Operation.DELETE && existing != null
        && existing.getState() == State.CLEANUP_PENDING) {
      throw new OMException("NFS staging range is pending cleanup", NFS_LEASE_CONFLICT);
    }
    return incoming;
  }

  private static Map.Entry<String, OmNfsTransferRangeInfo> findRecoveredRange(OMMetadataManager metadataManager,
      Table<String, OmNfsTransferRangeInfo> rangeTable, long volumeId, long bucketId,
      NfsTransferRangeRequest request) throws IOException {
    String prefix = metadataManager.getNfsObjectKey(volumeId, bucketId, request.getObjectID())
        + "/" + request.getExportUUID() + "/";
    try (Table.KeyValueIterator<String, OmNfsTransferRangeInfo> iterator =
             rangeTable.iterator(prefix, org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE)) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmNfsTransferRangeInfo> entry = iterator.next();
        if (sameDeleteManifest(entry.getValue(), volumeId, bucketId, request)) {
          return new java.util.AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue());
        }
      }
    }
    return null;
  }

  private static boolean sameDeleteManifest(OmNfsTransferRangeInfo existing, long volumeId, long bucketId,
      NfsTransferRangeRequest request) {
    return existing.getExportUUID().equals(request.getExportUUID()) && existing.getVolumeID() == volumeId
        && existing.getBucketID() == bucketId && existing.getObjectID() == request.getObjectID()
        && existing.getOffset() == request.getOffset() && existing.getLength() == request.getLength()
        && existing.getTransferID().equals(request.getTransferID())
        && existing.getStagingKey().equals(request.getStagingKey()) && existing.getSha256().equals(request.getSha256());
  }

  private static void validateRequest(NfsTransferRangeRequest request) throws OMException {
    if (!request.isInitialized()) {
      throw new OMException("NFS transfer range request is incomplete", INVALID_REQUEST);
    }
    try {
      UUID.fromString(request.getExportUUID());
    } catch (IllegalArgumentException ex) {
      throw new OMException("Invalid NFS export UUID", INVALID_REQUEST);
    }
    if (request.getVolumeName().isEmpty() || request.getBucketName().isEmpty() || request.getGatewayID().isEmpty()
        || request.getTransferID().isEmpty()) {
      throw new OMException("NFS transfer range identity is incomplete", INVALID_REQUEST);
    }
    if (request.getOwnerToken().isEmpty() || request.getGatewayGeneration() == 0 || request.getLeaseGeneration() == 0) {
      throw new OMException("NFS transfer range lease identity is incomplete", INVALID_REQUEST);
    }
    if (request.getOffset() < 0 || request.getLength() <= 0 || request.getStagingKey().isEmpty()
        || request.getSha256().size() != 32 || request.getStagingKey().startsWith("/")) {
      throw new OMException("NFS transfer range data is invalid", INVALID_REQUEST);
    }
    for (String component : request.getStagingKey().split("/")) {
      if (component.equals("..")) {
        throw new OMException("NFS staging key contains a parent traversal", INVALID_REQUEST);
      }
    }
    try {
      Math.addExact(request.getOffset(), request.getLength());
    } catch (ArithmeticException ex) {
      throw new OMException("NFS transfer range end overflows", INVALID_REQUEST);
    }
  }
}
