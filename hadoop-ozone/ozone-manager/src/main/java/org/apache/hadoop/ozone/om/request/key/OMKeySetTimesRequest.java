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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.key.OMKeySetTimesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTimesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTimesResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle add SetTime request for key.
 */
public class OMKeySetTimesRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeySetTimesRequest.class);

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preExecute(ozoneManager);
    SetTimesRequest setTimesRequest = request.getSetTimesRequest();
    String keyPath = setTimesRequest.getKeyName();
    String normalizedKeyPath =
        validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            keyPath, getBucketLayout());

    return request.toBuilder()
        .setSetTimesRequest(
            setTimesRequest.toBuilder()
                .setVolumeName(getVolumeName())
                .setBucketName(getBucketName())
                .setKeyName(normalizedKeyPath)
                .setMtime(getModificationTime()))
        .build();
  }

  private String volumeName;
  private String bucketName;
  private String keyName;
  //private long creationTime;
  private long modificationTime;

  public OMKeySetTimesRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
    OzoneManagerProtocolProtos.SetTimesRequest setTimesRequest =
        getOmRequest().getSetTimesRequest();
    volumeName = setTimesRequest.getVolumeName();
    bucketName = setTimesRequest.getBucketName();
    keyName = setTimesRequest.getKeyName();
    // ignore accessTime
    modificationTime = setTimesRequest.getMtime();
  }

  protected String getVolumeName() {
    return volumeName;
  }

  protected String getBucketName() {
    return bucketName;
  }

  protected String getKeyName() {
    return keyName;
  }

  protected long getModificationTime() {
    return modificationTime;
  }

  OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetTimesResponse(SetTimesResponse.newBuilder());
    return new OMKeySetTimesResponse(omResponse.build(), omKeyInfo);
  }

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse
   * @param exception
   * @return OMClientResponse
   */
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMKeySetTimesResponse(createErrorOMResponse(
        omResponse, exception), getBucketLayout());
  }

  void onComplete(Result result, boolean operationResult,
      IOException exception, long trxnLogIndex, AuditLogger auditLogger,
      Map<String, String> auditMap) {
    switch (result) {
    case SUCCESS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set time: {} to path: {} success!", modificationTime,
            getKeyName());
      }
      break;
    case FAILURE:
      LOG.error("Set time {} to path {} failed!", modificationTime,
          getKeyName(), exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeySetTimesRequest: {}",
          getOmRequest());
    }

    auditMap.put(OzoneConsts.VOLUME, getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, getBucketName());
    auditMap.put(OzoneConsts.KEY, getKeyName());
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(getModificationTime()));
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_TIMES, auditMap,
        exception, getOmRequest().getUserInfo()));
  }

  void apply(OmKeyInfo omKeyInfo, long trxnLogIndex) {
    // No need to check not null here, this will be never called with null.
    long mtime = getModificationTime();
    if (mtime != -1) {
      omKeyInfo.setModificationTime(getModificationTime());
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    ozoneManager.getMetrics().incNumSetTime();
    OmKeyInfo omKeyInfo = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    String key = null;
    boolean operationResult = false;
    Result result = null;
    try {
      volume = getVolumeName();
      bucket = getBucketName();
      key = getKeyName();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }
      lockAcquired =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume,
              bucket);

      String dbKey = omMetadataManager.getOzoneKey(volume, bucket, key);
      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(dbKey);

      if (omKeyInfo == null) {
        throw new OMException(OMException.ResultCodes.KEY_NOT_FOUND);
      }

      operationResult = true;
      apply(omKeyInfo, trxnLogIndex);
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // update cache.
      omMetadataManager.getKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(dbKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));

      omClientResponse = onSuccess(omResponse, omKeyInfo, operationResult);
      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (lockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume,
            bucket);
      }
    }

    Map<String, String> auditMap = new LinkedHashMap<>();
    onComplete(result, operationResult, exception, trxnLogIndex,
        ozoneManager.getAuditLogger(), auditMap);

    return omClientResponse;
  }
}

