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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.NfsObjectLookup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.protocolPB.grpc.GrpcClientConstants;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.apache.hadoop.ozone.util.ConcurrentMutableRate;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.Logger;

/**
 * Test ozone metadata reader.
 */
public class TestOMMetadataReader {

  private static final String ACCESS_KEY_ID = "ASIA7O1AJD8VV4KCEAX5";
  private static final String VOLUME_NAME = "s3v";
  private static final String BUCKET_NAME = "bucket123";
  private static final String KEY_PREFIX = "prefix";
  private static final long MAX_KEYS = 100L;

  @AfterEach
  public void clearOmThreadLocals() {
    OzoneManager.setStsTokenIdentifier(null);
    OzoneManager.setS3Auth(null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"alice", "bob"})
  public void testNfsLookupAuthorizesResolvedIdentity(String userName) throws Exception {
    boolean allowed = userName.equals("alice");
    UserGroupInformation caller = UserGroupInformation.createProxyUser(userName,
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    KeyManager keyManager = mock(KeyManager.class);
    OMMetadataManager manager = mock(OMMetadataManager.class);
    IOzoneManagerLock lock = mock(IOzoneManagerLock.class);
    AtomicBoolean bucketLockHeld = new AtomicBoolean();
    when(manager.getLock()).thenReturn(lock);
    doAnswer(invocation -> {
      bucketLockHeld.set(true);
      return null;
    }).when(lock).acquireReadLock(any(), anyString(), anyString());
    doAnswer(invocation -> {
      bucketLockHeld.set(false);
      return null;
    }).when(lock).releaseReadLock(any(), anyString(), anyString());
    when(authorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class))).thenAnswer(invocation -> {
      assertFalse(bucketLockHeld.get(), "NFS ACL checks must not hold the bucket lock");
      OzoneObj object = invocation.getArgument(0);
      RequestContext context = invocation.getArgument(1);
      return object.getResourceType() != KEY || "alice".equals(context.getClientUgi().getUserName());
    });
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    when(manager.getBucketTable()).thenReturn(bucketTable);
    when(manager.getBucketKey(VOLUME_NAME, BUCKET_NAME)).thenReturn("bucket-key");
    when(bucketTable.get("bucket-key")).thenReturn(new OmBucketInfo.Builder().setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME).setOwner("bucket-owner").setUpdateID(5).build());
    when(keyManager.getMetadataManager()).thenReturn(manager);
    OmKeyInfo file = new OmKeyInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("moved/file").setObjectID(42).setUpdateID(54).build();
    when(keyManager.getFileStatus(any(), anyString())).thenAnswer(invocation -> {
      assertTrue(bucketLockHeld.get(), "NFS metadata must be revalidated while holding the bucket lock");
      return new OzoneFileStatus(file, 1024, false);
    });
    when(keyManager.lookupFile(any(), anyString())).thenReturn(file);
    OmMetadataReader reader = createMetadataReader(authorizer, keyManager);
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("").build();
    try (MockedStatic<OMFileRequest> paths = mockStatic(OMFileRequest.class);
         MockedStatic<Server> server = mockStatic(Server.class)) {
      server.when(Server::getRemoteUser).thenReturn(caller);
      paths.when(() -> OMFileRequest.getNfsObjectPath(manager, VOLUME_NAME, BUCKET_NAME, 1, 2, 42,
          NfsFileHandle.Kind.FILE)).thenReturn("moved/file");
      if (allowed) {
        NfsObjectLookup lookup = reader.lookupNfsObjectDetails(args, 1, 2, 42, NfsFileHandle.Kind.FILE);
        assertEquals(42, lookup.getStatus().getKeyInfo().getObjectID());
        assertEquals(54, lookup.getChangeId());
        ArgumentCaptor<OmKeyArgs> resolved = ArgumentCaptor.forClass(OmKeyArgs.class);
        verify(keyManager).lookupFile(resolved.capture(), anyString());
        assertEquals("moved/file", resolved.getValue().getKeyName());
      } else {
        assertEquals(ResultCodes.PERMISSION_DENIED, assertThrows(OMException.class,
            () -> reader.lookupNfsObject(args, 1, 2, 42, NfsFileHandle.Kind.FILE)).getResult());
        verify(keyManager, never()).getFileStatus(any(), anyString());
        verify(keyManager, never()).lookupFile(any(), anyString());
      }
      ArgumentCaptor<OzoneObj> objects = ArgumentCaptor.forClass(OzoneObj.class);
      ArgumentCaptor<RequestContext> contexts = ArgumentCaptor.forClass(RequestContext.class);
      verify(authorizer, org.mockito.Mockito.atLeastOnce()).checkAccess(objects.capture(), contexts.capture());
      for (RequestContext context : contexts.getAllValues()) {
        assertEquals(userName, context.getClientUgi().getUserName());
        assertEquals("nfs-gateway", context.getClientUgi().getRealUser().getUserName());
      }
      if (allowed) {
        assertTrue(objects.getAllValues().stream().anyMatch(obj -> "moved/file".equals(obj.getKeyName())));
      }
    }
  }

  @Test
  public void testNfsBucketLookupReturnsBucketVerifier() throws Exception {
    UserGroupInformation caller = UserGroupInformation.createProxyUser("alice",
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class))).thenReturn(true);
    KeyManager keyManager = mock(KeyManager.class);
    OMMetadataManager manager = mock(OMMetadataManager.class);
    IOzoneManagerLock lock = mock(IOzoneManagerLock.class);
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    OmBucketInfo bucket = new OmBucketInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setOwner("bucket-owner").setUpdateID(73).build();
    when(manager.getLock()).thenReturn(lock);
    when(manager.getBucketTable()).thenReturn(bucketTable);
    when(manager.getBucketKey(VOLUME_NAME, BUCKET_NAME)).thenReturn("bucket-key");
    when(bucketTable.get("bucket-key")).thenReturn(bucket);
    when(keyManager.getMetadataManager()).thenReturn(manager);
    when(keyManager.getFileStatus(any(), anyString())).thenReturn(new OzoneFileStatus());
    OmMetadataReader reader = createMetadataReader(authorizer, keyManager);
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("").build();
    try (MockedStatic<OMFileRequest> paths = mockStatic(OMFileRequest.class);
         MockedStatic<Server> server = mockStatic(Server.class)) {
      server.when(Server::getRemoteUser).thenReturn(caller);
      paths.when(() -> OMFileRequest.getNfsObjectPath(manager, VOLUME_NAME, BUCKET_NAME, 1, 2, 2,
          NfsFileHandle.Kind.BUCKET)).thenReturn("");

      NfsObjectLookup lookup = reader.lookupNfsObjectDetails(args, 1, 2, 2, NfsFileHandle.Kind.BUCKET);

      assertTrue(lookup.getStatus().isDirectory());
      assertEquals(73, lookup.getChangeId());
    }
  }

  @Test
  public void testNfsFileLeaseLookupOnlyMarksUnpublishedOwnerDataRecoverable() throws Exception {
    final String exportUuid = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
    final String gatewayId = "gateway";
    final String principal = "alice";
    final long now = System.currentTimeMillis();
    UserGroupInformation alice = UserGroupInformation.createProxyUser(principal,
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    UserGroupInformation bob = UserGroupInformation.createProxyUser("bob",
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    OMMetadataManager manager = mock(OMMetadataManager.class);
    IOzoneManagerLock lock = mock(IOzoneManagerLock.class);
    Table<String, OmBucketInfo> buckets = mock(Table.class);
    Table<String, OmNfsLeaseInfo> exportLeases = mock(Table.class);
    Table<String, OmNfsLeaseInfo> fileLeases = mock(Table.class);
    OmBucketInfo bucket = new OmBucketInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setObjectID(2).setOwner("bucket-owner").setBucketLayout(FILE_SYSTEM_OPTIMIZED).build();
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, 1, 2, 1, now + 60_000,
        gatewayId, 3);
    OmNfsLeaseInfo unpublished = OmNfsLeaseInfo.acquireFileLease(null, exportLease, 42, "transfer",
        principal, ByteString.copyFromUtf8("owner-token"), now, 30_000);
    when(manager.getLock()).thenReturn(lock);
    when(manager.isNfsWriterLeaseEnabled()).thenReturn(true);
    when(manager.isNfsObjectIndexReady()).thenReturn(true);
    when(manager.getBucketTable()).thenReturn(buckets);
    when(manager.getBucketKey(VOLUME_NAME, BUCKET_NAME)).thenReturn("bucket-key");
    when(buckets.get("bucket-key")).thenReturn(bucket);
    when(manager.getVolumeId(VOLUME_NAME)).thenReturn(1L);
    when(manager.getNfsExportLeaseTable()).thenReturn(exportLeases);
    when(manager.getNfsExportLeaseKey(exportUuid)).thenReturn(exportUuid);
    when(exportLeases.get(exportUuid)).thenReturn(exportLease);
    when(manager.getNfsFileLeaseTable()).thenReturn(fileLeases);
    when(manager.getNfsFileLeaseKey(1, 2, 42)).thenReturn("file-key");
    when(fileLeases.get("file-key")).thenReturn(unpublished, unpublished.withContentPublished(true), unpublished);
    KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getMetadataManager()).thenReturn(manager);
    OmKeyInfo file = new OmKeyInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("file").setObjectID(42).build();
    when(keyManager.getFileStatus(any(), anyString())).thenReturn(new OzoneFileStatus(file, 1024, false));
    when(keyManager.lookupFile(any(), anyString())).thenReturn(file);
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class))).thenReturn(true);
    OmMetadataReader reader = createMetadataReader(authorizer, keyManager);
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("").build();
    LookupNfsFileLeaseRequest request = LookupNfsFileLeaseRequest.newBuilder().setExportUUID(exportUuid)
        .setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME).setVolumeID(1).setBucketID(2).setObjectID(42)
        .setRequesterPrincipal(principal).setGatewayID(gatewayId).setGatewayGeneration(3).build();

    try (MockedStatic<OMFileRequest> paths = mockStatic(OMFileRequest.class);
         MockedStatic<Server> server = mockStatic(Server.class)) {
      paths.when(() -> OMFileRequest.getNfsObjectPath(manager, VOLUME_NAME, BUCKET_NAME, 1, 2, 42,
          NfsFileHandle.Kind.FILE)).thenReturn("file");
      server.when(Server::getRemoteUser).thenReturn(alice);
      assertTrue(reader.lookupNfsFileLease(args, request).getRecoverable());
      assertFalse(reader.lookupNfsFileLease(args, request).getRecoverable());
      server.when(Server::getRemoteUser).thenReturn(bob);
      assertFalse(reader.lookupNfsFileLease(args, request.toBuilder().setRequesterPrincipal("bob").build())
          .getRecoverable());
    }
  }

  @Test
  public void testNfsChildrenChecksAclsWithoutBucketLock() throws Exception {
    UserGroupInformation caller = UserGroupInformation.createProxyUser("alice",
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    KeyManager keyManager = mock(KeyManager.class);
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMMetadataManager manager = mock(OMMetadataManager.class);
    IOzoneManagerLock lock = mock(IOzoneManagerLock.class);
    AtomicBoolean bucketLockHeld = new AtomicBoolean();
    when(manager.getLock()).thenReturn(lock);
    doAnswer(invocation -> {
      bucketLockHeld.set(true);
      return null;
    }).when(lock).acquireReadLock(any(), anyString(), anyString());
    doAnswer(invocation -> {
      bucketLockHeld.set(false);
      return null;
    }).when(lock).releaseReadLock(any(), anyString(), anyString());
    when(authorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class))).thenAnswer(invocation -> {
      assertFalse(bucketLockHeld.get(), "NFS ACL checks must not hold the bucket lock");
      return true;
    });
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    when(manager.getBucketTable()).thenReturn(bucketTable);
    when(manager.getBucketKey(VOLUME_NAME, BUCKET_NAME)).thenReturn("bucket-key");
    when(bucketTable.get("bucket-key")).thenReturn(new OmBucketInfo.Builder().setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME).setOwner("bucket-owner").build());
    when(keyManager.getMetadataManager()).thenReturn(manager);
    doAnswer(invocation -> {
      assertFalse(bucketLockHeld.get(), "NFS bucket-link resolution must not hold the bucket lock");
      return new ResolvedBucket(VOLUME_NAME, BUCKET_NAME, VOLUME_NAME, BUCKET_NAME, "bucket-owner",
          FILE_SYSTEM_OPTIMIZED);
    }).when(ozoneManager).resolveBucketLink(any(OmKeyArgs.class));
    when(keyManager.listStatus(any(), eq(false), any(), any(long.class), anyString(), eq(false), eq(false)))
        .thenReturn(Collections.emptyList());
    OmMetadataReader reader = createMetadataReader(authorizer, keyManager, true, ozoneManager);
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("").build();
    try (MockedStatic<OMFileRequest> paths = mockStatic(OMFileRequest.class);
         MockedStatic<Server> server = mockStatic(Server.class)) {
      server.when(Server::getRemoteUser).thenReturn(caller);
      paths.when(() -> OMFileRequest.getNfsObjectPath(manager, VOLUME_NAME, BUCKET_NAME, 1, 2, 2,
          NfsFileHandle.Kind.BUCKET)).thenReturn("");

      NfsChildrenPage page = reader.lookupNfsChildren(args, 1, 2, 2, NfsFileHandle.Kind.BUCKET,
          null, 16, null, null);

      assertTrue(page.getChildren().isEmpty());
      verify(keyManager).listStatus(any(), eq(false), any(), eq(17L), anyString(), eq(false), eq(false));
      assertFalse(bucketLockHeld.get());

      OMException changed = assertThrows(OMException.class, () -> reader.lookupNfsChildren(args, 1, 2, 2,
          NfsFileHandle.Kind.BUCKET, null, 16, 9L, 4L));
      assertEquals(OMException.ResultCodes.NFS_DIRECTORY_CHANGED, changed.getResult());
    }
  }

  @Test
  public void testNfsChildrenIncludesRowsAfterInclusiveCursor() throws Exception {
    UserGroupInformation caller = UserGroupInformation.createProxyUser("alice",
        UserGroupInformation.createRemoteUser("nfs-gateway"));
    IAccessAuthorizer authorizer = mock(IAccessAuthorizer.class);
    when(authorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class))).thenReturn(true);
    KeyManager keyManager = mock(KeyManager.class);
    OMMetadataManager manager = mock(OMMetadataManager.class);
    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    Table<String, OmNfsObjectInfo> nfsObjectTable = mock(Table.class);
    OmBucketInfo bucket = new OmBucketInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setOwner("bucket-owner").setUpdateID(5).build();
    OmNfsObjectInfo cursor = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 2, "b");
    when(manager.getLock()).thenReturn(mock(IOzoneManagerLock.class));
    when(manager.getBucketTable()).thenReturn(bucketTable);
    when(manager.getBucketKey(VOLUME_NAME, BUCKET_NAME)).thenReturn("bucket-key");
    when(bucketTable.get("bucket-key")).thenReturn(bucket);
    when(manager.getNfsObjectTable()).thenReturn(nfsObjectTable);
    when(manager.getNfsObjectKey(1, 2, 11)).thenReturn("cursor-key");
    when(nfsObjectTable.get("cursor-key")).thenReturn(cursor);
    when(keyManager.getMetadataManager()).thenReturn(manager);
    OmKeyInfo directory = new OmKeyInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("dir").setObjectID(2).setUpdateID(7).build();
    when(keyManager.getFileStatus(any(), anyString())).thenReturn(new OzoneFileStatus(directory, 1024, true));
    List<OzoneFileStatus> listed = new ArrayList<>();
    for (String name : Arrays.asList("b", "c", "d", "e")) {
      OmKeyInfo child = new OmKeyInfo.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
          .setKeyName("dir/" + name).setObjectID(10 + listed.size()).setParentObjectID(2).build();
      listed.add(new OzoneFileStatus(child, 1024, false));
    }
    when(keyManager.listStatus(any(), eq(false), eq("dir/b"), eq(4L), nullable(String.class), eq(false), eq(false)))
        .thenReturn(listed);
    OmMetadataReader reader = createMetadataReader(authorizer, keyManager);
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME).setBucketName(BUCKET_NAME)
        .setKeyName("").build();

    try (MockedStatic<OMFileRequest> paths = mockStatic(OMFileRequest.class);
         MockedStatic<Server> server = mockStatic(Server.class)) {
      server.when(Server::getRemoteUser).thenReturn(caller);
      paths.when(() -> OMFileRequest.getNfsObjectPath(manager, VOLUME_NAME, BUCKET_NAME, 1, 2, 2,
          NfsFileHandle.Kind.DIRECTORY)).thenReturn("dir");

      NfsChildrenPage page = reader.lookupNfsChildren(args, 1, 2, 2, NfsFileHandle.Kind.DIRECTORY,
          null, 2, 11L, 7L);

      assertEquals(Arrays.asList("c", "d"), page.getChildren().stream()
          .map(status -> status.getKeyInfo().getFileName()).collect(Collectors.toList()));
      assertFalse(page.isEof());
    }
  }

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void testGetClientAddress() throws Exception {
    try (MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class)) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      // when (GRPC call with defined client address)
      String clientAddress = Context.current()
          .withValue(GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY,
              expectedClientAddressInCaseOfGrpcCall)
          .call(OmMetadataReader::getClientAddress);
      // then
      assertEquals(expectedClientAddressInCaseOfGrpcCall, clientAddress);

      // and when (GRPC call without client address)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals("", clientAddress);

      // and when (Hadoop RPC client call)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfHadoopRpcCall, clientAddress);
    }
  }

  @Test
  public void testCheckAclsAttachesSessionPolicyFromThreadLocal() throws Exception {
    final String sessionPolicy = "session-policy-from-thread-local";
    setupStsTokenIdentifier();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext.Builder contextWithoutSessionPolicyBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutSessionPolicyBuilder, true));

    verifySessionPolicyPassedToAuthorizer(accessAuthorizer, obj, sessionPolicy);
  }

  @Test
  public void testNoSessionPolicyWhenThreadLocalIsNull() throws Exception {
    // No STS token identifier in thread local
    OzoneManager.setStsTokenIdentifier(null);

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext.Builder contextWithoutSessionPolicyBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutSessionPolicyBuilder, true));

    verifySessionPolicyPassedToAuthorizer(accessAuthorizer, obj, null);
  }

  @Test
  public void testCheckAclsAttachesS3ActionFromThreadLocal() throws Exception {
    OzoneManager.setS3Auth(S3Authentication.newBuilder()
        .setAccessId(ACCESS_KEY_ID)
        .setS3Action("GetObject")
        .build());

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext.Builder contextWithoutS3ActionBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutS3ActionBuilder, true));

    verifyS3ActionPassedToAuthorizer(accessAuthorizer, obj, "GetObject");
  }

  @Test
  public void testCheckAclsDoesNotAttachS3ActionWhenStsFeatureDisabled() throws Exception {
    OzoneManager.setS3Auth(S3Authentication.newBuilder()
        .setAccessId(ACCESS_KEY_ID)
        .setS3Action("GetObject")
        .build());

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, mock(KeyManager.class), false);

    final RequestContext.Builder contextWithoutS3ActionBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutS3ActionBuilder, true));

    verifyS3ActionPassedToAuthorizer(accessAuthorizer, obj, null);
  }

  @Test
  public void testCheckAclsLeavesS3ActionUnsetWhenS3AuthThreadLocalNull() throws Exception {
    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext.Builder contextWithoutS3ActionBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, contextWithoutS3ActionBuilder, true));

    verifyS3ActionPassedToAuthorizer(accessAuthorizer, obj, null);
  }

  @Test
  public void testCheckAclsAttachesSessionPolicyAndS3ActionFromThreadLocals() throws Exception {
    setupStsTokenIdentifier();

    OzoneManager.setS3Auth(S3Authentication.newBuilder()
        .setAccessId(ACCESS_KEY_ID)
        .setS3Action("PutObject")
        .build());

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer);

    final RequestContext.Builder baseContextBuilder = createTestRequestContextBuilder();
    final OzoneObj obj = createTestOzoneObj();

    assertTrue(omMetadataReader.checkAcls(obj, baseContextBuilder, true));

    verifySessionPolicyAndS3ActionPassedToAuthorizer(accessAuthorizer, obj);
  }

  @Test
  public void testListStatusUsesReadAclForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);

    // For STS S3 requests, listStatus() performs these checks:
    // 1. Volume READ (for volume access)
    // 2) Key READ (for the specific prefix being listed)
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, KEY_PREFIX);
  }

  @Test
  public void testListStatusUsesReadAclForNonStsRequest() throws Exception {
    setupNonStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertTrue(checks.stream().allMatch(check -> check.getContext().getAclRights() == READ));

    assertContainsVolumeReadCheck(checks);
    // We want to ensure the current behavior for non-STS requests remains the same
    assertContainsKeyReadCheckWithName(checks);
  }

  @Test
  public void testListStatusUsesListPrefixForAclWhenKeyNameEmptyAndListPrefixSet() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("")
        .setListPrefix("userA/")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "userA/");
  }

  @Test
  public void testListStatusUsesWildcardForAclWhenKeyNameAndListPrefixEmpty() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "*");
  }

  @Test
  public void testListStatusUsesListPrefixForAclWhenKeyNameIsDescendantOfListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("userA")
        .setListPrefix("user")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "user");
  }

  @Test
  public void testListStatusUsesListPrefixForAclWhenKeyNameIsAncestorOfListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("user")
        .setListPrefix("user/foo")
        .build();

    final List<OzoneFileStatus> statuses = omMetadataReader.listStatus(args, false, "", MAX_KEYS, false);
    assertTrue(statuses.isEmpty());

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "user/foo");
  }

  @Test
  public void testListStatusThrowsWhenStsKeyNameNotUnderListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_PREFIX)
        .setListPrefix("other/")
        .build();

    final OMException ex = assertThrows(
        OMException.class, () -> omMetadataReader.listStatus(args, false, "", MAX_KEYS, false));
    assertEquals(ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  public void testListStatusLightUsesListPrefixForAclWhenKeyNameIsAncestorOfListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("user")
        .setListPrefix("user/foo")
        .build();

    omMetadataReader.listStatusLight(args, false, "", MAX_KEYS, false);

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "user/foo");
  }

  @Test
  public void testListStatusLightThrowsWhenStsKeyNameNotUnderListPrefix() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListStatusKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_PREFIX)
        .setListPrefix("other/")
        .build();

    final OMException ex = assertThrows(
        OMException.class, () -> omMetadataReader.listStatusLight(args, false, "", MAX_KEYS, false));
    assertEquals(ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  public void testGetFileStatusUsesReadAclForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createGetFileStatusKeyManagerReturningStatus();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    omMetadataReader.getFileStatus(args);

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks, KEY_PREFIX);
  }

  @Test
  public void testGetFileStatusUsesReadAclForNonStsS3Request() throws Exception {
    setupNonStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createGetFileStatusKeyManagerReturningStatus();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);
    final OmKeyArgs args = createOmKeyArgs();

    omMetadataReader.getFileStatus(args);

    final List<AclCheck> checks = captureAclChecks(accessAuthorizer, 2);
    assertContainsVolumeReadCheck(checks);
    assertContainsKeyReadCheckWithName(checks);
  }

  @Test
  public void testListKeysUsesPrefixCheckForStsS3Request() throws Exception {
    setupStsS3Request();

    final IAccessAuthorizer accessAuthorizer = createMockIAccessAuthorizerReturningTrue();
    final KeyManager keyManager = createListKeysKeyManagerReturningEmpty();

    final OmMetadataReader omMetadataReader = createMetadataReader(accessAuthorizer, keyManager);

    // Case 1: List with prefix "userA/"
    omMetadataReader.listKeys(VOLUME_NAME, BUCKET_NAME, "", "userA/", (int) MAX_KEYS);

    List<AclCheck> checks = captureAclChecks(accessAuthorizer, 4);
    assertContainsBucketListCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "userA/");

    // Reset to make case 2 assertions independent of case 1 captures.
    reset(accessAuthorizer);
    reenableAllowAllAccessChecks(accessAuthorizer);

    // Case 2: List with empty prefix (should check "*")
    omMetadataReader.listKeys(VOLUME_NAME, BUCKET_NAME, "", "", (int) MAX_KEYS);

    checks = captureAclChecks(accessAuthorizer, 4);
    assertContainsBucketListCheck(checks);
    assertContainsKeyReadCheckWithName(checks, "*");
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer) throws IOException {
    return createMetadataReader(accessAuthorizer, mock(KeyManager.class));
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer, KeyManager keyManager)
      throws IOException {
    return createMetadataReader(accessAuthorizer, keyManager, true);
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer, KeyManager keyManager,
      boolean isS3StsEnabled) throws IOException {
    return createMetadataReader(accessAuthorizer, keyManager, isS3StsEnabled, mock(OzoneManager.class));
  }

  private OmMetadataReader createMetadataReader(IAccessAuthorizer accessAuthorizer, KeyManager keyManager,
      boolean isS3StsEnabled, OzoneManager ozoneManager) throws IOException {
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getConfiguration()).thenReturn(new OzoneConfiguration());
    when(ozoneManager.getAclsEnabled()).thenReturn(true);
    when(ozoneManager.isS3STSEnabled()).thenReturn(isS3StsEnabled);
    final OMPerformanceMetrics perfMetrics = mock(OMPerformanceMetrics.class);
    // OmMetadataReader uses these MutableRate metrics via MetricUtil.captureLatencyNs(...).
    when(perfMetrics.getListKeysResolveBucketLatencyNs()).thenReturn(mock(ConcurrentMutableRate.class));
    when(perfMetrics.getListKeysAclCheckLatencyNs()).thenReturn(mock(ConcurrentMutableRate.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);
    when(ozoneManager.getVolumeOwner(any(), any(), any())).thenReturn("volume-owner");
    when(ozoneManager.getBucketOwner(any(), any(), any(), any())).thenReturn("bucket-owner");
    when(ozoneManager.getOmRpcServerAddr()).thenReturn(new InetSocketAddress("127.0.0.1", 9874));
    when(ozoneManager.resolveBucketLink(any(Pair.class)))
        .thenReturn(
            new ResolvedBucket(
                VOLUME_NAME, BUCKET_NAME, VOLUME_NAME, BUCKET_NAME, "bucket-owner", FILE_SYSTEM_OPTIMIZED));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(
            new ResolvedBucket(
                VOLUME_NAME, BUCKET_NAME, VOLUME_NAME, BUCKET_NAME, "bucket-owner", FILE_SYSTEM_OPTIMIZED));

    return new OmMetadataReader(
        keyManager, mock(PrefixManager.class), ozoneManager, mock(Logger.class), mock(AuditLogger.class),
        mock(OmMetadataReaderMetrics.class), accessAuthorizer);
  }

  /**
   * Creates and sets a mock STSTokenIdentifier with a session policy in the thread-local.
   */
  private void setupStsTokenIdentifier() {
    final STSTokenIdentifier stsTokenIdentifier = mock(STSTokenIdentifier.class);
    when(stsTokenIdentifier.getSessionPolicy()).thenReturn("session-policy-from-thread-local");
    OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
  }

  /**
   * Creates a mock IAccessAuthorizer that returns the specified result for checkAccess.
   * @return the mocked IAccessAuthorizer
   */
  private IAccessAuthorizer createMockIAccessAuthorizerReturningTrue() throws OMException {
    final IAccessAuthorizer accessAuthorizer = mock(IAccessAuthorizer.class);
    when(accessAuthorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class)))
        .thenReturn(true);
    return accessAuthorizer;
  }

  /**
   * Creates a test RequestContext.Builder.
   *
   * @return the constructed RequestContext.Builder
   */
  private RequestContext.Builder createTestRequestContextBuilder() {
    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser("testUser"))
        .setIp(InetAddress.getLoopbackAddress())
        .setHost("localhost")
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(READ)
        .setOwnerName("owner");
  }

  /**
   * Creates a test OzoneObj representing a key.
   * @return the constructed OzoneObj
   */
  private OzoneObj createTestOzoneObj() {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName("key")
        .build();
  }

  private void setupStsS3Request() {
    OzoneManager.setStsTokenIdentifier(mock(STSTokenIdentifier.class));
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId(TestOMMetadataReader.ACCESS_KEY_ID).build());
  }

  private void setupNonStsS3Request() {
    OzoneManager.setStsTokenIdentifier(null);
    OzoneManager.setS3Auth(null);
  }

  private OmKeyArgs createOmKeyArgs() {
    return new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(TestOMMetadataReader.KEY_PREFIX)
        .build();
  }

  private KeyManager createListStatusKeyManagerReturningEmpty() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.listStatus(any(OmKeyArgs.class), eq(false), eq(""), eq(MAX_KEYS), any(), eq(false)))
        .thenReturn(Collections.emptyList());
    return keyManager;
  }

  private KeyManager createGetFileStatusKeyManagerReturningStatus() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getFileStatus(any(OmKeyArgs.class), any()))
        .thenReturn(mock(OzoneFileStatus.class));
    return keyManager;
  }

  private KeyManager createListKeysKeyManagerReturningEmpty() throws IOException {
    final KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.listKeys(any(), any(), any(), any(), eq(100)))
        .thenReturn(new ListKeysResult(Collections.emptyList(), false));
    return keyManager;
  }

  private void reenableAllowAllAccessChecks(IAccessAuthorizer accessAuthorizer) throws OMException {
    when(accessAuthorizer.checkAccess(any(OzoneObj.class), any(RequestContext.class)))
        .thenReturn(true);
  }

  /**
   * Verifies that the accessAuthorizer received a call to checkAccess with the expected session policy.
   * @param accessAuthorizer the mock authorizer to verify
   * @param expectedObj the expected OzoneObj
   * @param expectedSessionPolicy the expected session policy (could be null)
   */
  private void verifySessionPolicyPassedToAuthorizer(IAccessAuthorizer accessAuthorizer, OzoneObj expectedObj,
      String expectedSessionPolicy) throws OMException {
    final ArgumentCaptor<RequestContext> captor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer).checkAccess(eq(expectedObj), captor.capture());
    assertEquals(expectedSessionPolicy, captor.getValue().getSessionPolicy());
  }

  /**
   * Verifies that the accessAuthorizer received a call to checkAccess with the expected s3 action.
   * @param accessAuthorizer the mock authorizer to verify
   * @param expectedObj the expected OzoneObj
   * @param expectedS3Action the expected s3 action (could be null)
   */
  private void verifyS3ActionPassedToAuthorizer(IAccessAuthorizer accessAuthorizer, OzoneObj expectedObj,
      String expectedS3Action) throws OMException {
    final ArgumentCaptor<RequestContext> captor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer).checkAccess(eq(expectedObj), captor.capture());
    assertEquals(expectedS3Action, captor.getValue().getS3Action());
  }

  private void verifySessionPolicyAndS3ActionPassedToAuthorizer(IAccessAuthorizer accessAuthorizer,
      OzoneObj expectedObj) throws OMException {
    final ArgumentCaptor<RequestContext> captor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer).checkAccess(eq(expectedObj), captor.capture());
    assertEquals("session-policy-from-thread-local", captor.getValue().getSessionPolicy());
    assertEquals("PutObject", captor.getValue().getS3Action());
  }

  private List<AclCheck> captureAclChecks(IAccessAuthorizer accessAuthorizer, int expectedCheckCount)
      throws OMException {
    final ArgumentCaptor<OzoneObj> objCaptor = ArgumentCaptor.forClass(OzoneObj.class);
    final ArgumentCaptor<RequestContext> ctxCaptor = ArgumentCaptor.forClass(RequestContext.class);
    verify(accessAuthorizer, times(expectedCheckCount)).checkAccess(objCaptor.capture(), ctxCaptor.capture());
    return toAclChecks(objCaptor.getAllValues(), ctxCaptor.getAllValues());
  }

  private List<AclCheck> toAclChecks(List<OzoneObj> objs, List<RequestContext> contexts) {
    assertEquals(objs.size(), contexts.size(), "Captured ACL objects and contexts should align");
    final List<AclCheck> checks = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      checks.add(new AclCheck(objs.get(i), contexts.get(i)));
    }
    return checks;
  }

  private void assertContainsVolumeReadCheck(List<AclCheck> checks) {
    assertTrue(checks.stream().anyMatch(this::isVolumeReadCheck), "Expected a VOLUME READ ACL check");
  }

  private boolean isVolumeReadCheck(AclCheck check) {
    return check.getObj().getResourceType() == VOLUME && check.getContext().getAclRights() == READ;
  }

  private void assertContainsBucketListCheck(List<AclCheck> checks) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == OzoneObj.ResourceType.BUCKET &&
                check.getContext().getAclRights() == LIST),
        "Expected a BUCKET LIST ACL check");
  }

  private void assertContainsKeyReadCheckWithName(List<AclCheck> checks, String keyName) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == READ &&
                keyName.equals(check.getObj().getKeyName())),
        "Expected a KEY READ ACL check for key '" + keyName + "'");
  }

  private void assertContainsKeyReadCheckWithName(List<AclCheck> checks) {
    assertTrue(
        checks.stream().anyMatch(
            check -> check.getObj().getResourceType() == KEY && check.getContext().getAclRights() == READ &&
                TestOMMetadataReader.KEY_PREFIX.equals(check.getObj().getKeyName())),
        "Expected a KEY READ ACL check for key '" + TestOMMetadataReader.KEY_PREFIX + "'");
  }

  private static final class AclCheck {
    private final OzoneObj obj;
    private final RequestContext context;

    private AclCheck(OzoneObj obj, RequestContext context) {
      this.obj = obj;
      this.context = context;
    }

    private OzoneObj getObj() {
      return obj;
    }

    private RequestContext getContext() {
      return context;
    }
  }

  @Test
  public void getFileStatusRejectsObjectStoreLayout() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    KeyManager keyManager = mock(KeyManager.class);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(mock(OMPerformanceMetrics.class));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(new ResolvedBucket("vol", "obs-bucket", "vol", "obs-bucket",
            "owner", BucketLayout.OBJECT_STORE));

    OmMetadataReader reader = new OmMetadataReader(keyManager,
        mock(PrefixManager.class), ozoneManager, mock(org.slf4j.Logger.class),
        mock(AuditLogger.class), mock(OmMetadataReaderMetrics.class), null);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("vol")
        .setBucketName("obs-bucket")
        .setKeyName("key1")
        .build();

    OMException exception = assertThrows(OMException.class,
        () -> reader.getFileStatus(keyArgs));
    assertEquals(ResultCodes.NOT_SUPPORTED_OPERATION, exception.getResult());
    assertTrue(exception.getMessage().contains("obs-bucket"));
    assertTrue(exception.getMessage().contains("OBJECT_STORE"));
    verify(keyManager, never()).getFileStatus(any(), anyString());
  }

  @Test
  public void getFileStatusAllowsLegacyLayout() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    KeyManager keyManager = mock(KeyManager.class);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.getBucketManager()).thenReturn(mock(BucketManager.class));
    when(ozoneManager.getVolumeManager()).thenReturn(mock(VolumeManager.class));
    when(ozoneManager.getPerfMetrics()).thenReturn(mock(OMPerformanceMetrics.class));
    when(ozoneManager.resolveBucketLink(any(OmKeyArgs.class)))
        .thenReturn(new ResolvedBucket("vol", "legacy-bucket", "vol",
            "legacy-bucket", "owner", BucketLayout.LEGACY));
    OzoneFileStatus expectedStatus = new OzoneFileStatus();
    when(keyManager.getFileStatus(any(OmKeyArgs.class), anyString()))
        .thenReturn(expectedStatus);

    OmMetadataReader reader = new OmMetadataReader(keyManager,
        mock(PrefixManager.class), ozoneManager, mock(org.slf4j.Logger.class),
        mock(AuditLogger.class), mock(OmMetadataReaderMetrics.class), null);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("vol")
        .setBucketName("legacy-bucket")
        .setKeyName("key1")
        .build();

    OzoneFileStatus status = reader.getFileStatus(keyArgs);
    assertEquals(expectedStatus, status);
    verify(keyManager).getFileStatus(any(OmKeyArgs.class), anyString());
  }
}
