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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation.ACQUIRE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation.RELEASE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation.RENEW;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.junit.jupiter.api.Test;

class TestNfsExportLeaseManager {
  private static final ByteString EXPORT_UUID = ByteString.copyFrom(new byte[16]);

  @Test
  void commitRequestUsesGatewayFenceNotExportLeaseGeneration() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    ByteString ownerToken = ByteString.copyFromUtf8("recovered-owner");
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RECOVER), eq("gateway-a"), eq(2L), eq(101L),
        eq(""), eq(0L), eq(ByteString.EMPTY), eq(1500L), eq("alice")))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(4).setTransferID("transfer-a")
            .setOwnerToken(ownerToken).setOwnerPrincipal("alice")
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    when(reader.lookupTransferRanges(eq("gateway-a"), eq(2L), eq(101L), eq("transfer-a"), eq(4L),
        eq(ownerToken), eq("alice"), eq("")))
        .thenReturn(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
            .LookupNfsTransferRangesResponse.newBuilder().setEof(true).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      NfsFileLeaseResponse recovered = manager.recoverFileLease(EXPORT_UUID, 101, "alice");
      org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest request =
          manager.fileCommitRequest(EXPORT_UUID, recovered);

      assertEquals(2, request.getGatewayGeneration());
      assertEquals(4, request.getFileGeneration());
      verify(reader, org.mockito.Mockito.times(1)).updateFileLease(NfsFileLeaseRequest.Operation.RECOVER,
          "gateway-a", 2, 101, "", 0, ByteString.EMPTY, 1500, "alice");
    } finally {
      manager.close();
    }
  }

  @Test
  void acquiresAndReleasesExportLease() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(30_000L), any(), any()))
        .thenReturn(leaseResponseExpiringIn(30_000));
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponseExpiringIn(30_000));
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 30_000);

    manager.start();
    assertTrue(manager.isActive(EXPORT_UUID));
    manager.close();

    assertFalse(manager.isActive(EXPORT_UUID));
    verify(reader).updateExportLease(ACQUIRE, "gateway-a", 30_000, null, null);
    verify(reader).updateExportLease(RELEASE, "gateway-a", 0, 2L, 3L);
  }

  @Test
  void exportLeaseUsesOmExpiryInsteadOfConfiguredDuration() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(30_000L), any(), any()))
        .thenReturn(leaseResponseExpiringIn(500));
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 30_000);

    manager.start();
    try {
      Thread.sleep(600);
      assertFalse(manager.isActive(EXPORT_UUID));
    } finally {
      manager.close();
    }
  }

  @Test
  void startupReleasesExportLeaseWhenOmReportsItAlreadyExpired() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponseExpiringIn(-1));
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);

    assertThrows(IOException.class, manager::start);

    verify(reader).updateExportLease(RELEASE, "gateway-a", 0, 2L, 3L);
  }

  @Test
  void waitsForPreviousExportLeaseToExpireBeforeAcquiring() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(30_000L), any(), any()))
        .thenThrow(new OMException("NFS export lease is active", OMException.ResultCodes.NFS_LEASE_CONFLICT))
        .thenReturn(leaseResponseExpiringIn(30_000));
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponseExpiringIn(30_000));
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 30_000);

    manager.start();

    try {
      assertTrue(manager.isActive(EXPORT_UUID));
      verify(reader, times(2)).updateExportLease(ACQUIRE, "gateway-a", 30_000, null, null);
    } finally {
      manager.close();
    }
  }

  @Test
  void createsAndRegistersFileLeaseUnderTheActiveExportLease() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.createFile(any(), any(), any(), eq("gateway-a"), eq(2L), eq("transfer-create"), eq(1500L)))
        .thenReturn(fileLeaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-create"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    RequestContext context = RequestContext.newBuilder().setVersion(1).setExportUuid(EXPORT_UUID)
        .setCaller(Caller.newBuilder().setKerberosPrincipal("alice@EXAMPLE.COM")
            .setProtection(Caller.Protection.PRIVACY)).build();
    when(reader.getCallerUserName(context)).thenReturn("alice");
    ByteString parentHandle = ByteString.copyFromUtf8("parent");

    try {
      NfsFileCreateResponse response = manager.createFile(EXPORT_UUID, context, parentHandle,
          "new-file", "transfer-create");
      assertEquals(101, response.getObjectID());
      assertTrue(manager.isFileLeaseActive(EXPORT_UUID, "transfer-create"));
      verify(reader).createFile(context, parentHandle, "new-file", "gateway-a", 2,
          "transfer-create", 1500);
    } finally {
      manager.close();
    }
  }

  @Test
  void recoversAnExpiredFileLeaseOnlyForItsRecordedPrincipal() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    ByteString ownerToken = ByteString.copyFrom(new byte[16]);
    byte[] originalData = new byte[] {1, 2, 3};
    byte[] replacementData = new byte[] {9};
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse ranges =
        org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse.newBuilder()
            .addRanges(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry
                .newBuilder().setLeaseGeneration(1).setOffset(0).setLength(originalData.length)
                .setTransferID("transfer-a").setStagingKey("staging/volume/bucket/original")
                .setSha256(ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(originalData))).setTransactionIndex(10))
            .addRanges(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry
                .newBuilder().setLeaseGeneration(2).setOffset(1).setLength(replacementData.length)
                .setTransferID("transfer-a").setStagingKey("staging/volume/bucket/replacement")
                .setSha256(ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(replacementData))).setTransactionIndex(20))
            .setEof(true).build();
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RECOVER), eq("gateway-a"), eq(2L), eq(101L),
        eq(""), eq(0L), eq(ByteString.EMPTY), eq(1500L), eq("alice@EXAMPLE.COM")))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(2).setTransferID("transfer-a")
            .setOwnerToken(ownerToken).setOwnerPrincipal("alice@EXAMPLE.COM")
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    when(reader.lookupTransferRanges(eq("gateway-a"), eq(2L), eq(101L), eq("transfer-a"), eq(2L),
        eq(ownerToken), eq("alice@EXAMPLE.COM"), eq(""))).thenReturn(ranges);
    when(reader.readStagedRange("staging/volume/bucket/original")).thenReturn(originalData);
    when(reader.readStagedRange("staging/volume/bucket/replacement")).thenReturn(replacementData);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      NfsFileLeaseResponse recovered = manager.recoverFileLease(EXPORT_UUID, 101, "alice@EXAMPLE.COM");

      assertEquals(2, recovered.getGeneration());
      assertTrue(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
      assertArrayEquals(new byte[] {1, 9, 3}, manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 3)
          .getData().toByteArray());
      assertNull(manager.readStaged(EXPORT_UUID, 101, "bob@EXAMPLE.COM", 0, 3));
      OMException conflict = assertThrows(OMException.class,
          () -> manager.recoverFileLease(EXPORT_UUID, 101, "bob@EXAMPLE.COM"));
      assertEquals(OMException.ResultCodes.NFS_LEASE_CONFLICT, conflict.getResult());
      verify(reader).updateFileLease(NfsFileLeaseRequest.Operation.RECOVER, "gateway-a", 2, 101,
          "", 0, ByteString.EMPTY, 1500, "alice@EXAMPLE.COM");
    } finally {
      manager.close();
    }
  }

  @Test
  void publishedRecoveryUsesCanonicalContentInsteadOfRemainingStagedRanges() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    ByteString ownerToken = ByteString.copyFromUtf8("recovered-owner");
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RECOVER), eq("gateway-a"), eq(2L), eq(101L),
        eq(""), eq(0L), eq(ByteString.EMPTY), eq(1500L), eq("alice@EXAMPLE.COM")))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(2).setTransferID("transfer-a")
            .setOwnerToken(ownerToken).setOwnerPrincipal("alice@EXAMPLE.COM")
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).setContentPublished(true).build());
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse ranges =
        org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse.newBuilder()
            .addRanges(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry
                .newBuilder().setLeaseGeneration(2).setOffset(0).setLength(3).setTransferID("transfer-a")
                .setStagingKey("staging/remaining-range").setSha256(ByteString.copyFrom(new byte[32]))
                .setTransactionIndex(10))
            .setEof(true).build();
    when(reader.lookupTransferRanges(eq("gateway-a"), eq(2L), eq(101L), eq("transfer-a"), eq(2L),
        eq(ownerToken), eq("alice@EXAMPLE.COM"), eq(""))).thenReturn(ranges);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      NfsFileLeaseResponse recovered = manager.recoverFileLease(EXPORT_UUID, 101, "alice@EXAMPLE.COM");

      assertTrue(recovered.getContentPublished());
      assertTrue(manager.recoverFileLease(EXPORT_UUID, 101, "alice@EXAMPLE.COM").getContentPublished());
      assertNull(manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 3));
      verify(reader, never()).readStagedRange("staging/remaining-range");
    } finally {
      manager.close();
    }
  }

  @Test
  void recoveryCleansFencedPendingIntentsAndResumesCleanupButKeepsRecordedRanges() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    ByteString ownerToken = ByteString.copyFromUtf8("recovered-owner");
    ByteString digest = ByteString.copyFrom(new byte[32]);
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry pending =
        org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry
        .newBuilder().setLeaseGeneration(1).setOffset(8).setLength(4).setTransferID("transfer-a")
        .setStagingKey("staging/pending").setSha256(digest).setTransactionIndex(10)
        .setState(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry.State
            .PENDING_STAGE).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry recorded =
        pending.toBuilder().setOffset(0).setLength(1).setStagingKey("staging/recorded").setTransactionIndex(9)
        .setSha256(ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
            .digest(new byte[] {7})))
        .setState(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry.State
            .RECORDED).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry cleanupPending =
        pending.toBuilder().setOffset(12).setLength(2).setStagingKey("staging/already-deleted")
            .setState(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry.State
                .CLEANUP_PENDING).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse ranges =
        org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse
        .newBuilder().addRanges(pending).addRanges(recorded).addRanges(cleanupPending).setEof(true).build();
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RECOVER), eq("gateway-a"), eq(2L), eq(101L),
        eq(""), eq(0L), eq(ByteString.EMPTY), eq(1500L), eq("alice@EXAMPLE.COM")))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(2).setTransferID("transfer-a")
            .setOwnerToken(ownerToken).setOwnerPrincipal("alice@EXAMPLE.COM")
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    when(reader.lookupTransferRanges(eq("gateway-a"), eq(2L), eq(101L), eq("transfer-a"), eq(2L),
        eq(ownerToken), eq("alice@EXAMPLE.COM"), eq(""))).thenReturn(ranges);
    when(reader.readStagedRange("staging/recorded")).thenReturn(new byte[] {7});
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      manager.recoverFileLease(EXPORT_UUID, 101, "alice@EXAMPLE.COM");

      verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.MARK_CLEANUP, "gateway-a", 2, 101,
          "transfer-a", 2, ownerToken, 8, 4, "staging/pending", digest);
      verify(reader).deleteStagedRange("staging/pending");
      verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.DELETE, "gateway-a", 2, 101,
          "transfer-a", 2, ownerToken, 8, 4, "staging/pending", digest);
      verify(reader, never()).updateTransferRange(NfsTransferRangeRequest.Operation.MARK_CLEANUP, "gateway-a", 2,
          101, "transfer-a", 2, ownerToken, 12, 2, "staging/already-deleted", digest);
      verify(reader).deleteStagedRange("staging/already-deleted");
      verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.DELETE, "gateway-a", 2, 101,
          "transfer-a", 2, ownerToken, 12, 2, "staging/already-deleted", digest);
      assertNotNull(manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 1));
      verify(reader, never()).deleteStagedRange("staging/recorded");
    } finally {
      manager.close();
    }
  }

  @Test
  void retriesExportLeaseRenewalAfterTransientFailure() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    CountDownLatch retry = new CountDownLatch(1);
    AtomicInteger renewals = new AtomicInteger();
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> {
          if (renewals.incrementAndGet() == 1) {
            throw new IOException("OM unavailable");
          }
          retry.countDown();
          return leaseResponse();
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      assertTrue(retry.await(5, TimeUnit.SECONDS));
      assertTrue(manager.isActive(EXPORT_UUID));
      assertEquals(2, renewals.get());
    } finally {
      manager.close();
    }
  }

  @Test
  void stopsServingWhenLeaseExpiresDuringRenewalOutage() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    CountDownLatch renewalStarted = new CountDownLatch(1);
    AtomicInteger renewals = new AtomicInteger();
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> {
          renewals.incrementAndGet();
          renewalStarted.countDown();
          throw new IOException("OM unavailable");
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      assertTrue(renewalStarted.await(5, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (manager.isActive(EXPORT_UUID) && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(manager.isActive(EXPORT_UUID));
      assertTrue(renewals.get() >= 1);
    } finally {
      manager.close();
    }
  }

  @Test
  void stopsServingAfterRenewalConflict() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    CountDownLatch renewalFailed = new CountDownLatch(1);
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> {
          renewalFailed.countDown();
          throw new OMException("NFS export lease owner is stale", OMException.ResultCodes.NFS_LEASE_CONFLICT);
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      assertTrue(renewalFailed.await(5, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (manager.isActive(EXPORT_UUID) && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(manager.isActive(EXPORT_UUID));
      verify(reader, times(1)).updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  void renewsAndReleasesRegisteredFileLease() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RELEASE), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(0L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(2).setExpiresAtMillis(
            System.currentTimeMillis()).build());
    ByteString digest = ByteString.copyFrom(new byte[32]);
    when(reader.updateTransferRange(eq(NfsTransferRangeRequest.Operation.UPSERT), eq("gateway-a"), eq(2L),
        eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(0L), eq(8L),
        eq("staging/range-0"), eq(digest)))
        .thenReturn(NfsTransferRangeResponse.newBuilder().setRecorded(true).setTransactionIndex(1).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
      NfsTransferRangeResponse range = manager.updateTransferRange(EXPORT_UUID, "transfer-a",
          NfsTransferRangeRequest.Operation.UPSERT, 0, 8, "staging/range-0", digest);
      assertTrue(range.getRecorded());
      verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.UPSERT, "gateway-a", 2, 101,
          "transfer-a", 1, ByteString.copyFromUtf8("owner"), 0, 8, "staging/range-0", digest);
      verify(reader, timeout(5000).times(2)).updateFileLease(NfsFileLeaseRequest.Operation.RENEW, "gateway-a", 2,
          101, "transfer-a", 1, ByteString.copyFromUtf8("owner"), 1500);
      manager.releaseFileLease(EXPORT_UUID, "transfer-a");
      assertFalse(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
      org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> manager.updateTransferRange(
          EXPORT_UUID, "transfer-a", NfsTransferRangeRequest.Operation.DELETE, 0, 8,
          "staging/range-0", digest));
      verify(reader).updateFileLease(NfsFileLeaseRequest.Operation.RELEASE, "gateway-a", 2, 101, "transfer-a", 1,
          ByteString.copyFromUtf8("owner"), 0);
    } finally {
      manager.close();
    }
  }

  @Test
  void fileLeaseRenewalUsesOmExpiryInsteadOfConfiguredDuration() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    CountDownLatch fileLeaseRenewed = new CountDownLatch(1);
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build())
        .thenAnswer(invocation -> {
          fileLeaseRenewed.countDown();
          return NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
              System.currentTimeMillis() + 100).build();
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(fileLeaseRenewed.await(5, TimeUnit.SECONDS));
      Thread.sleep(150);
      assertFalse(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
    } finally {
      manager.close();
    }
  }

  @Test
  void renewsExportBeforeRegisteringFileLeaseNearExportExpiry() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    CountDownLatch scheduledRenewalStarted = new CountDownLatch(1);
    CountDownLatch finishScheduledRenewal = new CountDownLatch(1);
    AtomicInteger renewalCount = new AtomicInteger();
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> {
          if (renewalCount.incrementAndGet() == 1) {
            scheduledRenewalStarted.countDown();
            assertTrue(finishScheduledRenewal.await(5, TimeUnit.SECONDS));
          }
          return leaseResponse();
        });
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 15_000).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      assertTrue(scheduledRenewalStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(1500);
      manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());
      assertTrue(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
      verify(reader, times(2)).updateExportLease(RENEW, "gateway-a", 3000L, 2L, 3L);
    } finally {
      finishScheduledRenewal.countDown();
      manager.close();
    }
  }

  @Test
  void renewsExportBeforeRenewingFileLeaseNearExportExpiry() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    CountDownLatch scheduledRenewalStarted = new CountDownLatch(1);
    CountDownLatch finishScheduledRenewal = new CountDownLatch(1);
    CountDownLatch fileRenewalExportRenewed = new CountDownLatch(1);
    AtomicInteger renewalCount = new AtomicInteger();
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> {
          if (renewalCount.incrementAndGet() == 1) {
            scheduledRenewalStarted.countDown();
            assertTrue(finishScheduledRenewal.await(5, TimeUnit.SECONDS));
          } else {
            fileRenewalExportRenewed.countDown();
          }
          return leaseResponse();
        });
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 15_000).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(scheduledRenewalStarted.await(5, TimeUnit.SECONDS));
      assertTrue(fileRenewalExportRenewed.await(5, TimeUnit.SECONDS));
      verify(reader, timeout(5000).atLeast(2)).updateFileLease(NfsFileLeaseRequest.Operation.RENEW, "gateway-a", 2,
          101, "transfer-a", 1, ByteString.copyFromUtf8("owner"), 1500);
    } finally {
      finishScheduledRenewal.countDown();
      manager.close();
    }
  }

  @Test
  void delayedLeaseRenewalDoesNotClearPublishedContent() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    CountDownLatch renewalStarted = new CountDownLatch(1);
    CountDownLatch completeRenewal = new CountDownLatch(1);
    NfsFileLeaseResponse unpublished = NfsFileLeaseResponse.newBuilder().setGeneration(1)
        .setExpiresAtMillis(System.currentTimeMillis() + 1500).setContentPublished(false).build();
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(unpublished)
        .thenAnswer(invocation -> {
          renewalStarted.countDown();
          assertTrue(completeRenewal.await(5, TimeUnit.SECONDS));
          return unpublished;
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(renewalStarted.await(5, TimeUnit.SECONDS));
      manager.markContentPublished(EXPORT_UUID, "transfer-a");
      completeRenewal.countDown();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      NfsFileLeaseResponse renewed;
      do {
        renewed = manager.recoverFileLease(EXPORT_UUID, 101, "alice@EXAMPLE.COM");
        if (renewed.getContentPublished()) {
          break;
        }
        Thread.sleep(10);
      } while (System.nanoTime() < deadline);

      assertTrue(renewed.getContentPublished());
    } finally {
      completeRenewal.countDown();
      manager.close();
    }
  }

  @Test
  void delayedFileLeaseRenewalDoesNotExpireExportLease() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenAnswer(invocation -> leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    CountDownLatch renewalStarted = new CountDownLatch(1);
    CountDownLatch completeRenewal = new CountDownLatch(1);
    NfsFileLeaseResponse fileLease = NfsFileLeaseResponse.newBuilder().setGeneration(1)
        .setExpiresAtMillis(System.currentTimeMillis() + 1500).build();
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(fileLease)
        .thenAnswer(invocation -> {
          renewalStarted.countDown();
          assertTrue(completeRenewal.await(6, TimeUnit.SECONDS));
          return fileLease;
        });
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(renewalStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(3500);
      assertTrue(manager.isActive(EXPORT_UUID));
    } finally {
      completeRenewal.countDown();
      manager.close();
    }
  }

  @Test
  void storesRangeBeforeRecordingItsManifest() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] data = new byte[]{1, 2, 3};
    ByteString digest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(data));
    NfsStagingStore.StagedRange staged = new NfsStagingStore.StagedRange("nfs-system/staging/range", data.length,
        digest);
    stubStageRange(reader, "transfer-a", 0, data, staged, digest, 1);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertThrows(IOException.class,
          () -> manager.stageRange(EXPORT_UUID, "transfer-a", 101, "bob@EXAMPLE.COM", 0, data));
      assertThrows(IOException.class,
          () -> manager.stageRange(EXPORT_UUID, "transfer-a", 102, "alice@EXAMPLE.COM", 0, data));
      verify(reader, never()).stageRangeObject("transfer-a", 0, data);
      assertTrue(manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, data).getRecorded());
      org.mockito.InOrder order = inOrder(reader);
      order.verify(reader).stagingObjectKey("transfer-a", 0, data);
      order.verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.PREPARE, "gateway-a", 2, 101,
          "transfer-a", 1, ByteString.copyFromUtf8("owner"), 0, 3, staged.getQualifiedKey(), digest);
      order.verify(reader).stageRangeObject("transfer-a", 0, data);
      order.verify(reader).updateTransferRange(NfsTransferRangeRequest.Operation.PROMOTE, "gateway-a", 2, 101,
          "transfer-a", 1, ByteString.copyFromUtf8("owner"), 0, 3, staged.getQualifiedKey(), digest);
    } finally {
      manager.close();
    }
  }

  @Test
  void retriesAStageAfterFailuresBeforeCreationAndBeforePromotion() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] data = new byte[]{1, 2, 3};
    ByteString digest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(data));
    NfsStagingStore.StagedRange staged = new NfsStagingStore.StagedRange("nfs-system/staging/range", 0,
        data.length, digest);
    stubStageRange(reader, "transfer-a", 0, data, staged, digest, 3);
    org.mockito.Mockito.doThrow(new IOException("stage create failed")).doReturn(staged).doReturn(staged)
        .when(reader).stageRangeObject("transfer-a", 0, data);
    NfsTransferRangeResponse promoted = NfsTransferRangeResponse.newBuilder().setRecorded(true)
        .setTransactionIndex(3).build();
    when(reader.updateTransferRange(eq(NfsTransferRangeRequest.Operation.PROMOTE), eq("gateway-a"), eq(2L),
        eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(0L), eq(3L),
        eq(staged.getQualifiedKey()), eq(digest)))
        .thenThrow(new IOException("promotion response lost")).thenReturn(promoted);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertThrows(IOException.class, () -> manager.stageRange(EXPORT_UUID, "transfer-a", 101,
          "alice@EXAMPLE.COM", 0, data));
      assertThrows(IOException.class, () -> manager.stageRange(EXPORT_UUID, "transfer-a", 101,
          "alice@EXAMPLE.COM", 0, data));
      assertTrue(manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, data).getRecorded());
      verify(reader, times(3)).updateTransferRange(eq(NfsTransferRangeRequest.Operation.PREPARE), eq("gateway-a"),
          eq(2L), eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(0L), eq(3L),
          eq(staged.getQualifiedKey()), eq(digest));
      verify(reader, times(2)).updateTransferRange(eq(NfsTransferRangeRequest.Operation.PROMOTE), eq("gateway-a"),
          eq(2L), eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(0L), eq(3L),
          eq(staged.getQualifiedKey()), eq(digest));
      verify(reader, times(3)).stageRangeObject("transfer-a", 0, data);
    } finally {
      manager.close();
    }
  }

  @Test
  void enforcesConfiguredRangeLimitAndAllowsExactRetry() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build());
    byte[] firstData = new byte[]{1, 2, 3};
    ByteString firstDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
        .digest(firstData));
    NfsStagingStore.StagedRange first = new NfsStagingStore.StagedRange("nfs-system/staging/first", 0,
        firstData.length, firstDigest);
    stubStageRange(reader, "transfer-a", 0, firstData, first, firstDigest, 1);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000, 1);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      assertTrue(manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, firstData)
          .getRecorded());
      assertTrue(manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, firstData)
          .getRecorded());

      byte[] secondData = new byte[]{4};
      assertThrows(NfsExportLeaseManager.RangeLimitException.class,
          () -> manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 3, secondData));
      verify(reader, times(2)).stageRangeObject("transfer-a", 0, firstData);
      verify(reader, never()).stageRangeObject("transfer-a", 3, secondData);
    } finally {
      manager.close();
    }
  }

  @Test
  void readsStagedRangesForAnOpenFileAndZeroFillsHoles() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] data = new byte[]{1, 2, 3};
    ByteString digest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(data));
    NfsStagingStore.StagedRange staged = new NfsStagingStore.StagedRange("nfs-system/staging/range", 2,
        data.length, digest);
    stubStageRange(reader, "transfer-a", 2, data, staged, digest, 1);
    when(reader.readStagedRange(staged.getQualifiedKey())).thenReturn(data);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 2, data);
      NfsExportLeaseManager.StagedRead read = manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 5);
      assertArrayEquals(new byte[]{0, 0, 1, 2, 3}, read.getData().toByteArray());
      assertTrue(read.isEof());
      NfsExportLeaseManager.StagedRead eof = manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 5, 8);
      assertEquals(0, eof.getData().size());
      assertTrue(eof.isEof());
    } finally {
      manager.close();
    }
  }

  @Test
  void refreshesStagedRangesFromOmBeforePublication() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] staleData = new byte[]{9, 9};
    byte[] firstData = new byte[]{1, 2};
    byte[] secondData = new byte[]{3, 4};
    ByteString staleDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
        .digest(staleData));
    ByteString firstDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
        .digest(firstData));
    ByteString secondDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
        .digest(secondData));
    NfsStagingStore.StagedRange staleRange = new NfsStagingStore.StagedRange("stage/stale", 0, staleData.length,
        staleDigest);
    NfsStagingStore.StagedRange firstRange = new NfsStagingStore.StagedRange("stage/first", 0, firstData.length,
        firstDigest);
    NfsStagingStore.StagedRange secondRange = new NfsStagingStore.StagedRange("stage/second", 2,
        secondData.length, secondDigest);
    stubStageRange(reader, "transfer-a", 0, staleData, staleRange, staleDigest, 1);
    stubStageRange(reader, "transfer-a", 2, secondData, secondRange, secondDigest, 3);
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse manifest =
        org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse.newBuilder()
            .addRanges(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeEntry
                .newBuilder().setOffset(0).setLength(2).setTransferID("transfer-a")
                .setStagingKey(firstRange.getQualifiedKey()).setSha256(firstDigest).setTransactionIndex(2)
                .setLeaseGeneration(1))
            .setEof(true).build();
    when(reader.lookupTransferRanges(eq("gateway-a"), eq(2L), eq(101L), eq("transfer-a"), eq(1L),
        eq(ByteString.copyFromUtf8("owner")), eq("alice@EXAMPLE.COM"), eq(""))).thenReturn(manifest);
    when(reader.readStagedRange(staleRange.getQualifiedKey())).thenReturn(staleData);
    when(reader.readStagedRange(firstRange.getQualifiedKey())).thenReturn(firstData);
    when(reader.readStagedRange(secondRange.getQualifiedKey())).thenReturn(secondData);
    when(reader.readStagedRange(secondRange.getQualifiedKey())).thenReturn(secondData);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, staleData);
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 2, secondData);
      manager.refreshStagedRanges(EXPORT_UUID, 101, "alice@EXAMPLE.COM");

      assertEquals(4, manager.stagedFileSize(EXPORT_UUID, 101, "alice@EXAMPLE.COM"));
      assertArrayEquals(new byte[]{1, 2, 3, 4},
          manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 4).getData().toByteArray());
    } finally {
      manager.close();
    }
  }

  @Test
  void retainsRangeManifestWhenStagedObjectDeletionFails() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] data = new byte[]{1, 2, 3};
    ByteString digest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(data));
    NfsStagingStore.StagedRange staged = new NfsStagingStore.StagedRange("nfs-system/staging/range", 4,
        data.length, digest);
    stubStageRange(reader, "transfer-a", 4, data, staged, digest, 1);
    when(reader.readStagedRange(staged.getQualifiedKey())).thenReturn(data);
    org.mockito.Mockito.doThrow(new IOException("Staging object delete failed")).doNothing()
        .when(reader).deleteStagedRange(staged.getQualifiedKey());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 4, data);
      assertThrows(IOException.class, () -> manager.removeStagedRanges(EXPORT_UUID, "transfer-a"));
      verify(reader, never()).updateTransferRange(eq(NfsTransferRangeRequest.Operation.DELETE), eq("gateway-a"),
          eq(2L), eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(4L), eq(3L),
          eq(staged.getQualifiedKey()), eq(digest));
      assertArrayEquals(new byte[]{0, 0, 0, 0, 1, 2, 3},
          manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 7).getData().toByteArray());

      manager.removeStagedRanges(EXPORT_UUID, "transfer-a");

      verify(reader).updateTransferRange(eq(NfsTransferRangeRequest.Operation.DELETE), eq("gateway-a"),
          eq(2L), eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(4L), eq(3L),
          eq(staged.getQualifiedKey()), eq(digest));
      verify(reader, times(2)).deleteStagedRange(staged.getQualifiedKey());
      org.mockito.InOrder order = inOrder(reader);
      order.verify(reader).deleteStagedRange(staged.getQualifiedKey());
      order.verify(reader).deleteStagedRange(staged.getQualifiedKey());
      order.verify(reader).updateTransferRange(eq(NfsTransferRangeRequest.Operation.DELETE), eq("gateway-a"),
          eq(2L), eq(101L), eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(4L), eq(3L),
          eq(staged.getQualifiedKey()), eq(digest));
      assertTrue(manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 1).isEof());
    } finally {
      manager.close();
    }
  }

  @Test
  void stopsServingTransferAfterFileLeaseRenewalFailure() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RENEW), eq("gateway-a"), eq(3000L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.updateExportLease(eq(RELEASE), eq("gateway-a"), eq(0L), eq(2L), eq(3L)))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build())
        .thenThrow(new IOException("OM unavailable"));
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (manager.isFileLeaseActive(EXPORT_UUID, "transfer-a") && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
    } finally {
      manager.close();
    }
  }

  @Test
  void rejectsDuplicateFileLeaseWithoutRenewingItAgain() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1).setExpiresAtMillis(
            System.currentTimeMillis() + 1500).build());
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();

    try {
      manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());
      assertThrows(IOException.class,
          () -> manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse()));
      assertTrue(manager.isFileLeaseActive(EXPORT_UUID, "transfer-a"));
      verify(reader, times(1)).updateFileLease(NfsFileLeaseRequest.Operation.RENEW, "gateway-a", 2, 101,
          "transfer-a", 1, ByteString.copyFromUtf8("owner"), 1500);
    } finally {
      manager.close();
    }
  }

  @Test
  void laterOverlappingWritesReplaceEarlierBytes() throws Exception {
    NfsObjectReader reader = org.mockito.Mockito.mock(NfsObjectReader.class);
    when(reader.updateExportLease(eq(ACQUIRE), eq("gateway-a"), eq(3000L), any(), any()))
        .thenReturn(leaseResponse());
    when(reader.updateFileLease(eq(NfsFileLeaseRequest.Operation.RENEW), eq("gateway-a"), eq(2L), eq(101L),
        eq("transfer-a"), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(1500L)))
        .thenReturn(NfsFileLeaseResponse.newBuilder().setGeneration(1)
            .setExpiresAtMillis(System.currentTimeMillis() + 1500).build());
    byte[] original = new byte[]{1, 2, 3, 4};
    byte[] replacement = new byte[]{8, 9};
    ByteString firstDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256").digest(original));
    ByteString secondDigest = ByteString.copyFrom(java.security.MessageDigest.getInstance("SHA-256")
        .digest(replacement));
    NfsStagingStore.StagedRange first = new NfsStagingStore.StagedRange("stage/first", 0, original.length,
        firstDigest);
    NfsStagingStore.StagedRange second = new NfsStagingStore.StagedRange("stage/second", 1, replacement.length,
        secondDigest);
    stubStageRange(reader, "transfer-a", 0, original, first, firstDigest, 1);
    stubStageRange(reader, "transfer-a", 1, replacement, second, secondDigest, 2);
    when(reader.readStagedRange(first.getQualifiedKey())).thenReturn(original);
    when(reader.readStagedRange(second.getQualifiedKey())).thenReturn(replacement);
    NfsExportLeaseManager manager = new NfsExportLeaseManager(Map.of(EXPORT_UUID, reader), "gateway-a", 3000);
    manager.start();
    manager.registerFileLease(EXPORT_UUID, "transfer-a", "alice@EXAMPLE.COM", fileLeaseResponse());

    try {
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 0, original);
      manager.stageRange(EXPORT_UUID, "transfer-a", 101, "alice@EXAMPLE.COM", 1, replacement);
      NfsExportLeaseManager.StagedRead read = manager.readStaged(EXPORT_UUID, 101, "alice@EXAMPLE.COM", 0, 4);
      assertArrayEquals(new byte[]{1, 8, 9, 4}, read.getData().toByteArray());
      assertTrue(read.isEof());
    } finally {
      manager.close();
    }
  }

  private static NfsFileCreateResponse fileLeaseResponse() {
    return NfsFileCreateResponse.newBuilder().setObjectID(101).setGatewayGeneration(2).setGeneration(1)
        .setExpiresAtMillis(System.currentTimeMillis() + 10_000).setOwnerToken(ByteString.copyFromUtf8("owner"))
        .build();
  }

  private static void stubStageRange(NfsObjectReader reader, String transferId, long offset, byte[] data,
      NfsStagingStore.StagedRange range, ByteString digest, long transactionIndex) throws Exception {
    when(reader.stagingObjectKey(eq(transferId), eq(offset), same(data))).thenReturn(range.getQualifiedKey());
    when(reader.stageRangeObject(transferId, offset, data)).thenReturn(range);
    NfsTransferRangeResponse response = NfsTransferRangeResponse.newBuilder().setRecorded(true)
        .setTransactionIndex(transactionIndex).build();
    when(reader.updateTransferRange(eq(NfsTransferRangeRequest.Operation.PREPARE), eq("gateway-a"), eq(2L),
        eq(101L), eq(transferId), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(offset),
        eq((long) data.length), eq(range.getQualifiedKey()), eq(digest))).thenReturn(response);
    when(reader.updateTransferRange(eq(NfsTransferRangeRequest.Operation.PROMOTE), eq("gateway-a"), eq(2L),
        eq(101L), eq(transferId), eq(1L), eq(ByteString.copyFromUtf8("owner")), eq(offset),
        eq((long) data.length), eq(range.getQualifiedKey()), eq(digest))).thenReturn(response);
  }

  private static NfsExportLeaseResponse leaseResponse() {
    return leaseResponseExpiringIn(3000);
  }

  private static NfsExportLeaseResponse leaseResponseExpiringIn(long millis) {
    return NfsExportLeaseResponse.newBuilder().setExportUUID(EXPORT_UUID.toStringUtf8()).setVolumeID(1)
        .setBucketID(2).setGatewayID("gateway-a").setGatewayGeneration(2).setGeneration(3)
        .setExpiresAtMillis(System.currentTimeMillis() + millis).build();
  }
}
