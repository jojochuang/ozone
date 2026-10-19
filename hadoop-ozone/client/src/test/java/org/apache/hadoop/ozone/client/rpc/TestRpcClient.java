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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.ozone.client.rpc.RpcClient.validateOmVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;

/**
 * Run RPC Client tests.
 */
public class TestRpcClient {
  private enum ValidateOmVersionTestCases {
    NULL_EXPECTED_NO_OM(
        null, // Expected version
        null, // First OM Version
        null, // Second OM Version
        true), // Should validation pass
    NULL_EXPECTED_ONE_OM(
        null,
        OzoneManagerVersion.CURRENT,
        null,
        true),
    NULL_EXPECTED_TWO_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    NULL_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        true
    ),
    NULL_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        null,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        true
    ),
    NULL_EXPECTED_TWO_FUTURE_OM(
        null,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true
    ),

    DEFAULT_EXPECTED_NO_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        null,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        null,
        true),
    DEFAULT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        null,
        true),
    DEFAULT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        true),
    DEFAULT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    DEFAULT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        true),
    DEFAULT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    DEFAULT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        true),

    CURRENT_EXPECTED_NO_OM(
        OzoneManagerVersion.CURRENT,
        null,
        null,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        null,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        null,
        true),
    CURRENT_EXPECTED_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        null,
        true),
    CURRENT_EXPECTED_TWO_DEFAULT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.DEFAULT_VERSION,
        false),
    CURRENT_EXPECTED_TWO_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        true),
    CURRENT_EXPECTED_TWO_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        true),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_CURRENT_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.CURRENT,
        false),
    CURRENT_EXPECTED_ONE_DEFAULT_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.DEFAULT_VERSION,
        OzoneManagerVersion.FUTURE_VERSION,
        false),
    CURRENT_EXPECTED_ONE_CURRENT_ONE_FUTURE_OM(
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.CURRENT,
        OzoneManagerVersion.FUTURE_VERSION,
        true);

    private final OzoneManagerVersion expectedVersion;
    private final OzoneManagerVersion om1Version;
    private final OzoneManagerVersion om2Version;
    private final boolean validation;

    ValidateOmVersionTestCases(
        OzoneManagerVersion expectedVersion,
        OzoneManagerVersion om1Version,
        OzoneManagerVersion om2Version,
        boolean validation) {
      this.expectedVersion = expectedVersion;
      this.om1Version = om1Version;
      this.om2Version = om2Version;
      this.validation = validation;
    }
  }

  @ParameterizedTest
  @EnumSource(ValidateOmVersionTestCases.class)
  public void testValidateOmVersion(ValidateOmVersionTestCases testCase) {
    List<ServiceInfo> serviceInfoList = new LinkedList<>();
    ServiceInfo.Builder b1 = new ServiceInfo.Builder();
    ServiceInfo.Builder b2 = new ServiceInfo.Builder();
    b1.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
    b2.setNodeType(HddsProtos.NodeType.OM).setHostname("localhost");
    if (testCase.om1Version != null) {
      b1.setOmVersion(testCase.om1Version);
      serviceInfoList.add(b1.build());
    }
    if (testCase.om2Version != null) {
      b2.setOmVersion(testCase.om2Version);
      serviceInfoList.add(b2.build());
    }
    assertEquals(testCase.validation,
        validateOmVersion(testCase.expectedVersion, serviceInfoList),
        "Running test " + testCase);
  }

  @Test
  public void testFutureVersionShouldNotBeAnExpectedVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> validateOmVersion(OzoneManagerVersion.FUTURE_VERSION, null));
  }

  @Test
  public void testCloseTwiceDoesNotWarn() throws IOException {
    RpcClient rpcClient = createRpcClient();
    GenericTestUtils.setLogLevel(RpcClient.class, Level.DEBUG);
    LogCapturer logs = LogCapturer.captureLogs(RpcClient.class);
    logs.clearOutput();

    try {
      assertDoesNotThrow(() -> {
        rpcClient.close();
        rpcClient.close();
      });

      assertThat(logs.getOutput())
          .doesNotContain("WARN")
          .doesNotContain("This metrics class is not used.");
    } finally {
      logs.stopCapturing();
    }
  }

  @Test
  public void testNfsReadRefreshUsesIdentityAfterRenameAndRejectsRecreation() throws Exception {
    AtomicReference<OmKeyInfo> live = new AtomicReference<>(nfsKey(3, "original/file"));
    AtomicReference<Function<OmKeyInfo, OmKeyInfo>> refresh = new AtomicReference<>();
    AtomicInteger lookups = new AtomicInteger();
    OmTransport transport = new MockOmTransport() {
      @Override
      public OMResponse submitRequest(OMRequest request) throws IOException {
        if (request.getCmdType() == Type.LookupNfsObject) {
          lookups.incrementAndGet();
          LookupNfsObjectRequest lookup = request.getLookupNfsObjectRequest();
          assertEquals("vol", lookup.getVolumeName());
          assertEquals("bucket", lookup.getBucketName());
          assertEquals(1, lookup.getVolumeID());
          assertEquals(2, lookup.getBucketID());
          assertEquals(3, lookup.getObjectID());
          assertEquals(LookupNfsObjectRequest.Kind.FILE, lookup.getKind());
          assertThat(lookup.getHeadOp()).isFalse();
          assertThat(lookup.getLatestVersionLocation()).isTrue();
          if (live.get() == null) {
            throw new OMException("deleted", OMException.ResultCodes.FILE_NOT_FOUND);
          }
          return OMResponse.newBuilder().setCmdType(Type.LookupNfsObject).setStatus(Status.OK)
              .setLookupNfsObjectResponse(LookupNfsObjectResponse.newBuilder()
                  .setStatus(new OzoneFileStatus(live.get(), 1024, false).getProtobuf(ClientVersion.CURRENT_VERSION)))
              .build();
        }
        assertThat(request.getCmdType()).isNotIn(Type.LookupFile, Type.LookupKey, Type.GetKeyInfo);
        return super.submitRequest(request);
      }
    };
    RpcClient client = createRpcClient(transport);
    try (MockedStatic<KeyInputStream> streams = mockStatic(KeyInputStream.class)) {
      streams.when(() -> KeyInputStream.getFromOmKeyInfo(any(), any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            refresh.set(invocation.getArgument(2));
            return new LengthInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}), 3);
          });
      OmKeyInfo original = live.get();
      try (LengthInputStream input = client.readNfsFile("vol", "bucket", 1, 2, 3)) {
        assertEquals(3, input.getLength());
        assertEquals(1, input.read());
      }
      live.set(nfsKey(3, "moved/renamed"));
      assertEquals("moved/renamed", refresh.get().apply(original).getKeyName());
      live.set(null);
      assertNull(refresh.get().apply(original), "A deleted object must not fall back to its former path");
      live.set(nfsKey(4, "original/file"));
      assertNull(refresh.get().apply(original), "A replacement must not refresh the deleted object's pipeline");
      assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, assertThrows(OMException.class,
          () -> client.readNfsFile("vol", "bucket", 1, 2, 3)).getResult());
      assertEquals(5, lookups.get());
    } finally {
      client.close();
    }
  }

  private static OmKeyInfo nfsKey(long objectId, String name) {
    return new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName(name)
        .setDataSize(3).setObjectID(objectId)
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE)).build();
  }

  private static RpcClient createRpcClient() throws IOException {
    return createRpcClient(new MockOmTransport());
  }

  private static RpcClient createRpcClient(OmTransport transport) throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    return new RpcClient(config, null) {
      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return transport;
      }

      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };
  }
}
