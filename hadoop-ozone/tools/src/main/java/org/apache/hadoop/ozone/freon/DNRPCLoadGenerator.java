/*
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
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Utility to generate RPC request to DN.
 */
@Command(name = "dn-echo",
        aliases = "dne",
        description =
                "Generate echo RPC request to DataNode",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class DNRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private static final int RPC_PAYLOAD_MULTIPLICATION_FACTOR = 1024;
  private static final int MAX_SIZE_KB = 2097151;
  private Timer timer;
  private OzoneConfiguration configuration;
  //private OzoneManagerProtocolClientSideTranslatorPB[] clients;
  private ScmClient scmClient;
  private ByteString payloadReqBytes;
  private int payloadRespSize;
  private Pipeline pipeline;
  private ContainerInfo containerInfo;
  private XceiverClientSpi client;
  private XceiverClientFactory xceiverClientManager;
  private ByteString payloadReq;
  @Option(names = {"--payload-req"},
          description =
                  "Specifies the size of payload in KB in RPC request. " +
                          "Max size is 2097151 KB",
          defaultValue = "0")
  private int payloadReqSizeKB = 0;

  @Option(names = {"--clients"},
      description =
          "Number of clients, defaults 1.",
      defaultValue = "1")
  private int clientsCount = 1;

  @Option(names = {"--payload-resp"},
          description =
                  "Specifies the size of payload in KB in RPC response. " +
                          "Max size is 2097151 KB",
          defaultValue = "0")
  private int payloadRespSizeKB = 0;

  @Option(names = {"--ratis"},
      description = "Write to Ratis log, skip flag for read-only EchoRPC " +
          "request")
  private boolean writeToRatis = false;
  @Option(names = {"--containerID"},
      description = "Send echo to DataNodes associated with this container")
  private long containerID;

  @CommandLine.ParentCommand
  private Freon freon;

  // empy constructor for picocli
  DNRPCLoadGenerator() {
  }

  @VisibleForTesting
  DNRPCLoadGenerator(OzoneConfiguration ozoneConfiguration) {
    this.configuration = ozoneConfiguration;
  }

  @Override
  public Void call() throws Exception {
    Preconditions.checkArgument(payloadReqSizeKB >= 0,
            "OM echo request payload size should be positive value or zero.");
    Preconditions.checkArgument(payloadRespSizeKB >= 0,
            "OM echo response payload size should be positive value or zero.");

    if (configuration == null) {
      configuration = freon.createOzoneConfiguration();
    }
    scmClient = new ContainerOperationClient(configuration);
    containerInfo = scmClient.getContainer(containerID);

    List<Pipeline> pipelineList = scmClient.listPipelines();
    pipeline = pipelineList.stream()
        .filter(p -> p.getId().equals(containerInfo.getPipelineID()))
        .findFirst()
        .orElse(null);
    xceiverClientManager = new XceiverClientManager(configuration);
    client = xceiverClientManager.acquireClient(pipeline);

    init();
    payloadReqBytes = UnsafeByteOperations.unsafeWrap(RandomUtils.nextBytes(
        calculateMaxPayloadSize(payloadReqSizeKB)));
    payloadRespSize = calculateMaxPayloadSize(payloadRespSizeKB);
    timer = getMetrics().timer("rpc-payload");
    try {
      runTests(this::sendRPCReq);
    } finally {
      xceiverClientManager.releaseClient(client, false);
      xceiverClientManager.close();
      scmClient.close();
    }
    return null;
  }

  private int calculateMaxPayloadSize(int payloadSizeKB) {
    if (payloadSizeKB > 0) {
      return Math.min(
              Math.toIntExact((long)payloadSizeKB *
                      RPC_PAYLOAD_MULTIPLICATION_FACTOR),
              MAX_SIZE_KB);
    }
    return 0;
  }

  private void sendRPCReq(long l) throws Exception {
    timer.time(() -> {
      ContainerProtos.EchoResponseProto response =
          ContainerProtocolCalls.echo(client, containerID, payloadReqBytes, payloadRespSizeKB);
      return null;
    });
  }
}


