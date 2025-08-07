/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.transport;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A client for data transfer using UCX.
 */
public class XceiverClientUcx extends XceiverClientSpi {

    private static final Logger LOG = LoggerFactory.getLogger(XceiverClientUcx.class);

    private final Pipeline pipeline;
    private UcpContext context;
    private UcpWorker worker;
    private UcpEndpoint endpoint;

    public XceiverClientUcx(Pipeline pipeline, OzoneConfiguration conf) {
        super();
        this.pipeline = pipeline;
    }

    @Override
    public void connect() throws Exception {
        DatanodeDetails datanodeDetails = pipeline.getClosestNode();
        LOG.info("Connecting to UCX Transfer Server on {}", datanodeDetails.getIpAddress());
        try {
            UcpParams params = new UcpParams().requestTagFeature();
            context = new UcpContext(params);
            worker = context.newWorker(new UcpWorkerParams());
            UcpEndpointParams endpointParams = new UcpEndpointParams()
                .setSocketAddress(new InetSocketAddress(datanodeDetails.getIpAddress(), datanodeDetails.getPort(DatanodeDetails.Port.Name.UCX).getValue()));
            endpoint = worker.newEndpoint(endpointParams);
        } catch (UcxException e) {
            throw new IOException("Failed to connect to UCX server", e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing connection to UCX Transfer Server");
        if (endpoint != null) {
            endpoint.close();
        }
        if (worker != null) {
            worker.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) throws IOException {
        CompletableFuture<ContainerCommandResponseProto> future = new CompletableFuture<>();
        try {
            ByteBuffer data = ByteBuffer.wrap(request.toByteArray());
            endpoint.sendTagged(data, 0, (req) -> {}).get();
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
            endpoint.recvTagged(buffer, 0, (ucpRequest) -> {
                try {
                    buffer.flip();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    future.complete(ContainerCommandResponseProto.parseFrom(bytes));
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            throw new IOException("Failed to send command", e);
        }
        return new XceiverClientReply(future);
    }

    @Override
    public HddsProtos.ReplicationType getPipelineType() {
        return HddsProtos.ReplicationType.STAND_ALONE;
    }

    @Override
    public long getReplicatedMinCommitIndex() {
        return 0;
    }

    @Override
    public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(ContainerCommandRequestProto request) throws IOException, InterruptedException {
        return null;
    }
}
