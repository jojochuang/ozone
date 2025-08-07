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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverServerSpi;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A server for data transfer using UCX.
 */
public class XceiverServerUcx implements XceiverServerSpi {

    private static final Logger LOG = LoggerFactory.getLogger(XceiverServerUcx.class);

    private final int port;
    private UcpContext context;
    private UcpWorker worker;
    private UcpListener listener;
    private final HddsDispatcher dispatcher;

    public XceiverServerUcx(OzoneConfiguration conf, OzoneContainer container, HddsDispatcher dispatcher) {
        this.port = conf.getInt(OzoneConfigKeys.HDDS_DATANODE_TRANSPORT_UCX_PORT, OzoneConfigKeys.HDDS_DATANODE_TRANSPORT_UCX_PORT_DEFAULT);
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() throws IOException {
        LOG.info("Starting UCX Transfer Server on port {}", port);
        try {
            UcpParams params = new UcpParams().requestTagFeature();
            context = new UcpContext(params);
            worker = context.newWorker(new UcpWorkerParams());
            UcpListenerParams listenerParams = new UcpListenerParams().setSocketAddress(new InetSocketAddress(port));
            listener = worker.newListener(listenerParams, (endpoint) -> {
                LOG.info("Accepted connection from {}", endpoint.getRemoteAddress());
                try {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
                    endpoint.recvTagged(buffer, 0, (ucpRequest) -> {
                        try {
                            buffer.flip();
                            byte[] bytes = new byte[buffer.remaining()];
                            buffer.get(bytes);
                            ContainerCommandRequestProto request = ContainerCommandRequestProto.parseFrom(bytes);
                            ContainerCommandResponseProto response = dispatcher.dispatch(request, null);
                            ByteBuffer data = ByteBuffer.wrap(response.toByteArray());
                            endpoint.sendTagged(data, 0, (r) -> {}).get();
                        } catch (Exception e) {
                            LOG.error("Failed to handle request", e);
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Failed to handle request", e);
                }
            });
        } catch (UcxException e) {
            throw new IOException("Failed to start UCX server", e);
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping UCX Transfer Server");
        if (listener != null) {
            listener.close();
        }
        if (worker != null) {
            worker.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public HddsProtos.DatanodeDetailsProto getDatanodeDetails() {
        return null;
    }

    @Override
    public void submitRequest(ContainerCommandRequestProto request, XceiverClientProtocol stream) {

    }

    @Override
    public ContainerCommandResponseProto getResponse(CompletableFuture<ContainerCommandResponseProto> future) {
        return null;
    }

    @Override
    public void sendResponse(ContainerCommandResponseProto response, XceiverClientProtocol stream) {

    }
}