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
 * See the License for the a specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.transport.ucx.rpc;

import java.nio.ByteBuffer;

/**
 * A request to get a block of data.
 */
public class GetBlockRequest extends UcxRpcRequest {

    private final String blockId;

    public GetBlockRequest(String blockId) {
        this.blockId = blockId;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(blockId.getBytes());
        buffer.flip();
        return buffer;
    }
}
