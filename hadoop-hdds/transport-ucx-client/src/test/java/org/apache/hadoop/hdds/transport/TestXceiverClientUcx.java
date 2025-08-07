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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link XceiverClientUcx}.
 */
public class TestXceiverClientUcx {

    private XceiverClientUcx client;
    private OzoneConfiguration conf;
    private DatanodeDetails datanodeDetails;

    @BeforeEach
    public void setup() {
        conf = new OzoneConfiguration();
        datanodeDetails = DatanodeDetails.newBuilder().setHostName("localhost").setIpAddress("127.0.0.1").build();
        client = new XceiverClientUcx(datanodeDetails, conf);
    }

    @AfterEach
    public void teardown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testConnectAndClose() throws Exception {
        client.connect();
        assertEquals(datanodeDetails, client.getPipeline());
        client.close();
    }
}
