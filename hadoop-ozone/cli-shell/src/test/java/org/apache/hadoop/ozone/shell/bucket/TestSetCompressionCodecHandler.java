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

package org.apache.hadoop.ozone.shell.bucket;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.compression.CompressionCodec;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for SetCompressionCodecHandler.
 */
public class TestSetCompressionCodecHandler {

  @Test
  public void testSetCompressionCodec() throws IOException {
    SetCompressionCodecHandler handler = new SetCompressionCodecHandler();
    new CommandLine(handler).parseArgs(
        "--compression-codec", "SNAPPY", "volume/bucket");

    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);

    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(objectStore.getVolume(eq("volume"))).thenReturn(volume);
    when(volume.getBucket(anyString())).thenReturn(bucket);

    OzoneAddress address = new OzoneAddress("o3://om1/volume/bucket");
    handler.execute(client, address);

    verify(bucket).setCompressionCodec(CompressionCodec.SNAPPY);
  }
}
