/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.util.Time;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OmUtils.getOmHostsFromConfig;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus.PREPARE_COMPLETED;

/**
 * Handler of ozone admin om comact command.
 */
@CommandLine.Command(
    name = "compact",
    description = "Compact OM RocksDB",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class Compact implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = true
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host"
  )
  private String omHost;

  @CommandLine.Option(
      names = {"-cf", "--column-family"},
      description = "Column family to be compacted.",
      defaultValue = "")
  private String columnFamily;

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol client = parent.createOmClient(omServiceId, omHost, false);
    System.out.println("Compacting column family [" + columnFamily + "]...");

    client.compact(columnFamily);
    System.out.println("Compaction request issued.");

    return null;
  }

}
