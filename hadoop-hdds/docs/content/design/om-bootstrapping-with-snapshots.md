---
title: "OM Bootstrapping with Snapshots"
menu: main
jira: HDDS-13662
summary: Automatic snapshot installation and recovery process for Ozone Manager High Availability when follower nodes fall behind
status: implemented
date: 2024-01-01
author: Ozone Development Team
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Summary

Automatic snapshot installation and recovery process for Ozone Manager High Availability when OM follower nodes fall behind the OM leader's raft log.

## Problem statement

In an Ozone Manager High Availability setup, sometimes an OM follower node may be offline or fall far behind the OM leader's raft log. When this happens, the follower cannot easily catch up by replaying individual log entries due to the large gap in the raft log.

## Technical Description

The OM HA implementation includes an automatic snapshot installation and recovery process for such cases where a follower has fallen significantly behind.

### Architecture

The automatic snapshot installation process works as follows:

1. **Leader Detection**: The leader determines that the follower is too far behind to catch up through normal log replication.

2. **Snapshot Installation Notification**: The leader notifies the follower to install a snapshot rather than attempting to replay individual log entries.

3. **Snapshot Download**: The follower downloads and installs the latest snapshot from the leader, which contains the complete current state of the OM metadata.

4. **Recovery**: After installing the snapshot, the follower OM resumes normal operation and log replication from the new state.

### Implementation Details

This logic is implemented in the `OzoneManagerStateMachine.notifyInstallSnapshotFromLeader()` method. The implementation can be found in the [code](https://github.com/apache/ozone/blob/ozone-2.0.0/hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java#L520-L531) in Release 2.0.0.

### Important Distinctions

Note that this `Raft Snapshot`, used for OM HA state synchronization, is distinct from `Ozone Snapshot`, which is used for data backup and recovery purposes.

### Recovery Scenarios

In most scenarios, stale OMs will recover automatically, even if they have missed a large number of operations. Manual intervention (such as running `ozone om --bootstrap`) is only required when adding a new OM node to the cluster.

## Storage Requirements

When an Ozone Manager (OM) acts as a follower in an HA setup, it downloads snapshot tarballs from the leader to its local metadata directory. Therefore, always ensure your OM disks have at least 2x the current OM database size to accommodate the existing data and incoming snapshots, preventing disk space issues and maintaining cluster stability.

## References

* [OM HA Design Documentation]({{< ref "design/omha.md" >}})
* [Apache Ratis State Machine API documentation](https://github.com/apache/ratis/blob/ratis-3.1.3/ratis-server-api/src/main/java/org/apache/ratis/statemachine/StateMachine.java)
* [OM HA Snapshot Installation Troubleshooting]({{< ref "../troubleshooting/om-ha-snapshot-installation.md" >}})