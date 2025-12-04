---
title: "SCM Migration"
weight: 2
menu:
   main:
      parent: Features
summary: How to migrate a Storage Container Manager (SCM) to a new host in an HA setup.
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

# SCM Migration

Migrating a Storage Container Manager (SCM) from one host to another in an HA cluster is a multi-step process that can be performed with zero downtime. The process involves bootstrapping a new SCM on the target host, adding it to the cluster, reconfiguring Datanodes to recognize it, and then decommissioning the old SCM.

This guide assumes you have an existing SCM HA cluster.

### Step 1: Prepare the New SCM Host and Cluster Configuration

First, you need to prepare the new host and update the configuration on all SCMs to make them aware of the new SCM that will be added.

1.  **Install Ozone**: Install the same version of Ozone on the new host.
2.  **Copy `ozone-site.xml`**: Copy the `ozone-site.xml` from an existing SCM host to the new host.
3.  **Update `ozone-site.xml` on ALL SCM hosts**: On all SCM hosts (including the new one), edit `ozone-site.xml` to add the configuration for the new SCM.
    -   Assign a unique `nodeId` to the new SCM (e.g., `scm-new`).
    -   Set the address and port properties for the new SCM.

    For example, add the following properties, replacing `scm-ha` with your service ID and `scm-new` with your new node ID:
    ```xml
    <property>
      <name>ozone.scm.address.scm-ha.scm-new</name>
      <value>new-scm-hostname</value>
    </property>
    <property>
      <name>ozone.scm.datanode.address.scm-ha.scm-new</name>
      <value>new-scm-hostname:9860</value>
    </property>
    <property>
      <name>ozone.scm.http-address.scm-ha.scm-new</name>
      <value>new-scm-hostname:9876</value>
    </property>
    <property>
      <name>ozone.scm.ratis.port.scm-ha.scm-new</name>
      <value>10860</value> <!-- A free port for Ratis -->
    </property>
    ```

4.  **Update SCM Node List**: Also in `ozone-site.xml` on all SCM hosts, add the new SCM's `nodeId` to the `ozone.scm.nodes.<service-id>` property.
    ```xml
    <property>
      <name>ozone.scm.nodes.scm-ha</name>
      <value>scm-old-1,scm-old-2,scm-old-3,scm-new</value>
    </property>
    ```

### Step 2: Bootstrap and Start the New SCM

Now, initialize the new SCM and start it.

1.  **Set Node ID for Bootstrap**: On the new SCM host, ensure the `ozone.scm.node.id` property in its `ozone-site.xml` is set to its new `nodeId`.
    ```xml
    <property>
      <name>ozone.scm.node.id</name>
      <value>scm-new</value>
    </property>
    ```

2.  **Bootstrap**: Run the `scm --bootstrap` command on the new host. This prepares the SCM's storage directory.
    ```shell
    ozone scm --bootstrap
    ```

3.  **Start the new SCM**: Start the SCM service on the new host. It will join the HA ring, get the latest state from the leader SCM, and become a follower.

### Step 3: Reconfigure Datanodes to Add the New SCM

All Datanodes must be made aware of the new SCM.

1.  **Update Datanode `ozone-site.xml`**: On **each Datanode host**, edit `ozone-site.xml` and add the same properties for the new SCM as in Step 1 (its addresses and its addition to the `ozone.scm.nodes` list).
2.  **Apply Configuration**: Restart each Datanode service. After restarting, Datanodes will find the new SCM and start sending heartbeats to it.
3.  **Verify**: Check the web UI of the new SCM to confirm that all Datanodes have registered and are healthy.

### Step 4: Update Recon Server Configuration

After the new SCM has been added to the cluster and Datanodes are communicating with it, you need to update Recon's configuration to reflect the change in the SCM HA ring. Recon uses the SCM configuration to connect to SCMs and pull metadata.

1.  **Update `ozone-site.xml` on the Recon host**: Edit the `ozone-site.xml` file used by the Recon server.
2.  **Update Configuration for the New SCM**:
    -   Ensure that the address and port properties for the new SCM (e.g., `ozone.scm.address.scm-ha.scm-new`) are present.
    -   Update the `ozone.scm.nodes.<service-id>` property to include the `nodeId` of the new SCM.

    It's crucial that Recon has an up-to-date list of all active SCMs.

3.  **Restart Recon Server**: Restart the Recon server to apply the updated configuration.

    Use the following commands:
    ```shell
    ozone --daemon stop recon
    ozone --daemon start recon
    ```

    After restarting, Recon will connect to the updated list of SCMs and synchronize its metadata with them.

### Step 5: Transfer Leadership (If Migrating the Leader)

If the SCM you are migrating away from is the current SCM leader, you must transfer leadership to another SCM in the HA cluster. This can be any of the existing SCMs, including the newly added one.

```shell
ozone admin scm transfer --service-id=<your-scm-service-id> --new-leader=<new-scm-node-id>
```

### Step 6: Decommission the Old SCM

With the new SCM fully integrated, you can remove the old one.

1.  **Reconfigure Datanodes to Remove Old SCM**:
    -   On each Datanode, edit `ozone-site.xml` and remove the `nodeId` of the old SCM from the `ozone.scm.nodes.<service-id>` list.
    -   Restart each Datanode to apply the change.

2.  **Reconfigure Recon to Remove Old SCM**:
    -   On the Recon host, edit `ozone-site.xml` and remove the `nodeId` of the old SCM from the `ozone.scm.nodes.<service-id>` list.
    -   Restart the Recon server using the commands:
        ```shell
        ozone --daemon stop recon
        ozone --daemon start recon
        ```

3.  **Decommission the Old SCM**: After Datanodes and Recon have disconnected from the old SCM, run the decommission command.
    ```shell
    ozone admin scm decommission --service-id=<your-scm-service-id> --nodeid=<old-scm-node-id>
    ```

The old SCM will be gracefully removed from the HA ring and will shut down. The migration is now complete.

#### Special Cases for Decommissioning the Old SCM:

##### Primordial SCM
If the SCM you are decommissioning is the **primordial** SCM, you must update the `ozone.scm.primordial.node.id` property in `ozone-site.xml` on all SCMs to point to a different SCM's `nodeId` in the cluster. This change requires restarting all SCM services to take effect before proceeding with the decommission command.

### Final Note on Security
During SCM decommissioning, the private key of the decommissioned SCM should be manually deleted. The private keys can be found inside the `hdds.metadata.dir`. This manual deletion is needed until full certificate revocation support is implemented (HDDS-8399).
