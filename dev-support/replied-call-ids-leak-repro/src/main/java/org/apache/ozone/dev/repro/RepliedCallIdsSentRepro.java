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

package org.apache.ozone.dev.repro;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;

/**
 * Standalone repro for CDPD-123869: each completed read-only {@code watch} leaves an entry in
 * {@code RaftClientImpl.RepliedCallIds#sent}.
 *
 * <p>Run from repo root:
 * {@code mvn -f dev-support/replied-call-ids-leak-repro/pom.xml exec:java}
 */
public final class RepliedCallIdsSentRepro implements MiniRaftClusterWithGrpc.FactoryGet {

  private static final int ITERATIONS = 100;

  private RepliedCallIdsSentRepro() {
  }

  public static void main(String[] args) throws Exception {
    RepliedCallIdsSentRepro repro = new RepliedCallIdsSentRepro();
    repro.setStateMachine(SimpleStateMachine4Testing.class);
    MiniRaftCluster cluster = repro.newCluster(3);
    try {
      cluster.start();
      run(cluster);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.exit(1);
    } finally {
      try {
        cluster.shutdown();
      } catch (Throwable t) {
        System.err.println("cluster shutdown failed (ignored for repro): " + t);
      }
    }
  }

  private static void run(MiniRaftCluster cluster) throws Exception {
    try (RaftClient client = cluster.createClient()) {
      RaftClientReply first = client.io().send(new SimpleMessage("bootstrap"));
      client.async().watch(first.getLogIndex(), ReplicationLevel.MAJORITY_COMMITTED).get();
      int baseline = repliedCallIdsSentSize(client);
      System.out.printf("sent map size after bootstrap write+watch: %d%n", baseline);

      for (int i = 0; i < ITERATIONS; i++) {
        RaftClientReply write = client.io().send(new SimpleMessage("payload-" + i));
        RaftClientReply watch = client.async()
            .watch(write.getLogIndex(), ReplicationLevel.MAJORITY_COMMITTED).get();
        if (!watch.isSuccess()) {
          throw new IllegalStateException("watch failed at iteration " + i + ": " + watch);
        }
      }

      int after = repliedCallIdsSentSize(client);
      System.out.printf("sent map size after %d write+watch pairs: %d%n", ITERATIONS, after);
      int expectedMin = baseline + ITERATIONS - 2;
      if (after < expectedMin) {
        throw new AssertionError(String.format(
            "Expected sent size >= %d (baseline %d + ~%d watches), but was %d",
            expectedMin, baseline, ITERATIONS, after));
      }
      System.out.println("Repro succeeded: RepliedCallIds#sent grows with read-only watch RPCs.");
    }
  }

  private static int repliedCallIdsSentSize(RaftClient raftClient) throws Exception {
    RaftClientImpl impl = (RaftClientImpl) raftClient;
    Field repliedCallIdsField = RaftClientImpl.class.getDeclaredField("repliedCallIds");
    repliedCallIdsField.setAccessible(true);
    Object repliedCallIds = repliedCallIdsField.get(impl);
    Field sentField = repliedCallIds.getClass().getDeclaredField("sent");
    sentField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<Long, ?> sent = (ConcurrentMap<Long, ?>) sentField.get(repliedCallIds);
    return sent.size();
  }
}
