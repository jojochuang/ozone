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
package org.apache.hadoop.ozone.container.replication;

import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;

/**
 * ContainerReplicator wrapper with additional metrics.
 */
@Metrics(about = "Closed container replication metrics", context = "dfs")
public class MeasuredReplicator implements ContainerReplicator, AutoCloseable {

  private static final String NAME = ContainerReplicator.class.toString();

  private final ContainerReplicator delegate;

  @Metric(about = "Number of successful replication tasks")
  private MutableCounterLong success;

  @Metric(about = "Time spent on successful replication tasks")
  private MutableGaugeLong successTime;

  @Metric(about = "Number of failed replication attempts")
  private MutableCounterLong failure;

  @Metric(about = "Time spent waiting in the queue before starting the task")
  private MutableGaugeLong queueTime;

  @Metric(about = "Time spent on failed replication attempts")
  private MutableGaugeLong failureTime;

  @Metric(about = "Bytes transferred for failed replication attempts")
  private MutableGaugeLong failureBytes;

  @Metric(about = "Bytes transferred for successful replication tasks")
  private MutableGaugeLong transferredBytes;

  public MeasuredReplicator(ContainerReplicator delegate) {
    this.delegate = delegate;
    DefaultMetricsSystem.instance()
        .register(NAME, "Closed container replication", this);
  }

  @Override
  public void replicate(ReplicationTask task) {
    long start = Time.monotonicNow();

    long msInQueue =
        Duration.between(task.getQueued(), Instant.now()).toMillis();
    queueTime.incr(msInQueue);
    delegate.replicate(task);
    long elapsed = Time.monotonicNow() - start;
    if (task.getStatus() == Status.FAILED) {
      failure.incr();
      failureBytes.incr(task.getTransferredBytes());
      failureTime.incr(elapsed);
    } else if (task.getStatus() == Status.DONE) {
      transferredBytes.incr(task.getTransferredBytes());
      success.incr();
      successTime.incr(elapsed);
    }
  }

  @Override
  public void close() throws Exception {
    DefaultMetricsSystem.instance().unregisterSource(NAME);
  }

  @VisibleForTesting
  public MutableCounterLong getSuccess() {
    return success;
  }

  @VisibleForTesting
  public MutableGaugeLong getSuccessTime() {
    return successTime;
  }

  @VisibleForTesting
  public MutableGaugeLong getFailureTime() {
    return failureTime;
  }

  @VisibleForTesting
  public MutableCounterLong getFailure() {
    return failure;
  }

  @VisibleForTesting
  public MutableGaugeLong getQueueTime() {
    return queueTime;
  }

  @VisibleForTesting
  public MutableGaugeLong getTransferredBytes() {
    return transferredBytes;
  }

  @VisibleForTesting
  public MutableGaugeLong getFailureBytes() {
    return failureBytes;
  }

  @Metric("Metric to get unpacked bytes")
  public long getUnpackedBytes() {
    return TarContainerPacker.getUnpackBytes();
  }

}
