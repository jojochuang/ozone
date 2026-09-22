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

package org.apache.hadoop.ozone.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Aggregates latency samples (nanoseconds) into mean and median.
 */
public final class BenchmarkStats {

  private final List<Long> samplesNanos = new ArrayList<>();

  public void addSampleNanos(long nanos) {
    samplesNanos.add(nanos);
  }

  public int getSampleCount() {
    return samplesNanos.size();
  }

  public double meanMillis() {
    if (samplesNanos.isEmpty()) {
      return 0;
    }
    long sum = 0;
    for (long n : samplesNanos) {
      sum += n;
    }
    return (sum / (double) samplesNanos.size()) / 1_000_000.0;
  }

  public double medianMillis() {
    if (samplesNanos.isEmpty()) {
      return 0;
    }
    List<Long> sorted = new ArrayList<>(samplesNanos);
    Collections.sort(sorted);
    int mid = sorted.size() / 2;
    if (sorted.size() % 2 == 0) {
      return ((sorted.get(mid - 1) + sorted.get(mid)) / 2.0) / 1_000_000.0;
    }
    return sorted.get(mid) / 1_000_000.0;
  }

  public double meanThroughputMbPerSec(long payloadBytes) {
    double meanSec = meanMillis() / 1000.0;
    if (meanSec <= 0 || payloadBytes <= 0) {
      return 0;
    }
    return (payloadBytes / (1024.0 * 1024.0)) / meanSec;
  }

  public double medianThroughputMbPerSec(long payloadBytes) {
    double medianSec = medianMillis() / 1000.0;
    if (medianSec <= 0 || payloadBytes <= 0) {
      return 0;
    }
    return (payloadBytes / (1024.0 * 1024.0)) / medianSec;
  }
}
