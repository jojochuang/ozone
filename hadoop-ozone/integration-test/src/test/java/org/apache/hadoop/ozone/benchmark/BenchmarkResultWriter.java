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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes benchmark rows to JSON under target/compression-benchmark/.
 */
public final class BenchmarkResultWriter {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  private final String scenario;
  private final String gitSha;
  private final List<Map<String, Object>> rows = new ArrayList<>();

  public BenchmarkResultWriter(String scenario, String gitSha) {
    this.scenario = scenario;
    this.gitSha = gitSha;
  }

  public void addRow(String codec, long fileSizeBytes, String operation,
      BenchmarkStats stats, Double storedToLogicalRatio) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("codec", codec);
    row.put("fileSizeBytes", fileSizeBytes);
    row.put("operation", operation);
    row.put("samples", stats.getSampleCount());
    row.put("meanMillis", stats.meanMillis());
    row.put("medianMillis", stats.medianMillis());
    row.put("meanThroughputMbPerSec",
        stats.meanThroughputMbPerSec(fileSizeBytes));
    row.put("medianThroughputMbPerSec",
        stats.medianThroughputMbPerSec(fileSizeBytes));
    if (storedToLogicalRatio != null) {
      row.put("storedToLogicalRatio", storedToLogicalRatio);
    }
    rows.add(row);
  }

  public Path write() throws IOException {
    Path dir = Paths.get("target", "compression-benchmark");
    Files.createDirectories(dir);
    String safeScenario = scenario.replaceAll("[^a-zA-Z0-9_-]", "_");
    String fileName = String.format("results-%s-%s-%s.json",
        safeScenario,
        gitSha.length() > 12 ? gitSha.substring(0, 12) : gitSha,
        Instant.now().toString().replace(':', '-'));
    Path out = dir.resolve(fileName);

    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("scenario", scenario);
    doc.put("gitSha", gitSha);
    doc.put("timestamp", Instant.now().toString());
    doc.put("rows", rows);
    MAPPER.writeValue(out.toFile(), doc);
    System.out.println("Wrote benchmark results to " + out.toAbsolutePath());
    return out;
  }
}
