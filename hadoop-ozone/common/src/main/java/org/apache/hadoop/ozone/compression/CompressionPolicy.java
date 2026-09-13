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
 * See the License for applicable law governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.compression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;

/**
 * Resolves the compression codec for a key at create/overwrite time.
 */
public final class CompressionPolicy {

  public static final String DEFAULT_SKIP_EXTENSIONS =
      ".parquet,.orc,.gz,.zip,.bz2,.zst,.jpg,.jpeg,.png,.gif,.mp4,.avi,.pdf,"
          + ".doc,.docx,.xls,.xlsx";

  private CompressionPolicy() {
  }

  public static CompressionCodec resolveKeyCodec(OmBucketInfo bucketInfo,
      String keyName, FileEncryptionInfo encInfo, OmConfig omConfig) {
    if (bucketInfo == null
        || bucketInfo.getCompressionCodec() == null
        || !bucketInfo.getCompressionCodec().isEnabled()) {
      return CompressionCodec.NONE;
    }
    if (encInfo != null) {
      return CompressionCodec.NONE;
    }
    if (isExtensionDenied(keyName, omConfig)) {
      return CompressionCodec.NONE;
    }
    return bucketInfo.getCompressionCodec();
  }

  public static boolean isExtensionDenied(String keyName, OmConfig omConfig) {
    String configuredExtensions = omConfig.getCompressionSkipExtensions();
    if (StringUtils.isBlank(configuredExtensions)) {
      configuredExtensions = DEFAULT_SKIP_EXTENSIONS;
    }
    String extension = longestMatchingExtension(keyName,
        parseExtensions(configuredExtensions));
    return extension != null;
  }

  static Collection<String> parseExtensions(String configValue) {
    if (StringUtils.isBlank(configValue)) {
      return Collections.emptyList();
    }
    return org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(configValue);
  }

  static String normalizeExtension(String extension) {
    if (StringUtils.isBlank(extension)) {
      return null;
    }
    String normalized = extension.trim().toLowerCase();
    if (!normalized.startsWith(".")) {
      normalized = "." + normalized;
    }
    return normalized;
  }

  static String longestMatchingExtension(String keyName,
      Collection<String> configuredExtensions) {
    if (StringUtils.isBlank(keyName) || configuredExtensions.isEmpty()) {
      return null;
    }
    String lowerKeyName = keyName.toLowerCase();
    List<String> normalized = new ArrayList<>();
    for (String extension : configuredExtensions) {
      String normalizedExtension = normalizeExtension(extension);
      if (normalizedExtension != null) {
        normalized.add(normalizedExtension);
      }
    }
    normalized.sort(Comparator.comparingInt(String::length).reversed());
    for (String extension : normalized) {
      if (lowerKeyName.endsWith(extension)) {
        return extension;
      }
    }
    return null;
  }
}
