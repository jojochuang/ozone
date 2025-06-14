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

package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.Closeable;

/**
 * Supplier class for deleted directories that handles cleanup of underlying resources.
 */
public class DeletedDirSupplier implements Closeable {
  private TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
      deleteTableIterator;

  /**
   * Creates a new supplier for deleted directories.
   *
   * @param deleteTableIterator The iterator over deleted directories
   */
  public DeletedDirSupplier(TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> deleteTableIterator) {
    this.deleteTableIterator = deleteTableIterator;
  }

  /**
   * Gets the next deleted directory entry.
   *
   * @return The next directory entry or null if none available
   */
  public synchronized Table.KeyValue<String, OmKeyInfo> get() {
    if (deleteTableIterator.hasNext()) {
      return deleteTableIterator.next();
    }
    return null;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(deleteTableIterator);
  }
}
