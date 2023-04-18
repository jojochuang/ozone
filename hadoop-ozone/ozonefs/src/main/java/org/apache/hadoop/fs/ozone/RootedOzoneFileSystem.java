/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.security.token.DelegationTokenIssuer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * The Rooted Ozone Filesystem (OFS) implementation.
 * <p>
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(org.apache.hadoop.conf.Configuration)}
 * and variants to create one. If cast to {@link RootedOzoneFileSystem}, extra
 * methods and features may be accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RootedOzoneFileSystem extends BasicRootedOzoneFileSystem
    implements KeyProviderTokenIssuer, LeaseRecoverable, SafeMode {

  private OzoneFSStorageStatistics storageStatistics;

  public RootedOzoneFileSystem() {
    this.storageStatistics = new OzoneFSStorageStatistics();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return getAdapter().getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return getAdapter().getKeyProviderUri();
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    KeyProvider keyProvider;
    try {
      keyProvider = getKeyProvider();
    } catch (IOException ioe) {
      LOG.debug("Error retrieving KeyProvider.", ioe);
      return null;
    }
    if (keyProvider instanceof DelegationTokenIssuer) {
      return new DelegationTokenIssuer[]{(DelegationTokenIssuer)keyProvider};
    }
    return null;
  }

  StorageStatistics getOzoneFSOpsCountStatistics() {
    return storageStatistics;
  }

  @Override
  protected void incrementCounter(Statistic statistic, long count) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(statistic, count);
    }
  }

  @Override
  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String omHost, int omPort) throws IOException {
    return new RootedOzoneClientAdapterImpl(omHost, omPort, conf,
        storageStatistics);
  }

  @Override
  protected InputStream createFSInputStream(InputStream inputStream) {
    return new CapableOzoneFSInputStream(inputStream, statistics);
  }

  @Override
  protected OzoneFSOutputStream createFSOutputStream(
          OzoneFSOutputStream outputStream) {
    return new CapableOzoneFSOutputStream(outputStream, isHsyncEnabled());
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // qualify the path to make sure that it refers to the current FS.
    final Path p = makeQualified(path);
    boolean cap =
        OzonePathCapabilities.hasPathCapability(p, capability);
    if (cap) {
      return cap;
    }
    return super.hasPathCapability(p, capability);
  }

  /**
   * Start the lease recovery of a file.
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   */
  public boolean recoverLease(final Path f) throws IOException {
    return super.recoverLease(f);
  }

  @Override
  public boolean isFileClosed(Path file) throws IOException {
    return getAdapter().isFileClosed(file);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    switch (action) {
    case GET:
      return getSafeMode();
    case ENTER:
      throw new IOException("This file system does not support entering safe mode");
    case LEAVE:
    case FORCE_EXIT:
      forceExitSafeMode();
    }
    return false;
  }

  private boolean getSafeMode() throws IOException {
    return false; //HAUtils.getScmContainerClient(getConfSource()).inSafeMode();
  }

  private boolean forceExitSafeMode() throws IOException {
    return true; //HAUtils.getScmContainerClient(getConfSource()).forceExitSafeMode();
  }
}
