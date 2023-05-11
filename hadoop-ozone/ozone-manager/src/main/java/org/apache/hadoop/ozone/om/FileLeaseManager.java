package org.apache.hadoop.ozone.om;

import java.util.Set;

import org.apache.hadoop.ozone.lease.LeaseManager;

public class FileLeaseManager extends LeaseManager<Set<Long>> {
  /**
   * Creates an instance of lease manager.
   *
   * @param defaultTimeout Default timeout in milliseconds to be used for
   *                       lease creation.
   */
  public FileLeaseManager(long defaultTimeout) {
    super("File", defaultTimeout);
  }
}
