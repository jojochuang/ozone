package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.om.helpers.KeyIdentifier;

/**
 * Callback listener for client side lease events.
 */
public interface LeaseEventListener {
  void beginFileLease(KeyIdentifier inodeId,
      KeyOutputStream out);
  void endFileLease(KeyIdentifier inodeId);
}
