package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.ozone.client.io.KeyOutputStream;

/**
 * Callback listener for client side lease events.
 */
public interface LeaseEventListener {
  void beginFileLease(RpcClientFileLease.KeyIdentifier inodeId,
      KeyOutputStream out);
  void endFileLease(RpcClientFileLease.KeyIdentifier inodeId);
}
