package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;

import org.apache.hadoop.ozone.client.io.KeyOutputStream;

public interface LeaseEventListener {
  void beginFileLease(final RpcClientFileLease.KeyIdentifier inodeId,
      final KeyOutputStream out);
  void endFileLease(final RpcClientFileLease.KeyIdentifier inodeId);
}
