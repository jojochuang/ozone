package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;

/**
 * This class manages the file lease of a RpcClient.
 */
public class RpcClientFileLease implements LeaseEventListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(RpcClientFileLease.class);
  private final String omServiceId;
  private final UserGroupInformation ugi;
  private final OzoneManagerClientProtocol ozoneManagerClient;
  private final ClientId clientId;
  private final OzoneClientConfig clientConfig;
  private final RpcClient rpcClient;
  /**
   * A map from file names to {@link OzoneOutputStream} objects
   * that are currently being written by this client.
   * Note that a file can only be written by a single client.
   */
  private final Map<KeyIdentifier, KeyOutputStream> filesBeingWritten
      = new HashMap<>();
  volatile long lastLeaseRenewal;
  public RpcClientFileLease(String omServiceId, UserGroupInformation ugi,
      OzoneManagerClientProtocol ozoneManagerClient, ClientId clientId,
      OzoneClientConfig clientConfig, RpcClient rpcClient) {
    this.omServiceId = omServiceId;
    this.ugi = ugi;
    this.ozoneManagerClient = ozoneManagerClient;
    this.clientId = clientId;
    this.clientConfig = clientConfig;
    this.rpcClient = rpcClient;
  }

  static public class KeyIdentifier implements Comparable<KeyIdentifier> {
    private final String volumeName;
    private final String bucketName;
    private final String keyName;

    public KeyIdentifier(String volumeName, String bucketName, String keyName) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.keyName = keyName;
    }

    public KeyIdentifier(OmKeyInfo omKeyInfo) {
      this.volumeName = omKeyInfo.getVolumeName();
      this.bucketName = omKeyInfo.getBucketName();
      this.keyName = omKeyInfo.getKeyName();
    }

    public String getVolumeName() {
      return volumeName;
    }

    public String getBucketName() {
      return bucketName;
    }

    public String getKeyName() {
      return keyName;
    }

    @Override
    public int compareTo(KeyIdentifier other) {
      int volumeComparison = this.volumeName.compareTo(other.volumeName);
      if (volumeComparison != 0) {
        return volumeComparison;
      }

      int bucketComparison = this.bucketName.compareTo(other.bucketName);
      if (bucketComparison != 0) {
        return bucketComparison;
      }

      return this.keyName.compareTo(other.keyName);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      KeyIdentifier other = (KeyIdentifier) obj;
      return Objects.equals(volumeName, other.volumeName) &&
          Objects.equals(bucketName, other.bucketName) &&
          Objects.equals(keyName, other.keyName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(volumeName, bucketName, keyName);
    }

    @Override
    public String toString() {
      return "volume=" + volumeName + ", bucket=" + bucketName + ", keyName=" +
          keyName;
    }
  }

  /** Return the lease renewer instance. The renewer thread won't start
   *  until the first output stream is created. The same instance will
   *  be returned until all output streams are closed.
   */
  public LeaseRenewer getLeaseRenewer() {
    return LeaseRenewer.getInstance(
        omServiceId != null ? omServiceId : "null", ugi, this.rpcClient);
  }

  @Override
  /** Get a lease and start automatic renewal */
  public void beginFileLease(final KeyIdentifier inodeId,
      final KeyOutputStream out) {
    synchronized (filesBeingWritten) {
      putFileBeingWritten(inodeId, out);
      LeaseRenewer renewer = getLeaseRenewer();
      boolean result = renewer.put(this.rpcClient);
      if (!result) {
        // Existing LeaseRenewer cannot add another Daemon, so remove existing
        // and add new one.
        LeaseRenewer.remove(renewer);
        renewer = getLeaseRenewer();
        renewer.put(this.rpcClient);
      }
    }
  }

  @Override
  /** Stop renewal of lease for the file. */
  public void endFileLease(final KeyIdentifier inodeId) {
    synchronized (filesBeingWritten) {
      removeFileBeingWritten(inodeId);
      // remove client from renewer if no files are open
      if (filesBeingWritten.isEmpty()) {
        getLeaseRenewer().closeClient(this.rpcClient);
      }
    }
  }


  /** Put a file. Only called from LeaseRenewer, where proper locking is
   *  enforced to consistently update its local dfsclients array and
   *  client's filesBeingWritten map.
   */
  private void putFileBeingWritten(final KeyIdentifier inodeId,
      final KeyOutputStream out) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.put(inodeId, out);
      // update the last lease renewal time only when there was no
      // writes. once there is one write stream open, the lease renewer
      // thread keeps it updated well with in anyone's expiration time.
      if (lastLeaseRenewal == 0) {
        updateLastLeaseRenewal();
      }
    }
  }

  /** Remove a file. Only called from LeaseRenewer. */
  private void removeFileBeingWritten(final KeyIdentifier inodeId) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.remove(inodeId);
      if (filesBeingWritten.isEmpty()) {
        lastLeaseRenewal = 0;
      }
    }
  }

  /** Is file-being-written map empty? */
  public boolean isFilesBeingWrittenEmpty() {
    synchronized(filesBeingWritten) {
      return filesBeingWritten.isEmpty();
    }
  }

  long getLastLeaseRenewal() {
    return lastLeaseRenewal;
  }

  void updateLastLeaseRenewal() {
    synchronized(filesBeingWritten) {
      if (filesBeingWritten.isEmpty()) {
        return;
      }
      lastLeaseRenewal = Time.monotonicNow();
    }
  }

  /** Close/abort all files being written. */
  void closeAllFilesBeingWritten(final boolean abort) {
    for(;;) {
      final KeyIdentifier inodeId;
      final KeyOutputStream out;
      synchronized(filesBeingWritten) {
        if (filesBeingWritten.isEmpty()) {
          return;
        }
        inodeId = filesBeingWritten.keySet().iterator().next();
        out = filesBeingWritten.remove(inodeId);
      }
      if (out != null) {
        try {
          if (abort) {
            out.abort();
          } else {
            out.close();
          }
        } catch(IOException ie) {
          LOG.error("Failed to " + (abort ? "abort" : "close") + " file: "
              + out.getSrc() + " with inode: " + inodeId, ie);
        }
      }
    }
  }

  /**
   * Renew leases.
   * @return true if lease was renewed. May return false if this
   * client has been closed or has no files open.
   **/
  boolean renewLease() throws IOException {
    if (rpcClient.isClientRunning() && !isFilesBeingWrittenEmpty()) {
      try {
        ozoneManagerClient.renewLease();

        updateLastLeaseRenewal();
        return true;
      } catch (IOException e) {
        // Abort if the lease has already expired.
        final long elapsed = Time.monotonicNow() - getLastLeaseRenewal();
        if (elapsed > clientConfig.getLeaseHardLimitPeriod()) {
          LOG.warn("Failed to renew lease for " + getClientId() + " for "
              + (elapsed/1000) + " seconds (>= hard-limit ="
              + (clientConfig.getLeaseHardLimitPeriod() / 1000) + " seconds.) "
              + "Closing all files being written ...", e);
          closeAllFilesBeingWritten(true);
        } else {
          // Let the lease renewer handle it and retry.
          throw e;
        }
      }
    }
    return false;
  }

  public ClientId getClientId() {
    return clientId;
  }

  @VisibleForTesting
  OzoneClientConfig getConf() {
    return clientConfig;
  }
}
