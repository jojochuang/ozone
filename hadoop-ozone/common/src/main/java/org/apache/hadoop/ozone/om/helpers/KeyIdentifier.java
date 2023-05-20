package org.apache.hadoop.ozone.om.helpers;

import java.util.Objects;

/**
 * KeyIdentifier is a unique identifier of a key.
 * TODO: eventually, migrate to inode id based key identifier.
 */
public class KeyIdentifier implements Comparable<KeyIdentifier> {
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private String clientId;

  public KeyIdentifier(OmKeyInfo omKeyInfo) {
    this.volumeName = omKeyInfo.getVolumeName();
    this.bucketName = omKeyInfo.getBucketName();
    this.keyName = omKeyInfo.getKeyName();
  }

  public KeyIdentifier(String volumeName, String bucketName, String keyName,
      String clientId) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.clientId = clientId;
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

  public String getClientId() {
    return clientId;
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

    if (clientId != null) {
      int clientIdComparison = this.clientId.compareTo(other.clientId);
      if (clientIdComparison != 0) {
        return clientIdComparison;
      }
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
    boolean b = Objects.equals(volumeName, other.volumeName) && Objects.equals(
        bucketName, other.bucketName) && Objects.equals(keyName, other.keyName);
    if (clientId != null) {
      return Objects.equals(clientId, other.clientId) && b;
    }
    return b;
  }

  @Override
  public int hashCode() {
    if (clientId == null) {
      return Objects.hash(volumeName, bucketName, keyName);
    } else {
      return Objects.hash(volumeName, bucketName, keyName, clientId);
    }
  }

  @Override
  public String toString() {
    return "volume=" + volumeName + ", bucket=" + bucketName + ", keyName="
        + keyName;
  }
}
