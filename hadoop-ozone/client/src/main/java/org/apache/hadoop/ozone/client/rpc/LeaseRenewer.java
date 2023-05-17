/**
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
package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Used by {@link RpcClient} for renewing
 * file-being-written leases on the namenode.
 * When a file is opened for write (create or append),
 * namenode stores a file lease for recording the identity of the writer.
 * The writer (i.e. the RpcClient) is required to renew the lease periodically.
 * When the lease is not renewed before it expires,
 * the namenode considers the writer as failed and then it may either let
 * another writer to obtain the lease or close the file.
 * </p>
 * <p>
 * This class also provides the following functionality:
 * <ul>
 * <li>
 * It maintains a map from (namenode, user) pairs to lease renewers.
 * The same {@link LeaseRenewer} instance is used for renewing lease
 * for all the {@link RpcClient} to the same namenode and
 * the same user.
 * </li>
 * <li>
 * Each renewer maintains a list of {@link RpcClient}.
 * Periodically the leases for all the clients are renewed.
 * A client is removed from the list when the client is closed.
 * </li>
 * <li>
 * A thread per namenode per user is used by the {@link LeaseRenewer}
 * to renew the leases.
 * </li>
 * </ul>
 * </p>
 */
@InterfaceAudience.Private
public class LeaseRenewer {
  public static final Logger LOG = LoggerFactory.getLogger(LeaseRenewer.class);

  private static long leaseRenewerGraceDefault = 60*1000L;
  static final long LEASE_RENEWER_SLEEP_DEFAULT = 1000L;

  private AtomicBoolean isLSRunning = new AtomicBoolean(false);

  /** Get a {@link LeaseRenewer} instance */
  public static LeaseRenewer getInstance(final String authority,
      final UserGroupInformation ugi, final RpcClient dfsc) {
    final LeaseRenewer
        r = LeaseRenewer.Factory.INSTANCE.get(authority, ugi);
    r.addClient(dfsc);
    return r;
  }

  /**
   * Remove the given renewer from the Factory.
   * Subsequent call will receive new {@link LeaseRenewer} instance.
   * @param renewer Instance to be cleared from Factory
   */
  public static void remove(LeaseRenewer renewer) {
    synchronized (renewer) {
      LeaseRenewer.Factory.INSTANCE.remove(renewer);
    }
  }

  /**
   * A factory for sharing {@link LeaseRenewer} objects
   * among {@link RpcClient} instances
   * so that there is only one renewer per authority per user.
   */
  private static class Factory {
    private static final LeaseRenewer.Factory
        INSTANCE = new LeaseRenewer.Factory();

    private static class Key {
      /** Namenode info */
      final String authority;
      /** User info */
      final UserGroupInformation ugi;

      private Key(final String authority, final UserGroupInformation ugi) {
        if (authority == null) {
          throw new HadoopIllegalArgumentException("authority == null");
        } else if (ugi == null) {
          throw new HadoopIllegalArgumentException("ugi == null");
        }

        this.authority = authority;
        this.ugi = ugi;
      }

      @Override
      public int hashCode() {
        return authority.hashCode() ^ ugi.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof LeaseRenewer.Factory.Key) {
          final LeaseRenewer.Factory.Key that = (LeaseRenewer.Factory.Key)obj;
          return this.authority.equals(that.authority)
              && this.ugi.equals(that.ugi);
        }
        return false;
      }

      @Override
      public String toString() {
        return ugi.getShortUserName() + "@" + authority;
      }
    }

    /** A map for per user per namenode renewers. */
    private final Map<LeaseRenewer.Factory.Key, LeaseRenewer> renewers = new HashMap<>();

    /** Get a renewer. */
    private synchronized LeaseRenewer get(final String authority,
        final UserGroupInformation ugi) {
      final LeaseRenewer.Factory.Key
          k = new LeaseRenewer.Factory.Key(authority, ugi);
      LeaseRenewer r = renewers.get(k);
      if (r == null) {
        r = new LeaseRenewer(k);
        renewers.put(k, r);
      }
      return r;
    }

    /** Remove the given renewer. */
    private synchronized void remove(final LeaseRenewer r) {
      final LeaseRenewer stored = renewers.get(r.factorykey);
      //Since a renewer may expire, the stored renewer can be different.
      if (r == stored) {
        // Expire LeaseRenewer daemon thread as soon as possible.
        r.clearClients();
        r.setEmptyTime(0);
        renewers.remove(r.factorykey);
      }
    }
  }

  /** The time in milliseconds that the map became empty. */
  private long emptyTime = Long.MAX_VALUE;
  /** A fixed lease renewal time period in milliseconds */
  private long renewal = HdfsConstants.LEASE_SOFTLIMIT_PERIOD / 2;

  /** A daemon for renewing lease */
  private Daemon daemon = null;
  /** Only the daemon with currentId should run. */
  private int currentId = 0;

  /**
   * A period in milliseconds that the lease renewer thread should run
   * after the map became empty.
   * In other words,
   * if the map is empty for a time period longer than the grace period,
   * the renewer should terminate.
   */
  private long gracePeriod;
  /**
   * The time period in milliseconds
   * that the renewer sleeps for each iteration.
   */
  private long sleepPeriod;

  private final LeaseRenewer.Factory.Key factorykey;

  /** A list of clients corresponding to this renewer. */
  private final List<RpcClient> RpcClients = new ArrayList<>();

  /**
   * A stringified stack trace of the call stack when the Lease Renewer
   * was instantiated. This is only generated if trace-level logging is
   * enabled on this class.
   */
  private final String instantiationTrace;

  private LeaseRenewer(LeaseRenewer.Factory.Key factorykey) {
    this.factorykey = factorykey;
    unsyncSetGraceSleepPeriod(leaseRenewerGraceDefault);

    if (LOG.isTraceEnabled()) {
      instantiationTrace = StringUtils.stringifyException(
          new Throwable("TRACE"));
    } else {
      instantiationTrace = null;
    }
  }

  /** @return the renewal time in milliseconds. */
  private synchronized long getRenewalTime() {
    return renewal;
  }

  /** Used for testing only. */
  @VisibleForTesting
  public synchronized void setRenewalTime(final long renewal) {
    this.renewal = renewal;
  }

  /** Add a client. */
  private synchronized void addClient(final RpcClient dfsc) {
    for(RpcClient c : RpcClients) {
      if (c == dfsc) {
        //client already exists, nothing to do.
        return;
      }
    }
    //client not found, add it
    RpcClients.add(dfsc);

    //update renewal time
    final int hdfsTimeout = 0; //dfsc.getConf().getHdfsTimeout();
    if (hdfsTimeout > 0) {
      final long half = hdfsTimeout/2;
      if (half < renewal) {
        this.renewal = half;
      }
    }
  }

  private synchronized void clearClients() {
    RpcClients.clear();
  }

  private synchronized boolean clientsRunning() {
    for(Iterator<RpcClient> i = RpcClients.iterator(); i.hasNext(); ) {
      if (!i.next().isClientRunning()) {
        i.remove();
      }
    }
    return !RpcClients.isEmpty();
  }

  private synchronized long getSleepPeriod() {
    return sleepPeriod;
  }

  /** Set the grace period and adjust the sleep period accordingly. */
  synchronized void setGraceSleepPeriod(final long gracePeriod) {
    unsyncSetGraceSleepPeriod(gracePeriod);
  }

  private void unsyncSetGraceSleepPeriod(final long gracePeriod) {
    if (gracePeriod < 100L) {
      throw new HadoopIllegalArgumentException(gracePeriod
          + " = gracePeriod < 100ms is too small.");
    }
    this.gracePeriod = gracePeriod;
    final long half = gracePeriod/2;
    this.sleepPeriod = half < LEASE_RENEWER_SLEEP_DEFAULT?
        half: LEASE_RENEWER_SLEEP_DEFAULT;
  }

  @VisibleForTesting
  /** Is the daemon running? */
  public synchronized boolean isRunning() {
    return daemon != null && daemon.isAlive();
  }

  /** Does this renewer have nothing to renew? */
  public boolean isEmpty() {
    return RpcClients.isEmpty();
  }

  /** Used only by tests */
  synchronized String getDaemonName() {
    return daemon.getName();
  }

  /** Is the empty period longer than the grace period? */
  private synchronized boolean isRenewerExpired() {
    return emptyTime != Long.MAX_VALUE
        && Time.monotonicNow() - emptyTime > gracePeriod;
  }

  public synchronized boolean put(final RpcClient dfsc) {
    if (dfsc.isClientRunning()) {
      if (!isRunning() || isRenewerExpired()) {
        // Start a new daemon with a new id.
        final int id = ++currentId;
        if (isLSRunning.get()) {
          // Not allowed to add multiple daemons into LeaseRenewer, let client
          // create new LR and continue to acquire lease.
          return false;
        }
        isLSRunning.getAndSet(true);

        daemon = new Daemon(new Runnable() {
          @Override
          public void run() {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " started");
              }
              LeaseRenewer.this.run(id);
            } catch(InterruptedException e) {
              LOG.debug("LeaseRenewer is interrupted.", e);
            } finally {
              synchronized(LeaseRenewer.this) {
                LeaseRenewer.Factory.INSTANCE.remove(
                    LeaseRenewer.this);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " exited");
              }
            }
          }

          @Override
          public String toString() {
            return String.valueOf(
                LeaseRenewer.this);
          }
        });
        daemon.start();
      }
      emptyTime = Long.MAX_VALUE;
    }
    return true;
  }

  @VisibleForTesting
  synchronized void setEmptyTime(long time) {
    emptyTime = time;
  }

  /** Close the given client. */
  public synchronized void closeClient(final RpcClient dfsc) {
    RpcClients.remove(dfsc);
    if (RpcClients.isEmpty()) {
      if (!isRunning() || isRenewerExpired()) {
        LeaseRenewer.Factory.INSTANCE.remove(
            LeaseRenewer.this);
        return;
      }
      if (emptyTime == Long.MAX_VALUE) {
        //discover the first time that the client list is empty.
        emptyTime = Time.monotonicNow();
      }
    }

    //update renewal time
    int closedClientTimeout = 0;//dfsc.getConf().getHdfsTimeout()
    if (renewal == closedClientTimeout/2) {
      long min = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
      for(RpcClient c : RpcClients) {
        final int timeout = 0; //c.getConf().getHdfsTimeout();
        if (timeout > 0 && timeout < min) {
          min = timeout;
        }
      }
      renewal = min/2;
    }
  }

  public void interruptAndJoin() throws InterruptedException {
    Daemon daemonCopy = null;
    synchronized (this) {
      if (isRunning()) {
        daemon.interrupt();
        daemonCopy = daemon;
      }
    }

    if (daemonCopy != null) {
      LOG.debug("Wait for lease checker to terminate");
      daemonCopy.join();
    }
  }

  private void renew() throws IOException {
    final List<RpcClient> copies;
    synchronized(this) {
      copies = new ArrayList<>(RpcClients);
    }
    //sort the client names for finding out repeated names.
    Collections.sort(copies, new Comparator<RpcClient>() {
      @Override
      public int compare(final RpcClient left, final RpcClient right) {
        return left.getClientId().toString().compareTo(right.getClientId().toString());
      }
    });
    String previousName = "";
    for (final RpcClient c : copies) {
      //skip if current client name is the same as the previous name.
      if (!c.getClientId().equals(previousName)) {
        if (!c.renewLease()) {
          LOG.debug("Did not renew lease for client {}", c);
          continue;
        }
        previousName = c.getClientId().toString();
        LOG.debug("Lease renewed for client {}", previousName);
      }
    }
  }

  /**
   * Periodically check in with the namenode and renew all the leases
   * when the lease period is half over.
   */
  private void run(final int id) throws InterruptedException {
    for(long lastRenewed = Time.monotonicNow(); !Thread.interrupted();
        Thread.sleep(getSleepPeriod())) {
      final long elapsed = Time.monotonicNow() - lastRenewed;
      if (elapsed >= getRenewalTime()) {
        try {
          renew();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Lease renewer daemon for " + clientsString()
                + " with renew id " + id + " executed");
          }
          lastRenewed = Time.monotonicNow();
        } catch (SocketTimeoutException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Aborting ...", ie);
          List<RpcClient> RpcClientsCopy;
          synchronized (this) {
            //RpcClientFaultInjector.get().delayWhenRenewLeaseTimeout();
            RpcClientsCopy = new ArrayList<>(RpcClients);
            LeaseRenewer.Factory.INSTANCE.remove(
                LeaseRenewer.this);
          }
          for (RpcClient rpcClient : RpcClientsCopy) {
            rpcClient.closeAllFilesBeingWritten(true);
          }
          break;
        } catch (IOException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Will retry shortly ...", ie);
        }
      }

      synchronized(this) {
        if (id != currentId || isRenewerExpired()) {
          if (LOG.isDebugEnabled()) {
            if (id != currentId) {
              LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " is not current");
            } else {
              LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " expired");
            }
          }
          //no longer the current daemon or expired
          return;
        }

        // if no clients are in running state or there is no more clients
        // registered with this renewer, stop the daemon after the grace
        // period.
        if (!clientsRunning() && emptyTime == Long.MAX_VALUE) {
          emptyTime = Time.monotonicNow();
        }
      }
    }
  }

  @Override
  public String toString() {
    String s = getClass().getSimpleName() + ":" + factorykey;
    if (LOG.isTraceEnabled()) {
      return s + ", clients=" +  clientsString()
          + ", created at " + instantiationTrace;
    }
    return s;
  }

  /** Get the names of all clients */
  private synchronized String clientsString() {
    if (RpcClients.isEmpty()) {
      return "[]";
    } else {
      final StringBuilder b = new StringBuilder("[").append(
          RpcClients.get(0).getClientId());
      for(int i = 1; i < RpcClients.size(); i++) {
        b.append(", ").append(RpcClients.get(i).getClientId());
      }
      return b.append("]").toString();
    }
  }

  @VisibleForTesting
  public static void setLeaseRenewerGraceDefault(
      long leaseRenewerGraceDefault) {
    LeaseRenewer.leaseRenewerGraceDefault = leaseRenewerGraceDefault;
  }
}
