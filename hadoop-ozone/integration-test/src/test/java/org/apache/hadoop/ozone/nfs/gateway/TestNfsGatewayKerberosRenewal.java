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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verify the standalone gateway renews its Kerberos TGT from the configured keytab. */
class TestNfsGatewayKerberosRenewal {

  @Test
  void renewsTicketBeforeItExpires(@TempDir java.nio.file.Path tempDir) throws Exception {
    String originalKrb5Conf = System.getProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF);
    UserGroupInformation originalLoginUser = UserGroupInformation.getLoginUser();
    Properties securityProperties = MiniKdc.createConf();
    securityProperties.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "30");
    securityProperties.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "10");
    MiniKdc miniKdc = new MiniKdc(securityProperties, tempDir.toFile());
    try {
      miniKdc.start();
      System.setProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, miniKdc.getKrb5conf().getAbsolutePath());
      String principal = "nfs/localhost@" + miniKdc.getRealm();
      File keytab = tempDir.resolve("nfs-gateway.keytab").toFile();
      miniKdc.createPrincipal(keytab, principal);

      OzoneConfiguration configuration = new OzoneConfiguration();
      configuration.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
      configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      configuration.set(CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, "1");
      configuration.set(NfsGatewayConfigKeys.KERBEROS_PRINCIPAL, principal);
      configuration.set(NfsGatewayConfigKeys.KERBEROS_KEYTAB, keytab.getAbsolutePath());

      NfsGatewayDaemon.loginGatewayUser(configuration);
      UserGroupInformation gatewayUser = UserGroupInformation.getLoginUser();
      assertTrue(gatewayUser.isFromKeytab());
      Date initialEndTime = tgtEndTime(gatewayUser);
      assertTrue(initialEndTime.getTime() - System.currentTimeMillis() <= TimeUnit.SECONDS.toMillis(60),
          "MiniKdc did not issue the configured short-lived TGT: " + initialEndTime);
      long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(50);
      Date renewedEndTime = initialEndTime;
      while (!renewedEndTime.after(initialEndTime) && System.nanoTime() < timeout) {
        Thread.sleep(100);
        renewedEndTime = tgtEndTime(gatewayUser);
      }
      assertTrue(renewedEndTime.after(initialEndTime),
          "gateway keytab login did not obtain a renewed TGT before the original expired");
    } finally {
      miniKdc.stop();
      UserGroupInformation.setLoginUser(originalLoginUser);
      UserGroupInformation.setConfiguration(new OzoneConfiguration());
      if (originalKrb5Conf == null) {
        System.clearProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF);
      } else {
        System.setProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, originalKrb5Conf);
      }
    }
  }

  private Date tgtEndTime(UserGroupInformation user) throws Exception {
    return user.doAs((PrivilegedExceptionAction<Date>) () -> {
      Subject subject = currentSubject();
      Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
      return tickets.stream().filter(ticket -> ticket.getServer().getName().startsWith("krbtgt/"))
          .findFirst().orElseThrow(() -> new IllegalStateException("UGI has no TGT")).getEndTime();
    });
  }

  private Subject currentSubject() throws Exception {
    try {
      return (Subject) Subject.class.getMethod("current").invoke(null);
    } catch (NoSuchMethodException e) {
      return Subject.getSubject(AccessController.getContext());
    }
  }
}
