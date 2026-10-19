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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

class TestNfsGatewayDaemon {
  @Test
  void testRequiresSecureOzoneClusterBeforeStartup() {
    IOException failure = assertThrows(IOException.class,
        () -> NfsGatewayDaemon.start(new OzoneConfiguration()));

    assertEquals("NFS gateway requires Ozone security to be enabled", failure.getMessage());
  }

  @Test
  void testConfiguresKerberosAndKeytabRenewalBeforeLogin() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    String originalRealm = System.getProperty("java.security.krb5.realm");
    String originalKdc = System.getProperty("java.security.krb5.kdc");
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");

    try {
      IOException failure = assertThrows(IOException.class, () -> NfsGatewayDaemon.start(configuration));

      assertTrue(UserGroupInformation.isSecurityEnabled());
      assertTrue(configuration.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, false));
      assertTrue(failure.getMessage().contains("doesn't have a keytab"));
    } finally {
      UserGroupInformation.setConfiguration(new OzoneConfiguration());
      if (originalRealm == null) {
        System.clearProperty("java.security.krb5.realm");
      } else {
        System.setProperty("java.security.krb5.realm", originalRealm);
      }
      if (originalKdc == null) {
        System.clearProperty("java.security.krb5.kdc");
      } else {
        System.setProperty("java.security.krb5.kdc", originalKdc);
      }
    }
  }
}
