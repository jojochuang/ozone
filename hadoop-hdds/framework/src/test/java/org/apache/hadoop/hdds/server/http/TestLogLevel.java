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

package org.apache.hadoop.hdds.server.http;

import java.net.HttpURLConnection;
import java.net.URL;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for /logLevel endpoint authentication.
 */
public class TestLogLevel {
  private HttpServer2 server;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() throws Exception {
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");
    conf = new OzoneConfiguration();
    // Enable security
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY, "");
    conf.set("dummy.principal", "HTTP/localhost@EXAMPLE.COM");
    conf.set("dummy.keytab", "/dev/null");
    UserGroupInformation.setConfiguration(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testLogLevelUnauthenticatedAccess() throws Exception {
    server = new HttpServer2.Builder()
        .setName("test")
        .setConf(conf)
        .setSecurityEnabled(true)
        .setUsernameConfKey("dummy.principal")
        .setKeytabConfKey("dummy.keytab")
        .addEndpoint(new java.net.URI("http://localhost:0"))
        .build();
    server.start();

    int port = server.getConnectorAddress(0).getPort();
    URL url = new URL("http://localhost:" + port + "/logLevel");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    
    // In secure mode, it should challenge for authentication (401)
    // now that the filter is applied.
    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, conn.getResponseCode());
  }

  @Test
  public void testLogLevelNonSecureAccess() throws Exception {
    OzoneConfiguration nonSecureConf = new OzoneConfiguration();
    nonSecureConf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false);
    nonSecureConf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY, "");
    UserGroupInformation.setConfiguration(nonSecureConf);

    server = new HttpServer2.Builder()
        .setName("test")
        .setConf(nonSecureConf)
        .addEndpoint(new java.net.URI("http://localhost:0"))
        .build();
    server.start();

    int port = server.getConnectorAddress(0).getPort();
    URL url = new URL("http://localhost:" + port + "/logLevel");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    // In non-secure mode, it should return 200 OK
    assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
  }
}
