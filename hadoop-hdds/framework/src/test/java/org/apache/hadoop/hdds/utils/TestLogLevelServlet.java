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

package org.apache.hadoop.hdds.utils;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

/**
 * Unit test for LogLevel.Servlet authorization.
 */
public class TestLogLevelServlet {

  private LogLevel.Servlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext context;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() throws Exception {
    servlet = new LogLevel.Servlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    context = mock(ServletContext.class);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);
    servlet.init(config);

    conf = new OzoneConfiguration();
    when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
  }

  @Test
  public void testAuthorizationAllowed() throws Exception {
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    when(request.getRemoteUser()).thenReturn("adminUser");
    AccessControlList acl = new AccessControlList("adminUser");
    when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acl);

    servlet.doGet(request, response);

    verify(response, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testAuthorizationDenied() throws Exception {
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    when(request.getRemoteUser()).thenReturn("otherUser");
    AccessControlList acl = new AccessControlList("adminUser");
    when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acl);

    servlet.doGet(request, response);

    verify(response).sendError(eq(HttpServletResponse.SC_FORBIDDEN), ArgumentMatchers.contains("authorized"));
  }

  @Test
  public void testNoSecurityAlwaysAllowed() throws Exception {
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false);
    when(request.getRemoteUser()).thenReturn(null);

    servlet.doGet(request, response);

    verify(response, never()).sendError(anyInt(), anyString());
  }
}
