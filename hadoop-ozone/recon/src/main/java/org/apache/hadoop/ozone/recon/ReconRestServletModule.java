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

package org.apache.hadoop.ozone.recon;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.api.AccessHeatMapEndpoint;
import org.apache.hadoop.ozone.recon.api.BlocksEndPoint;
import org.apache.hadoop.ozone.recon.api.BucketEndpoint;
import org.apache.hadoop.ozone.recon.api.ClusterStateEndpoint;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.FeaturesEndpoint;
import org.apache.hadoop.ozone.recon.api.MetricsProxyEndpoint;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.NodeEndpoint;
import org.apache.hadoop.ozone.recon.api.OMDBInsightEndpoint;
import org.apache.hadoop.ozone.recon.api.PendingDeletionEndpoint;
import org.apache.hadoop.ozone.recon.api.PipelineEndpoint;
import org.apache.hadoop.ozone.recon.api.StorageDistributionEndpoint;
import org.apache.hadoop.ozone.recon.api.TaskStatusService;
import org.apache.hadoop.ozone.recon.api.TriggerDBSyncEndpoint;
import org.apache.hadoop.ozone.recon.api.UtilizationEndpoint;
import org.apache.hadoop.ozone.recon.api.VolumeEndpoint;
import org.apache.hadoop.ozone.recon.api.filters.ReconAdminFilter;
import org.apache.hadoop.ozone.recon.api.filters.ReconAuthFilter;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.api.ChatbotEndpoint;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to scan API Service classes and bind them to the injector.
 */
public class ReconRestServletModule extends ServletModule {

  public static final String BASE_API_PATH = "/api/v1";
  public static final String API_PACKAGE = "org.apache.hadoop.ozone.recon.api";

  public static final String CHATBOT_API_PACKAGE = "org.apache.hadoop.ozone.recon.chatbot.api";

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconRestServletModule.class);

  private final ConfigurationSource conf;

  public ReconRestServletModule(ConfigurationSource conf) {
    this.conf = conf;
  }

  @Override
  protected void configureServlets() {
    if (conf instanceof OzoneConfiguration
        && ChatbotConfigKeys.isChatbotEnabled((OzoneConfiguration) conf)) {
      configureApi(API_PACKAGE, CHATBOT_API_PACKAGE);
    } else {
      configureApi(API_PACKAGE);
    }
  }

  private void configureApi(String... packages) {
    for (String pkg : packages) {
      checkIfPackageExistsAndLog(pkg);
    }
    Map<String, String> params = new HashMap<>();
    params.put("javax.ws.rs.Application",
        GuiceResourceConfig.class.getCanonicalName());
    params.put(ServerProperties.FEATURE_AUTO_DISCOVERY_DISABLE, "true");
    params.put(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, "true");
    bind(ServletContainer.class).in(Scopes.SINGLETON);

    String allApiPath = UriBuilder.fromPath(BASE_API_PATH).path("*").build().toString();
    serve(allApiPath).with(ServletContainer.class, params);

    if (OzoneSecurityUtil.isHttpSecurityEnabled(conf)) {
      filter(allApiPath).through(ReconAuthFilter.class);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added authentication filter to path {}", allApiPath);
      }

      boolean authorizationEnabled = OzoneSecurityUtil.isAuthorizationEnabled(conf);
      if (authorizationEnabled) {
        filter(allApiPath).through(ReconAdminFilter.class);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added admin filter to path {}", allApiPath);
        }
      }
    }
  }

  private void checkIfPackageExistsAndLog(String pkg) {
    String resourcePath = pkg.replace(".", "/");
    URL resource = getClass().getClassLoader().getResource(resourcePath);
    if (resource != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using API endpoints from package {} for paths under {}.",
            pkg, BASE_API_PATH);
      }
    } else {
      LOG.warn("No Beans in '{}' found. Requests {} will fail.", pkg, BASE_API_PATH);
    }
  }
}

/**
 * Class to bridge Guice bindings to Jersey hk2 bindings.
 */
class GuiceResourceConfig extends Application {

  private static final Set<Class<?>> BASE_RECON_API_RESOURCES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          AccessHeatMapEndpoint.class,
          BlocksEndPoint.class,
          BucketEndpoint.class,
          ClusterStateEndpoint.class,
          ContainerEndpoint.class,
          FeaturesEndpoint.class,
          MetricsProxyEndpoint.class,
          NodeEndpoint.class,
          NSSummaryEndpoint.class,
          OMDBInsightEndpoint.class,
          PendingDeletionEndpoint.class,
          PipelineEndpoint.class,
          StorageDistributionEndpoint.class,
          TaskStatusService.class,
          TriggerDBSyncEndpoint.class,
          UtilizationEndpoint.class,
          VolumeEndpoint.class)));

  private final Set<Class<?>> resourceClasses;

  @Inject
  GuiceResourceConfig(ServiceLocator serviceLocator,
      @Context ServletContext servletContext) {
    Set<Class<?>> classes = new HashSet<>(BASE_RECON_API_RESOURCES);
    OzoneConfiguration ozoneConf = new ConfigurationProvider().get();
    if (ozoneConf != null && ChatbotConfigKeys.isChatbotEnabled(ozoneConf)) {
      classes.add(ChatbotEndpoint.class);
    }
    resourceClasses = Collections.unmodifiableSet(classes);
    GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
    GuiceIntoHK2Bridge guiceBridge = serviceLocator
        .getService(GuiceIntoHK2Bridge.class);
    Injector injector = (Injector) servletContext
        .getAttribute(Injector.class.getName());
    guiceBridge.bridgeGuiceInjector(injector);
  }

  @Override
  public Set<Class<?>> getClasses() {
    return resourceClasses;
  }
}
