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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ServletContext;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.api.ChatbotEndpoint;
import org.glassfish.hk2.api.ServiceLocator;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

/**
 * Explicit Recon JAX-RS resource classes (no Jersey classpath scanning).
 */
public final class ReconRestResources {

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

  private ReconRestResources() {
  }

  public static Set<Class<?>> resourceClasses(OzoneConfiguration ozoneConf) {
    Set<Class<?>> classes = new HashSet<>(BASE_RECON_API_RESOURCES);
    if (ozoneConf != null && ChatbotConfigKeys.isChatbotEnabled(ozoneConf)) {
      classes.add(ChatbotEndpoint.class);
    }
    return Collections.unmodifiableSet(classes);
  }

  public static void bridgeGuiceToJersey(ServiceLocator serviceLocator, ServletContext servletContext) {
    GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
    GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
    Injector injector = (Injector) servletContext.getAttribute(Injector.class.getName());
    guiceBridge.bridgeGuiceInjector(injector);
  }
}
