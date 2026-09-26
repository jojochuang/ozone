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

package org.glassfish.jersey.server;

import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.ConfigurationProvider;
import org.apache.hadoop.ozone.recon.ReconRestResources;
import org.glassfish.hk2.api.ServiceLocator;

/**
 * Recon Jersey {@link ResourceConfig} that registers REST resources explicitly.
 * <p>
 * Lives in this package so it can override package-private {@link ResourceConfig#_getClasses()}
 * and avoid Jersey 2.48 ASM classpath scanning, which fails on JDK 25.
 */
public class ReconResourceConfig extends ResourceConfig {

  private final Set<Class<?>> resourceClasses;

  @Inject
  ReconResourceConfig(ServiceLocator serviceLocator, @Context ServletContext servletContext) {
    property(ServerProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    OzoneConfiguration ozoneConf = new ConfigurationProvider().get();
    resourceClasses = ReconRestResources.resourceClasses(ozoneConf);
    for (Class<?> resourceClass : resourceClasses) {
      register(resourceClass);
    }
    ReconRestResources.bridgeGuiceToJersey(serviceLocator, servletContext);
  }

  @Override
  Set<Class<?>> _getClasses() {
    return new HashSet<>(resourceClasses);
  }
}
