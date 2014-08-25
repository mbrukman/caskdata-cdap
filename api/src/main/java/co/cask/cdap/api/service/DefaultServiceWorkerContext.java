/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.service;

import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.URL;
import java.util.Map;

/**
 * Default implementation of {@link ServiceWorkerContext}.
 */
public class DefaultServiceWorkerContext implements ServiceWorkerContext {
  private final Map<String, String> runtimeArgs;

  /**
   * Create a ServiceWorkerContext with runtime arguments.
   * @param runtimeArgs for the worker.
   */
  public DefaultServiceWorkerContext(Map<String, String> runtimeArgs) {
    this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }

  @Override
  public ServiceDiscovered discover(String applicationId, String serviceId, String serviceName) {
    throw new UnsupportedOperationException("Service discovery not yet supported.");
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    throw new UnsupportedOperationException("Service discovery not yet supported.");
  }

  @Override
  public URL getServiceURL(String serviceId) {
    throw new UnsupportedOperationException("Service discovery not yet supported.");
  }
}