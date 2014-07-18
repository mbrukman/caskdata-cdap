/**
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.jetstream.manager;

import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import java.util.List;

/**
 * DiscoveryServer
 */

public class DiscoveryServer {
  private NettyHttpService service;
  private DataStore dataStore;

  public DiscoveryServer(DataStore ds) {
    this.dataStore = ds;
    List<HttpHandler> handlers = Lists.newArrayList();
    handlers.add(new HubHttpHandler(ds));
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHttpChunkLimit(75 * 1024);
    service = builder.build();
    service.startAndWait();
    ds.setHubAddress(service.getBindAddress().getAddress().getHostAddress() + ":" + service.getBindAddress().getPort());
  }

  protected void finalize() throws Throwable {
    service.stopAndWait();
  }

  public String getHubAddress() {
    return service.getBindAddress().getAddress().getHostAddress() + ":" + service.getBindAddress().getPort();
  }

  public Service.State state() {
    return service.state();
  }

  public String getInstanceName() {
    return this.dataStore.getInstanceName();
  }

  public String getClearingHouseAddress() {
    return this.dataStore.getClearingHouseAddress();
  }
}
