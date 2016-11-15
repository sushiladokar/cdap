/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.operations.NodeStats;
import co.cask.cdap.operations.OperationalStatsFetcher;
import co.cask.cdap.operations.OperationalStatsFetcherLoader;
import co.cask.cdap.operations.ProcessStats;
import co.cask.cdap.operations.StorageStats;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for service provider statistics
 */
@Path(Constants.Gateway.API_VERSION_3 + "/system/serviceproviders")
public class ServiceProviderStatsHandler extends AbstractHttpHandler {

  private static class Info {
    private final String version;
    private final String url;
    private final String logsUrl;

    private Info(String version, String url, String logsUrl) {
      this.version = version;
      this.url = url;
      this.logsUrl = logsUrl;
    }
  }

  private static class Stats {
    private final OperationalStatsFetcher.AppStats apps;
    private final OperationalStatsFetcher.ComputeStats compute;
    private final OperationalStatsFetcher.EntityStats entities;
    private final OperationalStatsFetcher.MemoryStats memory;
    private final OperationalStatsFetcher.QueueStats queues;
    private final NodeStats nodes;
    private final ProcessStats processes;
    private final StorageStats storage;

    private Stats(OperationalStatsFetcher.AppStats apps, OperationalStatsFetcher.ComputeStats compute,
                  OperationalStatsFetcher.EntityStats entities, OperationalStatsFetcher.MemoryStats memory,
                  OperationalStatsFetcher.QueueStats queues, NodeStats nodes, ProcessStats processes,
                  StorageStats storage) {
      this.apps = apps;
      this.compute = compute;
      this.entities = entities;
      this.memory = memory;
      this.queues = queues;
      this.nodes = nodes;
      this.processes = processes;
      this.storage = storage;
    }
  }

  private final OperationalStatsFetcherLoader operationalExtensionsLoader;

  @Inject
  ServiceProviderStatsHandler(OperationalStatsFetcherLoader operationalExtensionsLoader) {
    this.operationalExtensionsLoader = operationalExtensionsLoader;
  }


  @GET
  @Path("/")
  public void getServiceProviders(HttpRequest request, HttpResponder responder) throws IOException {
    Set<Info> operationalStatsExtensions = new HashSet<>();
    for (Map.Entry<String, OperationalStatsFetcher> extension : operationalExtensionsLoader.getAll().entrySet()) {
      OperationalStatsFetcher fetcher = extension.getValue();
      URL webUrl = fetcher.getWebURL();
      URL logsUrl = fetcher.getLogsURL();
      operationalStatsExtensions.add(new Info(fetcher.getVersion(), webUrl == null ? null : webUrl.toString(),
                                              logsUrl == null ? null : logsUrl.toString()));

    }
    responder.sendJson(HttpResponseStatus.OK, operationalStatsExtensions);
  }

  @GET
  @Path("/{service-provider}/stats")
  public void getServiceProviderStats(HttpRequest request, HttpResponder responder,
                                      @PathParam("service-provider") String serviceProvider)
    throws NotFoundException, IOException {
    // TODO: Recognize not found and throw 404
    OperationalStatsFetcher fetcher = operationalExtensionsLoader.get(serviceProvider);
    responder.sendJson(HttpResponseStatus.OK, new Stats(fetcher.getAppStats(), fetcher.getComputeStats(),
                                                        fetcher.getEntityStats(), fetcher.getMemoryStats(),
                                                        fetcher.getQueueStats(), fetcher.getNodeStats(),
                                                        fetcher.getProcessStats(), fetcher.getStorageStats()));
  }
}
