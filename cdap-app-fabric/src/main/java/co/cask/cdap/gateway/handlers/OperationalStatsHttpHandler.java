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
import co.cask.cdap.operations.OperationalStatsReader;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for service provider statistics.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/system/serviceproviders")
public class OperationalStatsHttpHandler extends AbstractHttpHandler {

  private final OperationalStatsReader operationalExtensionsReader;

  @Inject
  OperationalStatsHttpHandler(OperationalStatsReader operationalStatsReader) {
    this.operationalExtensionsReader = operationalStatsReader;
  }

  @GET
  @Path("/")
  public void getServiceProviders(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, operationalExtensionsReader.getStatsOfType("info"));
  }

  @GET
  @Path("/{service-provider}/stats")
  public void getServiceProviderStats(HttpRequest request, HttpResponder responder,
                                      @PathParam("service-provider") String serviceProvider) throws Exception {
    Map<String, Map<String, Object>> stats = operationalExtensionsReader.getOperationalStats(serviceProvider);
    if (stats.isEmpty()) {
      throw new NotFoundException(String.format("Service provider %s not found", serviceProvider));
    }
    // info is only needed in the list API, not in the stats API
    stats.remove("info");
    responder.sendJson(HttpResponseStatus.OK, stats);
  }
}
