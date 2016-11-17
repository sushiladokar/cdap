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

package co.cask.cdap.explore.executor;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.http.BodyProducer;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * BodyProducer used for returning the results of a Query, chunk by chunk.
 */
final class QueryResultsBodyProducer extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(QueryResultsBodyProducer.class);
  private static final Gson GSON = new Gson();

  private final ExploreService exploreService;
  private final QueryHandle handle;

  private final StringBuffer sb;

  private List<QueryResult> results;

  QueryResultsBodyProducer(ExploreService exploreService,
                           QueryHandle handle) throws HandleNotFoundException, SQLException, ExploreException {
    this.exploreService = exploreService;
    this.handle = handle;

    this.sb = new StringBuffer();

    // initialize
    sb.append(getCSVHeaders(exploreService.getResultSchema(handle)));
    sb.append('\n');

    results = exploreService.previewResults(handle);
    if (results.isEmpty()) {
      results = exploreService.nextResults(handle, AbstractQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    }
  }

  @Override
  public ChannelBuffer nextChunk() throws Exception {
    if (results.isEmpty()) {
      return ChannelBuffers.EMPTY_BUFFER;
    }

    for (QueryResult result : results) {
      appendCSVRow(sb, result);
      sb.append('\n');
    }
    // If failed to send to client, just propagate the IOException and let netty-http to handle
    ChannelBuffer toReturn = ChannelBuffers.wrappedBuffer(sb.toString().getBytes("UTF-8"));
    sb.delete(0, sb.length());
    results = exploreService.nextResults(handle, AbstractQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    return toReturn;
  }

  @Override
  public void finished() throws Exception {

  }

  @Override
  public void handleError(Throwable cause) {
    LOG.error("Received error while chunking query results.", cause);
  }

  private String getCSVHeaders(List<ColumnDesc> schema)
    throws HandleNotFoundException, SQLException, ExploreException {
    StringBuffer sb = new StringBuffer();
    boolean first = true;
    for (ColumnDesc columnDesc : schema) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append(columnDesc.getName());
    }
    return sb.toString();
  }

  private String appendCSVRow(StringBuffer sb, QueryResult result)
    throws HandleNotFoundException, SQLException, ExploreException {
    boolean first = true;
    for (Object o : result.getColumns()) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      // Using GSON toJson will serialize objects - in particular, strings will be quoted
      sb.append(GSON.toJson(o));
    }
    return sb.toString();
  }
}
