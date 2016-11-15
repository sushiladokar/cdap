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

package co.cask.cdap.operations;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.extension.ExtensionLoader;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * A singleton class for discovering {@link OperationalStatsFetcher} through the extension mechanism that uses
 * the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class OperationalStatsFetcherLoader {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsFetcherLoader.class);

  // This OperationalStatsFetcher serves as a tagging instance to indicate there is not fetcher supported for a given
  // service
  private static final OperationalStatsFetcher NOT_SUPPORTED_FETCHER = new OperationalStatsFetcher() {
    @Override
    public String getVersion() {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getWebURL() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getLogsURL() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public NodeStats getNodeStats() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageStats getStorageStats() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public MemoryStats getMemoryStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ComputeStats getComputeStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AppStats getAppStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public QueueStats getQueueStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public EntityStats getEntityStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ProcessStats getProcessStats() throws IOException {
      throw new UnsupportedOperationException();
    }
  };

  private final ExtensionLoader<String, OperationalStatsFetcher> operationalStatsFetcherLoader;

  @Inject
  OperationalStatsFetcherLoader(CConfiguration cConf) {
    this.operationalStatsFetcherLoader = createOperationalStatsFetcherLoader(cConf);
    this.operationalStatsFetcherLoader.preload();
  }

  private ExtensionLoader<String, OperationalStatsFetcher> createOperationalStatsFetcherLoader(CConfiguration cConf) {
    // List of extension directories to scan
    String extDirs = cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, "");
    LOG.debug("Creating extension loader for operational stats from extension directories: {}.", extDirs);
    return new ExtensionLoader<>(
      extDirs, new Function<OperationalStatsFetcher, Set<String>>() {
        @Override
        public Set<String> apply(OperationalStatsFetcher operationalStatsFetcher) {
          // Get the supported service for the given fetcher
          OperationalStatsFetcher.ServiceName service =
            operationalStatsFetcher.getClass().getAnnotation(OperationalStatsFetcher.ServiceName.class);
          return ImmutableSet.of(service.value());
        }
      },
      OperationalStatsFetcher.class, NOT_SUPPORTED_FETCHER);
  }

  /**
   * Returns an {@link OperationalStatsFetcher} for the specified service.
   */
  public OperationalStatsFetcher get(String serviceName) {
    return operationalStatsFetcherLoader.getExtension(serviceName);
  }

  /**
   * Returns a list of all {@link OperationalStatsFetcher} extensions.
   */
  public Map<String, OperationalStatsFetcher> getAll() {
    return operationalStatsFetcherLoader.listExtensions();
  }
}
