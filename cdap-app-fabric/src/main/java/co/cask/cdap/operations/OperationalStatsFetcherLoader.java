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
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.internal.extension.ExtensionLoader;
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Map;
import java.util.ServiceLoader;

/**
 * A singleton class for discovering {@link OperationalStatsFetcher} through the extension mechanism that uses
 * the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class OperationalStatsFetcherLoader {
  // The ProgramRunnerProvider serves as a tagging instance to indicate there is not
  // provider supported for a given program type
  private static final OperationalStatsFetcher NOT_SUPPORTED_FETCHER = new OperationalStatsFetcher() {
    @Override
    public Map<String, Object> getStats() {
      throw new UnsupportedOperationException();
    }
  };

  private final ExtensionLoader<String, OperationalStatsFetcher> operationalStatsFetcherLoader;

  @Inject
  OperationalStatsFetcherLoader(CConfiguration cConf) {
    this.operationalStatsFetcherLoader = createOperationalStatsFetcherLoader(cConf);
  }

  private ExtensionLoader<String, OperationalStatsFetcher> createOperationalStatsFetcherLoader(CConfiguration cConf) {
    // List of extension directories to scan
    String extDirs = cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, "");
    return new ExtensionLoader<>(extDirs, Predicates.<ImmutablePair<String, OperationalStatsFetcher>>alwaysTrue(),
                                 NOT_SUPPORTED_FETCHER);
  }
}
