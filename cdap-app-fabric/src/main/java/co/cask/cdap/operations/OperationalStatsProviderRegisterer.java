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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 * Created by bhooshan on 11/16/16.
 */
public class OperationalStatsProviderRegisterer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsFetcherLoader.class);
  private static final String JMX_DOMAIN = "cdap";
  private static final String TYPE_KEY = "type";
  private static final String OPERATIONS_TYPE = "operations";
  private static final String NAME_KEY = "name";

  private final ExtensionLoader<String, OperationalStats> operationalStatsLoader;

  @Inject
  OperationalStatsProviderRegisterer(CConfiguration cConf) {
    this.operationalStatsLoader = createOperationalStatsFetcherLoader(cConf);
  }

  public void register() throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<String, OperationalStats> entry : operationalStatsLoader.listExtensions().entrySet()) {
      mbs.registerMBean(entry.getValue(), constructObjectName(entry.getKey()));
    }
  }

  private ExtensionLoader<String, OperationalStats> createOperationalStatsFetcherLoader(CConfiguration cConf) {
    // List of extension directories to scan
    String extDirs = cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, "");
    LOG.debug("Creating extension loader for operational stats from extension directories: {}.", extDirs);
    return new ExtensionLoader<>(
      extDirs, new Function<OperationalStats, Set<String>>() {
      @Override
      public Set<String> apply(OperationalStats operationalStatsFetcher) {
        // Get the supported service for the given fetcher
        OperationalStatsFetcher.ServiceName service =
          operationalStatsFetcher.getClass().getAnnotation(OperationalStatsFetcher.ServiceName.class);
        return ImmutableSet.of(service.value());
      }
    },
      OperationalStats.class, new OperationalStats() {
      @Override
      public String getName() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getVersion() {
        throw new UnsupportedOperationException();
      }
    });
  }

  @Override
  public void close() throws IOException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<String, OperationalStats> entry : operationalStatsLoader.listExtensions().entrySet()) {
      ObjectName name = constructObjectName(entry.getKey());
      try {
        mbs.unregisterMBean(name);
      } catch (InstanceNotFoundException e) {
        LOG.debug("MBean {} not found while un-registering. Ignoring.", name);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Error while un-registering MBean {}.", e);
      }
    }
  }

  private ObjectName constructObjectName(String operationalStatsName) {
    Hashtable<String, String> properties = new Hashtable<>();
    properties.put(TYPE_KEY, OPERATIONS_TYPE);
    properties.put(NAME_KEY, operationalStatsName);
    try {
      return new ObjectName(JMX_DOMAIN, properties);
    } catch (MalformedObjectNameException e) {
      // should never happen, since we're constructing a valid domain name, and properties is non-empty
      throw Throwables.propagate(e);
    }
  }
}
