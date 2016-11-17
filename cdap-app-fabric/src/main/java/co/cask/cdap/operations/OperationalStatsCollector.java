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
import co.cask.cdap.extension.AbstractExtensionLoader;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 * Class that registers {@link MXBean MXBeans} for reporting operational stats. To be registered by this class, the
 * class that implements an {@link MXBean} should also additionally extend {@link OperationalStats}. This class loads
 * implementations of {@link OperationalStats} using the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class OperationalStatsCollector
  extends AbstractExtensionLoader<OperationalExtensionId, OperationalStats> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsCollector.class);

  private final ScheduledExecutorService executor;
  private final int statsRefreshInterval;

  @Inject
  OperationalStatsCollector(CConfiguration cConf) {
    super(cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, ""));
    this.executor = Executors.newScheduledThreadPool(
      cConf.getInt(Constants.OperationalStats.COLLECTION_THREADS),
      Threads.createDaemonThreadFactory("operational-stats-collector-%d"));
    this.statsRefreshInterval = cConf.getInt(Constants.OperationalStats.REFRESH_INTERVAL_SECS);
  }

  @Override
  public Set<OperationalExtensionId> getSupportedTypesForProvider(OperationalStats operationalStats) {
    OperationalExtensionId operationalExtensionId = getOperationalExtensionId(operationalStats);
    return operationalExtensionId == null ?
      Collections.<OperationalExtensionId>emptySet() :
      Collections.singleton(operationalExtensionId);
  }

  /**
   * Registers all JMX {@link MXBean MXBeans} from {@link OperationalStats} extensions in the extensions directory.
   * Also schedules asynchronous stats collection for all {@link MXBean MXBeans} by calling the
   * {@link OperationalStats#collect()} method.
   */
  public void start() throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (final Map.Entry<OperationalExtensionId, OperationalStats> entry : getAll().entrySet()) {
      LOG.info("Starting stats collection for operational extension: {}", entry.getValue());
      ObjectName objectName = getObjectName(entry.getValue());
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type - {}. Ignoring this extension.",
                 OperationalStats.class.getName());
        continue;
      }
      // register MBean
      mbs.registerMBean(entry.getValue(), objectName);
      executor.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            entry.getValue().collect();
          } catch (IOException e) {
            LOG.warn("Exception while collecting stats for service: {}; type: {}", entry.getValue().getServiceName(),
                     entry.getValue().getStatType());
          }
        }
      }, 0, statsRefreshInterval, TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() throws IOException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : getAll().entrySet()) {
      ObjectName objectName = getObjectName(entry.getValue());
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type while unregistering - {}. " +
                   "Ignoring this extension.", entry.getValue().getClass().getName());
        continue;
      }
      try {
        mbs.unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("MBean {} not found while un-registering. Ignoring.", objectName);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Error while un-registering MBean {}.", e);
      }
    }
    executor.shutdownNow();
  }

  @Nullable
  private OperationalExtensionId getOperationalExtensionId(OperationalStats operationalStats) {
    String serviceName = operationalStats.getServiceName();
    String statType = operationalStats.getStatType();
    if (Strings.isNullOrEmpty(serviceName) && Strings.isNullOrEmpty(statType)) {
      return null;
    }
    if (!Strings.isNullOrEmpty(serviceName)) {
      serviceName = serviceName.toLowerCase();
    } else {
      LOG.warn("Found operational stat without service name - {}. This stat will not be discovered by service name.",
               operationalStats.getClass().getName());
    }
    if (!Strings.isNullOrEmpty(statType)) {
      statType = statType.toLowerCase();
    } else {
      LOG.warn("Found operational stat without stat type - {}. This stat will not be discovered by stat type.",
               operationalStats.getClass().getName());
    }
    return new OperationalExtensionId(serviceName, statType);
  }

  @Nullable
  private ObjectName getObjectName(OperationalStats operationalStats) {
    OperationalExtensionId operationalExtensionId = getOperationalExtensionId(operationalStats);
    if (operationalExtensionId == null) {
      return null;
    }
    Hashtable<String, String> properties = new Hashtable<>();
    properties.put(OperationalStatsConstants.SERVICE_NAME_KEY, operationalExtensionId.getServiceName());
    properties.put(OperationalStatsConstants.STAT_TYPE_KEY, operationalExtensionId.getStatType());
    try {
      return new ObjectName(OperationalStatsConstants.JMX_DOMAIN, properties);
    } catch (MalformedObjectNameException e) {
      // should never happen, since we're constructing a valid domain name, and properties is non-empty
      throw Throwables.propagate(e);
    }
  }
}
