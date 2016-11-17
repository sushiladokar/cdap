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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
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
  extends AbstractExtensionLoader<String, OperationalStats> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsCollector.class);
  public static final String JMX_DOMAIN = "co.cask.cdap.operations";
  public static final String TYPE_KEY = "type";
  public static final String NAME_KEY = "name";

  private final ScheduledExecutorService executor;
  private final int statsRefreshInterval;

  @Inject
  OperationalStatsCollector(CConfiguration cConf) {
    super(cConf.get(Constants.OperationalStats.EXTENSIONS_DIR, ""), OperationalStats.class);
    this.executor = Executors.newScheduledThreadPool(
      cConf.getInt(Constants.OperationalStats.COLLECTION_THREADS), Threads.createDaemonThreadFactory("foo")
    );
    this.statsRefreshInterval = cConf.getInt(Constants.OperationalStats.REFRESH_INTERVAL_SECS);
  }

  @Override
  public Set<String> getSupportedTypesForProvider(OperationalStats operationalStats) {
    return ImmutableSet.of(operationalStats.getServiceName());
  }

  public void start() {
    // load all extensions
    loadAll();

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (final Map.Entry<String, OperationalStats> entry : list().entrySet()) {
      ObjectName objectName = getObjectName(entry.getValue());
      if (objectName == null) {
        LOG.warn("Both service name and stat type for operational stat {} are null. Ignoring stat.", entry.getValue());
        continue;
      }
      // register MBean
      try {
        mbs.registerMBean(entry.getValue(), objectName);
      } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
        LOG.warn("Error while registering MBean {} with object name {}. Ignoring this MBean.", e);
        continue;
      }
      executor.scheduleWithFixedDelay(createDaemonThread(entry.getValue()), 0, statsRefreshInterval, TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() throws IOException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<String, OperationalStats> entry : list().entrySet()) {
      ObjectName name = getObjectName(entry.getValue());
      if (name == null) {
        continue;
      }
      try {
        mbs.unregisterMBean(name);
      } catch (InstanceNotFoundException e) {
        LOG.debug("MBean {} not found while un-registering. Ignoring.", name);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Error while un-registering MBean {}.", e);
      }
    }
    executor.shutdownNow();
  }

  @Nullable
  private ObjectName getObjectName(OperationalStats operationalStats) {
    Hashtable<String, String> properties = new Hashtable<>();
    String serviceName = operationalStats.getServiceName();
    String statType = operationalStats.getStatType();
    if (Strings.isNullOrEmpty(serviceName) && Strings.isNullOrEmpty(statType)) {
      return null;
    }
    if (!Strings.isNullOrEmpty(serviceName)) {
      properties.put(NAME_KEY, serviceName);
    } else {
      LOG.warn("Found operational stat without service name - {}. This stat cannot be discovered by the service name.",
               operationalStats.getClass().getName());
    }
    if (!Strings.isNullOrEmpty(statType)) {
      properties.put(TYPE_KEY, statType);
    } else {
      LOG.warn("Found operational stat without stat type - {}. This stat cannot be discovered by the stat type.",
               operationalStats.getClass().getName());
    }
    try {
      return new ObjectName(JMX_DOMAIN, properties);
    } catch (MalformedObjectNameException e) {
      // should never happen, since we're constructing a valid domain name, and properties is non-empty
      throw Throwables.propagate(e);
    }
  }

  private Thread createDaemonThread(final OperationalStats operationalStats) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        operationalStats.collect();
      }
    }, operationalStats.getServiceName() + "-" + operationalStats.getStatType());
    thread.setDaemon(true);
    return thread;
  }
}
