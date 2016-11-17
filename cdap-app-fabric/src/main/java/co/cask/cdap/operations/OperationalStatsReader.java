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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Reads operational stats collected using {@link OperationalStatsReader}.
 */
public class OperationalStatsReader {

  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsReader.class);

  /**
   * Gets stats for all services, of the specified statType.
   *
   * @param statType the type of stats to return
   * @return a map from service name to a map containing the stats of the specified type for that service
   */
  public Map<String, Map<String, String>> getStatsOfType(String statType)
    throws MalformedObjectNameException, IntrospectionException, InstanceNotFoundException, ReflectionException,
    AttributeNotFoundException, MBeanException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(statType), "Stat type should not be null or empty");
    Hashtable<String, String> properties = new Hashtable<>();
    // we want to fetch basic details for all services, so use * as the service name
    properties.put(OperationalStatsConstants.SERVICE_NAME_KEY, "*");
    // we only want to fetch basic stats, so only fetch stats of the type info
    properties.put(OperationalStatsConstants.STAT_TYPE_KEY, statType);
    ObjectName objectName = new ObjectName(OperationalStatsConstants.JMX_DOMAIN, properties);
    Map<String, Map<String, String>> result = new HashMap<>();
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (ObjectName name : mbs.queryNames(objectName, null)) {
      String serviceName = name.getKeyProperty(OperationalStatsConstants.SERVICE_NAME_KEY);
      MBeanInfo mBeanInfo = mbs.getMBeanInfo(name);
      Map<String, String> stats = new HashMap<>();
      for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
        // casting to String is ok, because we know that all info stats are supposed to be strings.
        stats.put(attributeInfo.getName().toLowerCase(), (String) mbs.getAttribute(name, attributeInfo.getName()));
      }
      result.put(serviceName, stats);
      LOG.trace("Found object {} with attributes {}", name, stats);
    }
    return result;

  }

  /**
   * Get the operational stats for a given service.
   *
   * @param serviceName the service for which operational stats are required
   * @return a map of stat type (e.g. storage, compute, etc) to a map of stats of the specific type
   */
  public Map<String, Map<String, Object>> getOperationalStats(String serviceName)
    throws MalformedObjectNameException, IntrospectionException, InstanceNotFoundException, ReflectionException,
    AttributeNotFoundException, MBeanException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceName), "Service name should not be null or empty");
    Hashtable<String, String> properties = new Hashtable<>();
    // we want to fetch all stats for a specific service, so set the service name in the query
    properties.put(OperationalStatsConstants.SERVICE_NAME_KEY, serviceName.toLowerCase());
    // we do not care about the type of the stat, we want all stats for the specified service
    properties.put(OperationalStatsConstants.STAT_TYPE_KEY, "*");
    ObjectName objectName = new ObjectName(OperationalStatsConstants.JMX_DOMAIN, properties);
    Map<String, Map<String, Object>> result = new HashMap<>();
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (ObjectName name : mbs.queryNames(objectName, null)) {
      String type = name.getKeyProperty(OperationalStatsConstants.STAT_TYPE_KEY);
      MBeanInfo mBeanInfo = mbs.getMBeanInfo(name);
      Map<String, Object> stats = new HashMap<>();
      for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
        stats.put(attributeInfo.getName().toLowerCase(), mbs.getAttribute(name, attributeInfo.getName()));
      }
      result.put(type, stats);
      LOG.trace("Found stats of type {} for service {} as {}", type, serviceName, stats);
    }
    return result;
  }
}
