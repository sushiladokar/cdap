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

import co.cask.cdap.operations.StorageStats;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Created by bhooshan on 11/16/16.
 */
public class JMXTest {

  public interface HDFSMXBean {
    int getThis();
    String getThat();
    StorageStats getStorage();
  }

  public static class HDFS implements HDFSMXBean {

    @Override
    public int getThis() {
      return 0;
    }

    @Override
    public String getThat() {
      return "that";
    }

    @Override
    public StorageStats getStorage() {
      return new StorageStats(3453453, 343453453, 345345345, 34534534, 3453453, 34534534);
    }
  }

  public interface YARNMXBean {
    long getFoo();
    boolean isBar();
  }

  public static class YARN implements YARNMXBean {

    @Override
    public long getFoo() {
      return 0;
    }

    @Override
    public boolean isBar() {
      return false;
    }
  }

  public static void main(String [] args) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanException, IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // register a few mbeans
    Hashtable<String, String> properties1 = new Hashtable<>();
    properties1.put("type", "storage");
    properties1.put("name", "hdfs");
//    ObjectName object1 = new ObjectName("co.cask.cdap.operations:name=hdfs,type=storage");
    ObjectName object1 = new ObjectName("co.cask.cdap.operations", properties1);
    mbs.registerMBean(new HDFS(), object1);
    properties1.put("type", "compute");
    object1 = new ObjectName("co.cask.cdap.operations", properties1);
    mbs.registerMBean(new HDFS(), object1);

    Hashtable<String, String> properties2 = new Hashtable<>();
    properties2.put("type", "compute");
    properties2.put("name", "yarn");
    ObjectName object2 = new ObjectName("co.cask.cdap.operations", properties2);
    mbs.registerMBean(new YARN(), object2);


    // read 'em
    System.out.println(read(mbs, "hdfs"));
  }

  private static Map<String, Map<String, Object>> read(MBeanServer mbs, String serviceName) throws MalformedObjectNameException, IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
    Hashtable<String, String> properties = new Hashtable<>();
    properties.put("name", serviceName);
    properties.put("type", "*");
//    ObjectName objectName = new ObjectName("co.cask.cdap.operations", properties);
//    ObjectName objectName = new ObjectName("co.cask.cdap.operations:name=" + serviceName + ",type=s*");
//    ObjectName objectName = new ObjectName("*:name=" + serviceName + ",type=storage");
    ObjectName objectName = new ObjectName("co.cask.cdap.operations", properties);
    Map<String, Map<String, Object>> result = new HashMap<>();
    for (ObjectName name : mbs.queryNames(objectName, null)) {
      System.out.println(name.getCanonicalName());
      String type = name.getKeyProperty("type");
      System.out.println(type);
      MBeanInfo mBeanInfo = mbs.getMBeanInfo(name);
      Map<String, Object> inner = new HashMap<>();
      for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
        System.out.println(attributeInfo.getName());
        System.out.println(mbs.getAttribute(name, attributeInfo.getName()));
        inner.put(attributeInfo.getName(), mbs.getAttribute(name, attributeInfo.getName()));
      }
      result.put(type, inner);
      System.out.println();
    }
    return result;
  }
}
