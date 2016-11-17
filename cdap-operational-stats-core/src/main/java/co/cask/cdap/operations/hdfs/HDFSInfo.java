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

package co.cask.cdap.operations.hdfs;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.flume.tools.VersionInfo;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * {@link OperationalStats} representing HDFS information.
 */
@SuppressWarnings("unused")
public class HDFSInfo extends AbstractHDFSStats implements HDFSInfoMXBean {

  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public URL getWebURL() throws IOException {
    if (HAUtil.isHAEnabled(conf, getNameService())) {
      return getHAWebURL();
    }
    try (DistributedFileSystem dfs = createDFS()) {
      return rpcToHttpAddress(dfs.getUri());
    }
  }

  @Override
  public URL getLogsURL() throws IOException {
    URL webURL = getWebURL();
    // should never be null for HDFS, since we know that a web url exists for HDFS
    Preconditions.checkNotNull(webURL);
    return new URL(webURL.getProtocol(), webURL.getHost(), webURL.getPort(), "/logs");
  }

  @Override
  public String getServiceName() {
    return "HDFS";
  }

  @Override
  public String getStatType() {
    return "info";
  }

  @Override
  public void collect() throws IOException {
    // TODO: Should probably throw exception.
  }

  @Nullable
  private String getNameService() {
    Collection<String> nameservices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
    if (nameservices.isEmpty()) {
      return null;
    }
    if (1 == nameservices.size()) {
      return Iterables.getOnlyElement(nameservices);
    }
    throw new IllegalStateException("Found multiple nameservices configured in HDFS. CDAP currently does not support " +
                                      "HDFS Federation.");
  }

  private URL getHAWebURL() throws IOException {
    String activeNamenode = null;
    String nameService = getNameService();
    for (String nnId : DFSUtil.getNameNodeIds(conf, nameService)) {
      HAServiceTarget haServiceTarget = new NNHAServiceTarget(conf, nameService, nnId);
      HAServiceProtocol proxy = haServiceTarget.getProxy(conf, 10000);
      HAServiceStatus serviceStatus = proxy.getServiceStatus();
      if (HAServiceProtocol.HAServiceState.ACTIVE != serviceStatus.getState()) {
        continue;
      }
      activeNamenode = DFSUtil.getNamenodeServiceAddr(conf, nameService, nnId);
    }
    if (activeNamenode == null) {
      throw new IllegalStateException("Could not find an active namenode");
    }
    return rpcToHttpAddress(URI.create(activeNamenode));
  }

  private URL rpcToHttpAddress(URI rpcURI) throws MalformedURLException {
    String host = rpcURI.getHost();
    boolean httpsEnabled = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY, DFSConfigKeys.DFS_HTTPS_ENABLE_DEFAULT);
    String namenodeWebAddress = httpsEnabled ?
      conf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT) :
      conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT);
    InetSocketAddress socketAddress = NetUtils.createSocketAddr(namenodeWebAddress);
    int namenodeWebPort = socketAddress.getPort();
    String protocol = httpsEnabled ? "https" : "http";
    return new URL(protocol, host, namenodeWebPort, "");
  }
}
