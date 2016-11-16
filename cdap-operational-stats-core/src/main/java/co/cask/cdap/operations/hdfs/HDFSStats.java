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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Created by bhooshan on 11/15/16.
 */
public class HDFSStats extends OperationalStats implements HDFSStatsMXBean {
  private final Configuration conf;
  private final DistributedFileSystem dfs;

  public HDFSStats() throws IOException {
    this.conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Preconditions.checkArgument(fs instanceof DistributedFileSystem, "Expected Distributed Filesystem to be the " +
      "configured file system, but found %s", fs.getClass().getName());
    this.dfs = (DistributedFileSystem) fs;
  }

  @Override
  public String getName() {
    return "HDFS";
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public URL getWebURL() throws IOException {
    if (HAUtil.isHAEnabled(conf, getNameService())) {
      return getHAWebURL();
    }
    return rpcToHttpAddress(dfs.getUri());
  }

  @Override
  public URL getLogsURL() throws IOException {
    URL webURL = getWebURL();
    // should never be null for HDFS, since we know that a web url exists for HDFS
    Preconditions.checkNotNull(webURL);
    return new URL(webURL.getProtocol(), webURL.getHost(), webURL.getPort(), "/logs");
  }

  @Override
  public int getTotalDatanodes() {
    return 0;
  }

  @Override
  public int getHealthyDatanodes() {
    return 0;
  }

  @Override
  public int getDecommissionedDatanodes() {
    return 0;
  }

  @Override
  public long getCapacity() throws IOException {
    FsStatus status = dfs.getStatus();
    return status.getCapacity();
  }

  @Override
  public long getUsedBytes() throws IOException {
    FsStatus status = dfs.getStatus();
    return status.getUsed();
  }

  @Override
  public long getRemainingBytes() throws IOException {
    FsStatus status = dfs.getStatus();
    return status.getRemaining();
  }

  @Override
  public long getMissingBlocks() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  @Override
  public long getUnderReplicatedBlocks() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  @Override
  public long getCorruptBlocks() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  @Override
  public int getNumNamenodes() {
    return getNameNodes().size();
  }

  @Override
  public int getNumDatanodes() {
    return 0;
  }

  private List<String> getNameNodes() {
    List<String> namenodes = new ArrayList<>();
    if (!HAUtil.isHAEnabled(conf, getNameService())) {
      return Collections.singletonList(dfs.getUri().toString());
    }
    String nameService = getNameService();
    for (String nnId : DFSUtil.getNameNodeIds(conf, nameService)) {
      namenodes.add(DFSUtil.getNamenodeServiceAddr(conf, nameService, nnId));
    }
    return namenodes;
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
