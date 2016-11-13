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

import co.cask.cdap.operations.NodeStats;
import co.cask.cdap.operations.OperationalStatsFetcher;
import co.cask.cdap.operations.ProcessStats;
import co.cask.cdap.operations.StorageStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;

/**
 * Tests for {@link HDFSStatsFetcher}
 */
public class HDFSStatsFetcherTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TMP_FOLDER.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(2).build();
    dfsCluster.waitClusterUp();
  }

  @AfterClass
  public static void teardown() {
    dfsCluster.shutdown();
  }

  @Test
  public void test() throws IOException {
    DistributedFileSystem dfs = dfsCluster.getFileSystem();
    OperationalStatsFetcher fetcher = new HDFSStatsFetcher(dfs);
    Assert.assertNotNull(fetcher.getVersion());
    URL webURL = fetcher.getWebURL();
    Assert.assertNotNull(webURL);
    URL logsURL = fetcher.getLogsURL();
    Assert.assertNotNull(logsURL);
    Assert.assertEquals(webURL.getProtocol(), logsURL.getProtocol());
    Assert.assertEquals(webURL.getHost(), logsURL.getHost());
    Assert.assertEquals(webURL.getPort(), logsURL.getPort());
    Assert.assertEquals("/logs", logsURL.getPath());
    StorageStats storageStats = fetcher.getStorageStats();
    Assert.assertEquals(0, storageStats.getCorruptBlocks());
    Assert.assertEquals(0, storageStats.getMissingBlocks());
    Assert.assertEquals(0, storageStats.getUnderreplicatedBlocks());
    Assert.assertTrue(storageStats.getTotalMB() > storageStats.getAvailableMB());
    Assert.assertTrue(storageStats.getAvailableMB() > 0);
    NodeStats nodeStats = fetcher.getNodeStats();
    Assert.assertEquals(2, nodeStats.getTotal());
    Assert.assertEquals(2, nodeStats.getHealthy());
    Assert.assertEquals(0, nodeStats.getDecomissioned());
    ProcessStats processStats = fetcher.getProcessStats();
    Assert.assertEquals((Integer) 1, processStats.getProcesses().get("namenodes"));
    Assert.assertEquals((Integer) 2, processStats.getProcesses().get("datanodes"));
    try {
      fetcher.getAppStats();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
    try {
      fetcher.getComputeStats();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
    try {
      fetcher.getMemoryStats();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
    try {
      fetcher.getQueueStats();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
    try {
      fetcher.getEntityStats();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}
