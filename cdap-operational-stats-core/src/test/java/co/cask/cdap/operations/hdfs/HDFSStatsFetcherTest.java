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

import co.cask.cdap.operations.OperationalStatsFetcher;
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
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
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
    Assert.assertTrue(logsURL.toString().startsWith(webURL.toString()));
  }
}
