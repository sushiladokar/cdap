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

import co.cask.cdap.operations.OperationalStatsMXBean;

import java.io.IOException;
import java.net.URL;

/**
 * Created by bhooshan on 11/15/16.
 */
public interface HDFSStatsMXBean extends OperationalStatsMXBean {
  URL getWebURL() throws IOException;
  URL getLogsURL() throws IOException;
  int getTotalDatanodes();
  int getHealthyDatanodes();
  int getDecommissionedDatanodes();
  long getCapacity() throws IOException;
  long getUsedBytes() throws IOException;
  long getRemainingBytes() throws IOException;
  long getMissingBlocks() throws IOException;
  long getUnderReplicatedBlocks() throws IOException;
  long getCorruptBlocks() throws IOException;
  int getNumNamenodes();
  int getNumDatanodes();
}
