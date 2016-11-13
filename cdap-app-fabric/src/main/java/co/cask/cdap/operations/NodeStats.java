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

/**
 * Maintains stats about the node states of a service (e.g. HDFS, YARN, etc).
 */
public class NodeStats {
  private final int total;
  private final int healthy;
  private final int decomissioned;

  public NodeStats(int total, int healthy, int decomissioned) {
    this.total = total;
    this.healthy = healthy;
    this.decomissioned = decomissioned;
  }

  public int getTotal() {
    return total;
  }

  public int getHealthy() {
    return healthy;
  }

  public int getDecomissioned() {
    return decomissioned;
  }
}
