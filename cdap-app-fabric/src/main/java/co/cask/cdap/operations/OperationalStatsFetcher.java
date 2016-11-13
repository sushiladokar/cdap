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

import java.io.IOException;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * An interface to allow fetching operational statistics from services such as HDFS, YARN, HBase, etc.
 */
public interface OperationalStatsFetcher {

  /**
   * Represents the name of the service for which an {@link OperationalStatsFetcher} fetches operational stats.
   */
  @interface ServiceName {
    String value();
  }

  /**
   * Returns the version of the service.
   */
  String getVersion();

  /**
   * Returns the web URL of the service if available, and {@code null} if no such URL is available.
   */
  @Nullable
  URL getWebURL() throws IOException;

  /**
   * Returns the web URL that serves logs of the service if available, and {@code null} if no such URL is available.
   */
  @Nullable
  URL getLogsURL() throws IOException;

  /**
   * Returns the node stats of the service. These include the total number of nodes, healthy nodes and decommissioned
   * nodes.
   *
   * @throws UnsupportedOperationException if node statistics are unavailable for this service
   */
  NodeStats getNodeStats() throws IOException;

  /**
   * Returns the storage stats of the service. These include the total, used and available sizes in MB. In
   * addition, it can also serve other stats like block statistics.
   *
   * @throws UnsupportedOperationException if storage statistics are unavailable for this service
   */
  StorageStats getStorageStats() throws IOException;

  /**
   * Returns the memory stats of the service. These include the total, used and available memory in MB.
   *
   * @throws UnsupportedOperationException if storage statistics are unavailable for this service
   */
  MemoryStats getMemoryStats();

  /**
   * Returns the compute stats of the service. These include the total, used and available vcores.
   *
   * @throws UnsupportedOperationException if compute statistics are unavailable for this service
   */
  ComputeStats getComputeStats();

  /**
   * Returns the app stats of the service. These include stats about total, running, failed or killed apps.
   *
   * @throws UnsupportedOperationException if app statistics are unavailable for this service
   */
  AppStats getAppStats();

  /**
   * Returns the queue stats of the service.
   *
   * @throws UnsupportedOperationException if queue statistics are unavailable for this service
   */
  QueueStats getQueueStats();

  /**
   * Returns the entity stats of the service.
   *
   * @throws UnsupportedOperationException if entity statistics are unavailable for this service
   */
  EntityStats getEntityStats();

  /**
   * Returns the service stats summarizing the processes that are part of the service.
   *
   * @throws UnsupportedOperationException if storage statistics are unavailable for this service
   */
  ProcessStats getProcessStats() throws IOException;

  /**
   * Represents memory statistics for services.
   */
  class MemoryStats {
    long totalMB;
    long usedMB;
    long availableMB;
  }

  /**
   * Represents compute statistics for services.
   */
  class ComputeStats {
    int totalVCores;
    int usedVCores;
    int availableVCores;
  }

  /**
   * Represents app statistics for services.
   */
  class AppStats {
    int totalApps;
    int runningApps;
    int failedApps;
    int killedApps;
  }

  /**
   * Represents queue statistics for services.
   */
  class QueueStats {
    int totalQueues;
    int stoppedQueues;
    int runningQueues;
    int maxCapacity;
    int currentCapacity;
  }

  /**
   * Represents entity statistics for services.
   */
  class EntityStats {
    int namespaces;
    int artifacts;
    int apps;
    int programs;
    int datasets;
    int streams;
    int tables;
  }
}
