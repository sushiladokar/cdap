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
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * An interface to allow fetching operational statistics from services such as HDFS, YARN, HBase, etc.
 */
public interface OperationalStatsFetcher {

  /**
   * Represents the name of the service for which an {@link OperationalStatsFetcher} fetches operational stats.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
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
   * nodes. Returns {@code null} if no such stats are available.
   */
  @Nullable
  NodeStats getNodeStats() throws IOException;

  /**
   * Returns the storage stats of the service. These include the total, used and available sizes in bytes. In
   * addition, it can also serve other stats like block statistics. Returns {@code null} if no such stats are available.
   */
  @Nullable
  StorageStats getStorageStats() throws IOException;

  /**
   * Returns the memory stats of the service. These include the total, used and available memory in bytes.
   * Returns {@code null} if no such stats are available.
   */
  @Nullable
  MemoryStats getMemoryStats();

  /**
   * Returns the compute stats of the service. These include the total, used and available vcores.
   * Returns {@code null} if no such stats are available.
   */
  @Nullable
  ComputeStats getComputeStats();

  /**
   * Returns the app stats of the service. These include stats about total, running, failed or killed apps. Returns
   * {@code null} if no such stats are available.
   */
  @Nullable
  AppStats getAppStats();

  /**
   * Returns the queue stats of the service, or {@code null} if no such stats are available..
   */
  @Nullable
  QueueStats getQueueStats();

  /**
   * Returns the entity stats of the service, or {@code null} if no such stats are available..
   */
  @Nullable
  EntityStats getEntityStats();

  /**
   * Returns the process stats summarizing the processes that are part of the service, or {@code null} if no such stats
   * are available.
   */
  @Nullable
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
