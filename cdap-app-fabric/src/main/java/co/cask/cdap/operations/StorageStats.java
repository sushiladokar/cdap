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
 * Represents storage statistics.
 */
public class StorageStats {
  private final long totalBytes;
  private final long usedBytes;
  private final long availableBytes;
  private final long missingBlocks;
  private final long corruptBlocks;
  private final long underreplicatedBlocks;

  public StorageStats(long totalBytes, long usedBytes, long availableBytes, long missingBlocks, long corruptBlocks,
                      long underreplicatedBlocks) {
    this.totalBytes = totalBytes;
    this.usedBytes = usedBytes;
    this.availableBytes = availableBytes;
    this.missingBlocks = missingBlocks;
    this.corruptBlocks = corruptBlocks;
    this.underreplicatedBlocks = underreplicatedBlocks;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getAvailableBytes() {
    return availableBytes;
  }

  public long getMissingBlocks() {
    return missingBlocks;
  }

  public long getCorruptBlocks() {
    return corruptBlocks;
  }

  public long getUnderreplicatedBlocks() {
    return underreplicatedBlocks;
  }
}
