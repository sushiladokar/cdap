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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.etl.batch.BatchTransformDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 *
 * @param <IN>
 */
public class BatchTransformExecutor<IN> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTransformExecutor.class);
  private final Set<String> startingPoints;
  private final Map<String, BatchTransformDetail> transformDetailMap;

  public BatchTransformExecutor(Map<String, BatchTransformDetail> transformDetailMap, Set<String> startingPoints) {
    this.transformDetailMap = transformDetailMap;
    this.startingPoints = startingPoints;
  }

  public void runOneIteration(IN input) throws Exception {
    for (String stageName : startingPoints) {
      LOG.info("Starting from stage: {}", stageName);
      BatchTransformDetail transformDetail = transformDetailMap.get(stageName);
      transformDetail.process(input);
    }
  }
}
