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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.batch.mapreduce.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MockOutputWriter<KEY_OUT, VAL_OUT> extends OutputWriter<KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(MockOutputWriter.class);

  public MockOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
    super(context);
    LOG.info("MockOutputWriter created!");
  }

  @Override
  public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
    LOG.info("Writing from sink {}, key: ", sinkName);
  }
}
