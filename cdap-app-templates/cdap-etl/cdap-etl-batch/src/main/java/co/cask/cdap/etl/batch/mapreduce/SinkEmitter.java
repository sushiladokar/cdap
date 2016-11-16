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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.batch.ETLTransformDetail;

import java.util.HashMap;

/**
 *
 * @param <KEY_OUT>
 * @param <VAL_OUT>
 */
public class SinkEmitter<KEY_OUT, VAL_OUT> extends ETLEmitter<Object> {
  private final OutputWriter outputWriter;
  private final String stageName;

  public SinkEmitter(String stageName, OutputWriter<KEY_OUT, VAL_OUT> outputWriter) {
    super(new HashMap<String, ETLTransformDetail>());
    this.outputWriter = outputWriter;
    this.stageName = stageName;
  }

  @Override
  public void emit(Object value) {
    // emit to context
    try {
      outputWriter.write(stageName, (KeyValue<Object, Object>) value);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    super.emitError(invalidEntry);
  }
}
