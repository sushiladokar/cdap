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

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.batch.ETLTransformDetail;

import java.util.Map;

/**
 *
 * @param <T>
 */
public class ETLEmitter<T> implements Emitter<T> {
  private final Map<String, ETLTransformDetail> nextStages;

  public ETLEmitter(Map<String, ETLTransformDetail> nextStages) {
    this.nextStages = nextStages;
  }

  @Override
  public void emit(T value) {
    for (ETLTransformDetail etlTransformDetail : nextStages.values()) {
      try {
        etlTransformDetail.process(value);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {

  }

  public void addTransformDetail(String stageName, ETLTransformDetail etlTransformDetail) {
    nextStages.put(stageName, etlTransformDetail);
  }

  public Map<String, ETLTransformDetail> getNextStages() {
    return nextStages;
  }
}
