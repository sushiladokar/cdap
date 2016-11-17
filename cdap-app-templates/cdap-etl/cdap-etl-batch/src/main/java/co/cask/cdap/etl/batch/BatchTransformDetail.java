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

import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchEmitter;


/**
 *
 */
public class BatchTransformDetail {
  private final Transformation transformation;
  private final BatchEmitter<BatchTransformDetail> emitter;

  public BatchTransformDetail(Transformation transformation, BatchEmitter<BatchTransformDetail> emitter) {
    this.transformation = transformation;
    this.emitter = emitter;
  }

  public void process(Object value) {
    try {
      transformation.transform(value, emitter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public BatchEmitter<BatchTransformDetail> getEmitter() {
    return emitter;
  }

  public Transformation getTransformation() {
    return transformation;
  }
}
