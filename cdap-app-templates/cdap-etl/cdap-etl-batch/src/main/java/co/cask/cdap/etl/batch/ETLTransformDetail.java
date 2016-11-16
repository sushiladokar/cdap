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
import co.cask.cdap.etl.batch.mapreduce.ETLEmitter;


/**
 *
 */
public class ETLTransformDetail {
  private final Transformation transformation;
  private final ETLEmitter<Object> emitter;

  public ETLTransformDetail(Transformation transformation, ETLEmitter<Object> emitter) {
    this.transformation = transformation;
    this.emitter = emitter;
  }

  public void process(Object value) throws Exception {
    transformation.transform(value, emitter);
  }

  public ETLEmitter<Object> getEmitter() {
    return emitter;
  }

  public Transformation getTransformation() {
    return transformation;
  }
}
