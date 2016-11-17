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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchEmitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.batch.mapreduce.BatchTransformExecutor;
import co.cask.cdap.etl.batch.mapreduce.ErrorOutputWriter;
import co.cask.cdap.etl.batch.mapreduce.OutputWriter;
import co.cask.cdap.etl.batch.mapreduce.SinkEmitter;
import co.cask.cdap.etl.batch.mapreduce.TransformEmitter;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public abstract class TransformExecutorFactory<T> {
  protected final Map<String, Map<String, Schema>> perStageInputSchemas;
  private final String sourceStageName;
  private final MacroEvaluator macroEvaluator;
  protected final PipelinePluginInstantiator pluginInstantiator;
  protected final Metrics metrics;
  protected Schema outputSchema;
  protected boolean isMapPhase;

  public TransformExecutorFactory(JobContext hadoopContext, PipelinePluginInstantiator pluginInstantiator,
                                  Metrics metrics, @Nullable String sourceStageName, MacroEvaluator macroEvaluator) {
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
    this.perStageInputSchemas = new HashMap<>();
    this.outputSchema = null;
    this.sourceStageName = sourceStageName;
    this.macroEvaluator = macroEvaluator;
    this.isMapPhase = hadoopContext instanceof Mapper.Context;
  }

  protected abstract BatchRuntimeContext createRuntimeContext(String stageName);

  protected TrackedTransform getTransformation(String pluginType, String stageName) throws Exception {
    return new TrackedTransform(KVTransformations.getKVTransformation(stageName, pluginType,
                                                                     isMapPhase,
                                                                     getInitializedTransformation(stageName)),
                                new DefaultStageMetrics(metrics, stageName));
  }

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public <KEY_OUT, VAL_OUT> BatchTransformExecutor<T> create(PipelinePhase pipeline,
                                                             OutputWriter<KEY_OUT, VAL_OUT> outputWriter,
                                                             Map<String, ErrorOutputWriter<Object, Object>>
                                                               transformErrorSinkMap)
    throws Exception {
    Map<String, BatchTransformDetail> transformations = new HashMap<>();
    Set<String> sinks = pipeline.getSinks();

    for (String sink : sinks) {
      transformations.put(sink, new BatchTransformDetail(getTransformation(BatchSink.PLUGIN_TYPE, sink), new
        SinkEmitter<>(sink, outputWriter)));
    }

    // recursively set ETLTransformDetail for all the stages
    for (String sink : sinks) {
      StageInfo stageInfo = pipeline.getStage(sink);
      setETLTransformDetail(pipeline, stageInfo, transformations, transformErrorSinkMap);
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new BatchTransformExecutor<>(transformations, startingPoints);
  }

  private void setETLTransformDetail(PipelinePhase pipeline, StageInfo stageInfo,
                                     Map<String, BatchTransformDetail> transformations,
                                     Map<String, ErrorOutputWriter<Object, Object>> transformErrorSinkMap)
    throws Exception {
    String stageName = stageInfo.getName();
    BatchTransformDetail etlTransformDetail = transformations.get(stageName);

    for (String input : stageInfo.getInputs()) {
      BatchTransformDetail inputEtlTransformDetail;
      if (transformations.containsKey(input)) {
        inputEtlTransformDetail = transformations.get(input);
        BatchEmitter<BatchTransformDetail> emitter = inputEtlTransformDetail.getEmitter();
        Map<String, BatchTransformDetail> nextStages = emitter.getNextStages();

        if (nextStages != null && !nextStages.containsKey(stageName)) {
          emitter.addTransformDetail(stageName, etlTransformDetail);
        }
      } else {
        StageInfo inputStage = pipeline.getStage(input);
        HashMap<String, BatchTransformDetail> map = new HashMap<>();
        map.put(stageName, etlTransformDetail);

        if (transformErrorSinkMap.containsKey(input)) {
          transformations.put(input,
                              new BatchTransformDetail(getTransformation(inputStage.getPluginType(), input),
                                                       new TransformEmitter(input, map,
                                                                            transformErrorSinkMap.get(input))));
        } else {
          transformations.put(input,
                              new BatchTransformDetail(getTransformation(inputStage.getPluginType(), input),
                                                       new TransformEmitter(input, map,
                                                                            transformErrorSinkMap.get(input))));
        }

      }
      setETLTransformDetail(pipeline, pipeline.getStage(input), transformations, transformErrorSinkMap);
    }
  }

  /**
   * Instantiates and initializes the plugin for the stage.
   *
   * @param stageName the stage name.
   * @return the initialized Transformation
   * @throws InstantiationException if the plugin for the stage could not be instantiated
   * @throws Exception              if there was a problem initializing the plugin
   */
  protected <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(String stageName) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageName);
    T plugin = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
    plugin.initialize(runtimeContext);
    return plugin;
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedEmitKeyStep(Transformation<IN, OUT> transform,
                                                                             StageMetrics stageMetrics) {
    return new TrackedTransform<>(transform, stageMetrics, TrackedTransform.RECORDS_IN, null);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedAggregateStep(Transformation<IN, OUT> transform,
                                                                               StageMetrics stageMetrics) {
    // 'aggregator.groups' is the number of groups output by the aggregator
    return new TrackedTransform<>(transform, stageMetrics, "aggregator.groups", TrackedTransform.RECORDS_OUT);
  }

  protected static <IN, OUT> TrackedTransform<IN, OUT> getTrackedMergeStep(Transformation<IN, OUT> transform,
                                                                           StageMetrics stageMetrics) {
    return new TrackedTransform<>(transform, stageMetrics, null, TrackedTransform.RECORDS_OUT);
  }
}
