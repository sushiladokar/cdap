package co.cask.cdap.etl.common;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ETLMapReduceTransformExecutor<IN> {
  private final Set<String> startingPoints;
  private final Map<String, ETLMapReduceTransformDetail> transformDetailMap;

  public ETLMapReduceTransformExecutor(Map<String, ETLMapReduceTransformDetail> transformDetailMap,
                                       Set<String> startingPoints) {
    this.transformDetailMap = transformDetailMap;
    this.startingPoints = startingPoints;
  }

  public void runOneIteration(IN input) throws Exception {
    for (String stageName : startingPoints) {
      executeTransformation(stageName, ImmutableList.of(input));
    }
  }

  private <T> void executeTransformation(final String stageName, Collection<T> input) throws Exception {
    if (input == null) {
      return;
    }

    ETLMapReduceTransformDetail transformDetail = transformDetailMap.get(stageName);
    for (T inputEntry : input) {
      transformDetail.process(inputEntry);
    }
  }
}
