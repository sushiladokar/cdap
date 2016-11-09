package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;

import java.util.Map;

/**
 * Emitter for source and transforms
 */
public class ETLEmitter<T> implements Emitter<T> {
  private final Map<String, ETLMapReduceTransformDetail> nextTransformations;

  public ETLEmitter(Map<String, ETLMapReduceTransformDetail> nextTransformations) {
    this.nextTransformations = nextTransformations;
  }

  @Override
  public void emit(T value) {
    for (Map.Entry<String, ETLMapReduceTransformDetail> entry : nextTransformations.entrySet()) {
      ETLMapReduceTransformDetail transformDetail = entry.getValue();
      try {
        transformDetail.process(value);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // TODO Handle Errors
  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {

  }
}
