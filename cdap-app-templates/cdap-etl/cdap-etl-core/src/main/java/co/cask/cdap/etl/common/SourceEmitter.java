package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transformation;

import java.util.Map;

/**
 *
 */
public class SourceEmitter<T> implements Emitter<T> {
  private final Map<String, Transformation>


  @Override
  public void emit(T value) {
    // for all the next stages, call stage.transform(value, )

  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {

  }
}
