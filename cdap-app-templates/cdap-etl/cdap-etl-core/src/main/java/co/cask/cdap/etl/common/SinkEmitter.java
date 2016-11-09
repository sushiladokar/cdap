package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;

/**
 * Emitter for Sink
 */
public class SinkEmitter<T> implements Emitter<T> {
  private final OutputWriter<Object, Object> outputWriter;

  @Override
  public void emit(T value) {
    outputWriter.write();
  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {

  }
}
