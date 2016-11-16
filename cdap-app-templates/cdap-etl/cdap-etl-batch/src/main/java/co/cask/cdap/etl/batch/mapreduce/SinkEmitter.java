package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.batch.ETLTransformDetail;

import java.util.HashMap;

/**
 *
 */
public class SinkEmitter< KEY_OUT, VAL_OUT> extends ETLEmitter<Object> {
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
      outputWriter.write(stageName, new KeyValue("test", value));
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    super.emitError(invalidEntry);
  }
}
