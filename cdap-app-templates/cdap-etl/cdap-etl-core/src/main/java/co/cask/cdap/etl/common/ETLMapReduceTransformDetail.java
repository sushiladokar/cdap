package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transformation;

/**
 *
 */
public class ETLMapReduceTransformDetail {
  private final Transformation transformation;
  private final Emitter<Object> emitter;

  public ETLMapReduceTransformDetail(Transformation transformation, Emitter<Object> emitter) {
    this.transformation = transformation;
    this.emitter = emitter;
  }

  public void destroy() {
    if (transformation instanceof Destroyable) {
      Destroyables.destroyQuietly((Destroyable) transformation);
    }
  }

  public Transformation getTransformation() {
    return transformation;
  }

  // process input
  public void process(Object value) throws Exception {
    transformation.transform(value, emitter);
  }
}
