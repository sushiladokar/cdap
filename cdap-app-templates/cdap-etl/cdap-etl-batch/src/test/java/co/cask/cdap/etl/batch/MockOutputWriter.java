package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.batch.mapreduce.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MockOutputWriter<KEY_OUT, VAL_OUT> extends OutputWriter<KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(MockOutputWriter.class);

  public MockOutputWriter(MapReduceTaskContext context) {
    super(context);
    LOG.info("MockOutputWriter created!");
  }

  @Override
  public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
    LOG.info("Writing from sink {}, key: ", sinkName);
  }
}
