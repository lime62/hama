package org.apache.hama.bsp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.ReflectionUtils;

import java.io.IOException;

public class DataflowBSP extends BSP<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> {

  private static final Log LOG = LogFactory.getLog(DataflowBSP.class);
  private Superstep<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable>[] supersteps;
  private int startSuperstep;

  private WritableComparable<?> keyIn;
  private Writable valueIn;
  private WritableComparable<?> keyOut;
  private Writable valueOut;

  public static final String KEY_IN_CLASS_NAME = "hama.dataflow.key.input.class";
  public static final String VALUE_IN_CLASS_NAME = "hama.dataflow.value.input.class";
  public static final String KEY_OUT_CLASS_NAME = "hama.dataflow.key.output.class";
  public static final String VALUE_OUT_CLASS_NAME = "hama.dataflow.value.output.class";

  @SuppressWarnings("unchecked")
  @Override
  public void setup(BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> peer) throws IOException,
      SyncException, InterruptedException {
    try {
      keyIn = ReflectionUtils.newInstance(peer.getConfiguration().get(KEY_IN_CLASS_NAME));
      valueIn = ReflectionUtils.newInstance(peer.getConfiguration().get(VALUE_IN_CLASS_NAME));
      keyOut = ReflectionUtils.newInstance(peer.getConfiguration().get(KEY_OUT_CLASS_NAME));
      valueOut = ReflectionUtils.newInstance(peer.getConfiguration().get(VALUE_OUT_CLASS_NAME));
    } catch (ClassNotFoundException e) {
      LOG.error(e);
      throw new IOException(e);
    }

    // instantiate our superstep classes
    /*
    String classList = peer.getConfiguration().get("hama.dataflow.supersteps.class");
    String[] classNames = classList.split(",");

    LOG.debug("Size of classes = " + classNames.length);

    supersteps = new Superstep[classNames.length];
    Superstep<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> newInstance;
    for (int i = 0; i < classNames.length; i++) {
      try {
        newInstance = ReflectionUtils.newInstance(classNames[i]);
      } catch (ClassNotFoundException e) {
        LOG.error((new StringBuffer("Could not instantiate a Superstep class")
            .append(classNames[i])).toString(), e);
        throw new IOException(e);
      }
      newInstance.setup(peer);
      supersteps[i] = newInstance;
    }
    startSuperstep = peer.getConfiguration().getInt("attempt.superstep", 0);
    */
  }

  @Override
  public void bsp(BSPPeer<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> peer) throws IOException, SyncException, InterruptedException {
    for (int index = startSuperstep; index < supersteps.length; index++) {
      Superstep<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> superstep = supersteps[index];

      if (index == 0) {
        while (peer.readNext(keyIn, valueIn)) {
          // todo : add kvpair to superstep
        }
      } else {
        KVWritable msg;
        while ((msg = peer.getCurrentMessage()) != null) {
          // todo : add kvpair to superstep
        }
      }

      superstep.compute(peer);
      if (superstep.haltComputation(peer)) {
        break;
      }
      peer.sync();
      startSuperstep = 0;
    }
  }
}
