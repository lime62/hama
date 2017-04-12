/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.sync.SyncException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.List;

public class DataflowBSP<K1 extends WritableComparable<?>, V1 extends Writable,
    K2 extends WritableComparable<?>, V2 extends Writable, M extends Writable> extends BSP<K1, V1, K2, V2, M> {

  private static final Log LOG = LogFactory.getLog(DataflowBSP.class);
  private DataflowSuperstep<K1, V1, K2, V2, M>[] supersteps;
  private int startingSuperstep;

  @SuppressWarnings("unchecked")
  @Override
  public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {

    try {
      // deserialize superstep objects
      String tempPath = peer.getConfiguration().get("hama.dataflow.tempPath");
      FileInputStream fis = new FileInputStream(tempPath);
      ObjectInput oi = new ObjectInputStream(fis);
      List<DataflowSuperstep> superstepList = (List<DataflowSuperstep>)oi.readObject();
      oi.close();
      fis.close();

      // initiate supersteps
      supersteps = new DataflowSuperstep[superstepList.size()];
      for (int i = 0; i < superstepList.size(); i++) {
        DataflowSuperstep dss = superstepList.get(i);
        dss.setup(peer);
        supersteps[i] = dss;
      }
    } catch(ClassNotFoundException e) {
      LOG.error("Could not instantiate a DataflowSuperstep objects.", e);
      throw new IOException(e);
    }
    startingSuperstep = peer.getConfiguration().getInt("attempt.superstep", 0);
  }

  @Override
  public void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException, SyncException, InterruptedException {
    for (int i = startingSuperstep; i < supersteps.length; i++) {
      DataflowSuperstep<K1, V1, K2, V2, M> superstep = supersteps[i];
      superstep.compute(peer);
      if (superstep.haltComputation(peer)) {
        LOG.info("Superstep computation is halt.");
        break;
      }
      peer.sync();
      startingSuperstep = 0;
    }
  }
}
