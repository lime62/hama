/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama;

import org.apache.hadoop.io.Text;

public class TestFeatureVector extends HamaTestCase {
  public void testAddition() {
    matrixA = new Matrix(conf, A);
    matrixA.set(0, 0, 2);
    matrixA.set(0, 1, 5);

    matrixB = new Matrix(conf, B);
    matrixB.set(0, 0, 4);
    matrixB.set(0, 1, 1);

    FeatureVector v1 = new FeatureVector(matrixA.getRowResult(0));
    FeatureVector v2 = new FeatureVector(matrixB.getRowResult(0));

    FeatureVector v3 = v1.addition(v2);
    assertTrue(v3.getValueAt(0) == 6.0);
    assertTrue(v3.getValueAt(1) == 6.0);

    LOG.info(v3.getValueAt(0));
    LOG.info(v3.getValueAt(1));
    matrixA.clear();
    matrixB.clear();
  }

  /**
   * Test cosine similarity
   */
  public void testCosine() {
    final double result = 0.6978227007909176;
    Matrix m1 = new Matrix(conf, new Text("cosine"));

    // TODO : We need setArray(int row, double[] value) to matrix
    // e.g. matrixA.setArray(0, new double[] {2,5,1,4});
    // -- Edward

    m1.set(0, 0, 2);
    m1.set(0, 1, 5);
    m1.set(0, 2, 1);
    m1.set(0, 3, 4);

    m1.set(1, 0, 4);
    m1.set(1, 1, 1);
    m1.set(1, 2, 3);
    m1.set(1, 3, 3);

    LOG.info("get test : " + m1.get(0, 0));
    LOG.info("get test : " + m1.get(0, 1));

    FeatureVector v1 = new FeatureVector(m1.getRowResult(0));
    FeatureVector v2 = new FeatureVector(m1.getRowResult(1));

    double cos = v1.getCosine(v2);
    assertEquals(cos, result);
    m1.close();
  }
}
