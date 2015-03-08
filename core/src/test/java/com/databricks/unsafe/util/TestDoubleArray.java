/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.unsafe.util;

import org.junit.Assert;
import org.junit.Test;

import com.databricks.unsafe.util.memory.MemoryBlock;

public class TestDoubleArray {

  private DoubleArray createTestData() {
    byte[] bytes = new byte[16];
    DoubleArray arr = new DoubleArray(MemoryBlock.fromByteArray(bytes));
    arr.set(0, 1.0);
    arr.set(1, 2.0);
    arr.set(1, 3.0);
    return arr;
  }

  @Test
  public void basicTest() {
    DoubleArray arr = createTestData();
    Assert.assertEquals(2, arr.size());
    Assert.assertEquals(1.0, arr.get(0), 0.00000000001);
    Assert.assertEquals(3.0, arr.get(1), 0.00000000001);
  }

  @Test
  public void toJvmArray() {
    DoubleArray arr = createTestData();
    double[] expected = {1.0, 3.0};
    Assert.assertArrayEquals(expected, arr.toJvmArray(), 0.00000000001);
  }
}
