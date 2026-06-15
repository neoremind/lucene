/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBitUtil extends LuceneTestCase {

  public void testVIntSize() {
    // 1-byte VInt: values 0–127
    assertEquals(1, BitUtil.vIntSize(0));
    assertEquals(1, BitUtil.vIntSize(1));
    assertEquals(1, BitUtil.vIntSize(127));

    // 2-byte VInt: values 128–16383
    assertEquals(2, BitUtil.vIntSize(128));
    assertEquals(2, BitUtil.vIntSize(16383));

    // 3-byte VInt: values 16384–2097151
    assertEquals(3, BitUtil.vIntSize(16384));
    assertEquals(3, BitUtil.vIntSize(2097151));

    // 4-byte VInt: values 2097152–268435455
    assertEquals(4, BitUtil.vIntSize(2097152));
    assertEquals(4, BitUtil.vIntSize(268435455));

    // 5-byte VInt: values 268435456+
    assertEquals(5, BitUtil.vIntSize(268435456));
    assertEquals(5, BitUtil.vIntSize(Integer.MAX_VALUE));

    for (int shift = 0; shift < 31; shift++) {
      int val = 1 << shift;
      int expected = (shift / 7) + 1;
      assertEquals("1<<" + shift, expected, BitUtil.vIntSize(val));
    }
  }

  public void testIsZeroOrPowerOfTwo() {
    assertTrue(BitUtil.isZeroOrPowerOfTwo(0));
    for (int shift = 0; shift <= 31; ++shift) {
      assertTrue(BitUtil.isZeroOrPowerOfTwo(1 << shift));
    }
    assertFalse(BitUtil.isZeroOrPowerOfTwo(3));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(5));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(6));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(7));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(9));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(Integer.MAX_VALUE));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(Integer.MAX_VALUE + 2));
    assertFalse(BitUtil.isZeroOrPowerOfTwo(-1));
  }
}
