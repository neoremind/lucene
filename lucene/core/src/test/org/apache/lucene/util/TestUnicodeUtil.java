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

/*
 * Some of this code came from the excellent Unicode
 * conversion examples from:
 *
 *   http://www.unicode.org/Public/PROGRAMS/CVTUTF
 *
 * Full Copyright for that code follows:
 */

/*
 * Copyright 2001-2004 Unicode, Inc.
 *
 * Disclaimer
 *
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 *
 * Limitations on Rights to Redistribute This Code
 *
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

/*
 * Additional code came from the IBM ICU library.
 *
 *  http://www.icu-project.org
 *
 * Full Copyright for that code follows.
 */

/*
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * provided that the above copyright notice(s) and this permission notice appear
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

package org.apache.lucene.util;

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;

public class TestUnicodeUtil extends LuceneTestCase {
  public void testCodePointCount() {
    // Check invalid codepoints.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0x80, 'z', 'z', 'z'));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xc0 - 1, 'z', 'z', 'z'));
    // Check 5-byte and longer sequences.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf8, 'z', 'z', 'z'));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xfc, 'z', 'z', 'z'));
    // Check improperly terminated codepoints.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xc2));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xe2));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xe2, 0x82));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0, 0xa4));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0, 0xa4, 0xad));

    // Check some typical examples (multibyte).
    assertEquals(0, UnicodeUtil.codePointCount(newBytesRef(asByteArray())));
    assertEquals(3, UnicodeUtil.codePointCount(newBytesRef(asByteArray('z', 'z', 'z'))));
    assertEquals(2, UnicodeUtil.codePointCount(newBytesRef(asByteArray('z', 0xc2, 0xa2))));
    assertEquals(2, UnicodeUtil.codePointCount(newBytesRef(asByteArray('z', 0xe2, 0x82, 0xac))));
    assertEquals(
        2, UnicodeUtil.codePointCount(newBytesRef(asByteArray('z', 0xf0, 0xa4, 0xad, 0xa2))));

    // And do some random stuff.
    int num = atLeast(50000);
    for (int i = 0; i < num; i++) {
      final String s = TestUtil.randomUnicodeString(random());
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(s.length())];
      final int utf8Len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8);
      assertEquals(
          s.codePointCount(0, s.length()),
          UnicodeUtil.codePointCount(newBytesRef(utf8, 0, utf8Len)));
    }
  }

  private byte[] asByteArray(int... ints) {
    byte[] asByteArray = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      asByteArray[i] = (byte) ints[i];
    }
    return asByteArray;
  }

  private void assertcodePointCountThrowsAssertionOn(byte... bytes) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          UnicodeUtil.codePointCount(newBytesRef(bytes));
        });
  }

  public void testUTF8toUTF32() {
    int[] utf32 = new int[0];
    int num = atLeast(50000);
    for (int i = 0; i < num; i++) {
      final String s = TestUtil.randomUnicodeString(random());
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(s.length())];
      final int utf8Len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8);
      utf32 = ArrayUtil.grow(utf32, utf8Len);
      final int utf32Len = UnicodeUtil.UTF8toUTF32(newBytesRef(utf8, 0, utf8Len), utf32);

      int[] codePoints = s.codePoints().toArray();
      if (!Arrays.equals(codePoints, 0, codePoints.length, utf32, 0, codePoints.length)) {
        System.out.println("FAILED");
        for (int j = 0; j < s.length(); j++) {
          System.out.println("  char[" + j + "]=" + Integer.toHexString(s.charAt(j)));
        }
        System.out.println();
        assertEquals(codePoints.length, utf32Len);
        for (int j = 0; j < codePoints.length; j++) {
          System.out.println(
              "  " + Integer.toHexString(utf32[j]) + " vs " + Integer.toHexString(codePoints[j]));
        }
        fail("mismatch");
      }
    }
  }

  public void testUTF8CodePointAt() {
    int num = atLeast(50000);
    UnicodeUtil.UTF8CodePoint reuse = null;
    for (int i = 0; i < num; i++) {
      final String s = TestUtil.randomUnicodeString(random());
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(s.length())];
      final int utf8Len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8);

      int[] expected = s.codePoints().toArray();
      int pos = 0;
      int expectedUpto = 0;
      while (pos < utf8Len) {
        reuse = UnicodeUtil.codePointAt(utf8, pos, reuse);
        assertEquals(expected[expectedUpto], reuse.codePoint);
        expectedUpto++;
        pos += reuse.numBytes;
      }
      assertEquals(utf8Len, pos);
      assertEquals(expected.length, expectedUpto);
    }
  }

  public void testUTF8SpanMultipleBytes() throws Exception {
    Automaton.Builder b = new Automaton.Builder();
    // start state:
    int s1 = b.createState();

    // single end accept state:
    int s2 = b.createState();
    b.setAccept(s2, true);

    // utf8 codepoint length range from [1,2]
    b.addTransition(s1, s2, 0x7F, 0x80);
    // utf8 codepoint length range from [2,3]
    b.addTransition(s1, s2, 0x7FF, 0x800);
    // utf8 codepoint length range from [3,4]
    b.addTransition(s1, s2, 0xFFFF, 0x10000);

    Automaton a = b.finish();

    CompiledAutomaton c = new CompiledAutomaton(a);
    FiniteStringsIterator it = new FiniteStringsIterator(c.automaton);
    int termCount = 0;
    for (IntsRef r = it.next(); r != null; r = it.next()) {
      termCount++;
    }
    assertEquals(6, termCount);
  }

  public void testNewString() {
    final int[] codePoints = {
      Character.toCodePoint(Character.MIN_HIGH_SURROGATE, Character.MAX_LOW_SURROGATE),
      Character.toCodePoint(Character.MAX_HIGH_SURROGATE, Character.MIN_LOW_SURROGATE),
      Character.MAX_HIGH_SURROGATE,
      'A',
      -1,
    };

    final String cpString =
        ""
            + Character.MIN_HIGH_SURROGATE
            + Character.MAX_LOW_SURROGATE
            + Character.MAX_HIGH_SURROGATE
            + Character.MIN_LOW_SURROGATE
            + Character.MAX_HIGH_SURROGATE
            + 'A';

    final int[][] tests = {
      {0, 1, 0, 2},
      {0, 2, 0, 4},
      {1, 1, 2, 2},
      {1, 2, 2, 3},
      {1, 3, 2, 4},
      {2, 2, 4, 2},
      {2, 3, 0, -1},
      {4, 5, 0, -1},
      {3, -1, 0, -1}
    };

    for (int i = 0; i < tests.length; ++i) {
      int[] t = tests[i];
      int s = t[0];
      int c = t[1];
      int rs = t[2];
      int rc = t[3];

      try {
        String str = UnicodeUtil.newString(codePoints, s, c);
        assertFalse(rc == -1);
        assertEquals(cpString.substring(rs, rs + rc), str);
        continue;
      } catch (IndexOutOfBoundsException | IllegalArgumentException _) {
        // Ignored.
      }
      assertTrue(rc == -1);
    }
  }

  public void testUTF8UTF16CharsRef() {
    int num = atLeast(3989);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomRealisticUnicodeString(random());
      BytesRef ref = newBytesRef(unicode);
      CharsRefBuilder cRef = new CharsRefBuilder();
      cRef.copyUTF8Bytes(ref);
      assertEquals(cRef.toString(), unicode);
    }
  }

  public void testCalcUTF16toUTF8Length() {
    int num = atLeast(5000);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomUnicodeString(random());
      byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(unicode.length())];
      int len = UnicodeUtil.UTF16toUTF8(unicode, 0, unicode.length(), utf8);
      assertEquals(len, UnicodeUtil.calcUTF16toUTF8Length(unicode, 0, unicode.length()));
    }
  }

  public void testCalcVIntSizeForUTF8Length() {
    // Test against the ground truth: compute actual UTF-8 length, then count VInt bytes
    int num = atLeast(50000);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomUnicodeString(random());
      int charCount = unicode.length();
      int actualByteLen = UnicodeUtil.calcUTF16toUTF8Length(unicode, 0, charCount);
      int expectedVIntSize = BitUtil.vIntSize(actualByteLen);
      int computedVIntSize = UnicodeUtil.calcVIntSizeForUTF8Length(unicode, 0, charCount);
      assertEquals(
          "charCount=" + charCount + ", byteLen=" + actualByteLen,
          expectedVIntSize,
          computedVIntSize);
    }
  }

  public void testCalcVIntSizeForUTF8LengthBoundaries() {
    // === 1-byte VInt boundary (byteLen 0–127) ===

    // charCount <= 42, 1-byte VInt
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("", 0, 0));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a", 0, 1));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(42), 0, 42));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(42), 0, 42));

    // charCount 43 with all ASCII, byteLen=43, 1-byte VInt
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(43), 0, 43));

    // charCount 43 with all 3-byte chars, byteLen=129, 2-byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(43), 0, 43));

    // 42 ASCII + 1 CJK = 43 chars, byteLen=42+3=45 < 128, 1-byte VInt
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(42) + "\u4e00", 0, 43));

    // 40 ASCII + 30 latin-ext (2-byte) = 70 chars, byteLen=40+60=100 < 128, 1-byte VInt
    assertEquals(
        1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(40) + "\u00e9".repeat(30), 0, 70));

    // 127 ASCII chars, byteLen=127, 1-byte VInt
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(127), 0, 127));

    // 126 ASCII + 1 latin-ext = 127 chars, byteLen=126+2=128, 2-byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(126) + "\u00e9", 0, 127));

    // === 2-byte VInt boundary (byteLen 128–16383) ===

    // charCount 128, 2-byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(128), 0, 128));
    assertEquals(
        2, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(128), 0, 128)); // byteLen=384

    // charCount 5461, max byteLen = 5461*3 = 16383, 2-byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(5461), 0, 5461));
    assertEquals(
        2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(5461), 0, 5461)); // byteLen=5461

    // All CJK, byteLen = 5462*3 = 16386 > 16383, 3-byte VInt
    assertEquals(3, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(5462), 0, 5462));
    // All ASCII: byteLen = 5462, 2 byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(5462), 0, 5462));

    // 5000 ASCII + 462 CJK = 5462 chars, byteLen=5000+1386=6386, 2-byte VInt
    assertEquals(
        2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(5000) + "\u4e00".repeat(462), 0, 5462));

    // 1000 ASCII + 5128 CJK = 6128 chars, byteLen=1000+15384=16384, 3-byte VInt
    assertEquals(
        3,
        UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(1000) + "\u4e00".repeat(5128), 0, 6128));
    // 1000 ASCII + 5128 CJK + 1 latin-ext = 6129 chars, byteLen=1000+15384+2=16386, 3-byte VInt
    assertEquals(
        3,
        UnicodeUtil.calcVIntSizeForUTF8Length(
            "a".repeat(999) + "\u4e00".repeat(5128) + "\u00e9".repeat(1), 0, 6128));
    // 1002 ASCII + 5127 CJK = 6129 chars, byteLen=1002+15381=16383, 2-byte VInt
    assertEquals(
        2,
        UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(1002) + "\u4e00".repeat(5127), 0, 6129));

    // === 3-byte VInt boundary (byteLen 16384–2097151) ===

    // charCount 16384: byteLen >= 16384 guaranteed, 3-byte VInt
    assertEquals(3, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(16384), 0, 16384));

    // === Surrogate pairs ===

    // 32 surrogate pairs = 64 chars, byteLen=128, 2-byte VInt
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 64));

    // 31 surrogate pairs = 62 chars, byteLen=124, 1-byte VInt
    sb = new StringBuilder();
    for (int i = 0; i < 31; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 62));

    // Mix: 30 ASCII + 25 surrogate pairs (50 chars) = 80 chars, byteLen=30+100=130, 2-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(30));
    for (int i = 0; i < 25; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 80));

    // Mix: 120 ASCII + 3 surrogate pairs (6 chars) = 126 chars, byteLen=120+12=132, 2-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(120));
    for (int i = 0; i < 3; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 126));

    // Mix: 110 ASCII + 2 surrogate pairs (4 chars) + 9 ASCII = 123 chars, byteLen=110+8+9=127,
    // 1-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(110));
    for (int i = 0; i < 2; i++) {
      sb.append("\ud83d\ude00");
    }
    sb.append("a".repeat(9));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 123));

    // Mix: 110 ASCII + 2 surrogate pairs (4 chars) + 10 ASCII = 124 chars, byteLen=110+8+10=128,
    // 2-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(110));
    for (int i = 0; i < 2; i++) {
      sb.append("\ud83d\ude00");
    }
    sb.append("a".repeat(10));
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 124));

    // Mix: 123 ASCII + 2 surrogate pairs (4 chars) = 127 chars, byteLen=123+8=131, 2-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(123));
    for (int i = 0; i < 2; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 127));

    // Mix: 125 ASCII + 1 surrogate pair (2 chars) = 127 chars, byteLen=125+4=129, 2-byte VInt
    sb = new StringBuilder();
    sb.append("a".repeat(125));
    sb.append("\ud83d\ude00");
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 127));

    // === Latin extended (2-byte) ===

    // 64 latin-ext chars = 64 chars, byteLen=128, 2-byte VInt
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("\u00e9".repeat(64), 0, 64));

    // 63 latin-ext chars = 63 chars, byteLen=126, 1-byte VInt
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("\u00e9".repeat(63), 0, 63));

    // 50 ASCII + 39 latin-ext = 89 chars, byteLen=50+78=128, 2-byte VInt
    assertEquals(
        2, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(50) + "\u00e9".repeat(39), 0, 89));

    // === Unpaired/corrupt surrogates ===

    // Orphan high surrogate at the end: 43 chars total, last is unpaired high surrogate
    // 42 ASCII + 1 orphan high surrogate = 43 chars, byteLen=42+3=45, 1-byte VInt
    assertEquals(45, UnicodeUtil.calcUTF16toUTF8Length("a".repeat(42) + "\ud800", 0, 43));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(42) + "\ud800", 0, 43));

    // Orphan low surrogate: treated as replacement char (3 bytes)
    // 42 ASCII + 1 orphan low surrogate = 43 chars, byteLen=42+3=45, 1-byte VInt
    assertEquals(45, UnicodeUtil.calcUTF16toUTF8Length("a".repeat(42) + "\udc00", 0, 43));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(42) + "\udc00", 0, 43));

    // Two consecutive high surrogates (invalid pair)
    // 40 ASCII + 2 unpaired high surrogates = 42 chars, byteLen=40+6=46, 1-byte VInt
    assertEquals(46, UnicodeUtil.calcUTF16toUTF8Length("a".repeat(40) + "\ud800\ud800", 0, 42));
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length("a".repeat(40) + "\ud800\ud800", 0, 42));

    // 42 CJK + 1 orphan high surrogate = 43 chars, byteLen=126+3=129, 2-byte VInt
    assertEquals(129, UnicodeUtil.calcUTF16toUTF8Length("\u4e00".repeat(42) + "\ud800", 0, 43));
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(42) + "\ud800", 0, 43));

    // High surrogate followed by non-low-surrogate (invalid pair)
    // 41 CJK + high surrogate + ASCII = 43 chars, byteLen=123+3+1=127, 1-byte VInt
    assertEquals(
        127, UnicodeUtil.calcUTF16toUTF8Length("\u4e00".repeat(41) + "\ud800" + "a", 0, 43));
    assertEquals(
        1, UnicodeUtil.calcVIntSizeForUTF8Length("\u4e00".repeat(41) + "\ud800" + "a", 0, 43));
  }

  public void testCalcVIntSizeForUTF8LengthWithSurrogates() {
    // 21 surrogate pairs = 42 chars, byteLen = 84, 1-byte VInt
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 21; i++) {
      sb.append("\ud83d\ude00"); // 😀
    }
    assertEquals(42, sb.length());
    assertEquals(1, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 42));

    // 32 pairs = 64 chars, byteLen = 128, 2-byte VInt
    sb = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(64, sb.length());
    assertEquals(128, UnicodeUtil.calcUTF16toUTF8Length(sb.toString(), 0, 64));
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 64));

    // 4096 pairs = 8192 chars, byteLen = 16384, 3-byte VInt
    sb = new StringBuilder();
    for (int i = 0; i < 4096; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(8192, sb.length());
    assertEquals(16384, UnicodeUtil.calcUTF16toUTF8Length(sb.toString(), 0, 8192));
    assertEquals(3, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 8192));

    // 4095 pairs = 8190 chars, byteLen = 16380, 2-byte VInt
    sb = new StringBuilder();
    for (int i = 0; i < 4095; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(8190, sb.length());
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 8190));

    // 3000 ASCII + 1096 pairs (2192 chars) = 5192 chars, byteLen = 3000 + 4384 = 7384, VInt 2 bytes
    sb = new StringBuilder();
    sb.append("a".repeat(3000));
    for (int i = 0; i < 1096; i++) {
      sb.append("\ud83d\ude00");
    }
    assertEquals(5192, sb.length());
    assertEquals(2, UnicodeUtil.calcVIntSizeForUTF8Length(sb.toString(), 0, 5192));
  }
}
