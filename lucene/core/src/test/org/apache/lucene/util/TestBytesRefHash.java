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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.junit.Before;

public class TestBytesRefHash extends LuceneTestCase {

  BytesRefHash hash;
  ByteBlockPool pool;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    pool = newPool();
    hash = newHash(pool);
  }

  private ByteBlockPool newPool() {
    return random().nextBoolean() && pool != null
        ? pool
        : new ByteBlockPool(new RecyclingByteBlockAllocator(random().nextInt(25)));
  }

  private BytesRefHash newHash(ByteBlockPool blockPool) {
    final int initSize = 2 << 1 + random().nextInt(5);
    return random().nextBoolean()
        ? new BytesRefHash(blockPool)
        : new BytesRefHash(blockPool, initSize, new BytesRefHash.DirectBytesStartArray(initSize));
  }

  /** Test method for {@link org.apache.lucene.util.BytesRefHash#size()}. */
  public void testSize() {
    BytesRefBuilder ref = new BytesRefBuilder();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      final int mod = 1 + random().nextInt(39);
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        int count = hash.size();
        int key = hash.add(ref.get());
        if (key < 0) assertEquals(hash.size(), count);
        else assertEquals(hash.size(), count + 1);
        if (i % mod == 0) {
          hash.clear();
          assertEquals(0, hash.size());
          hash.reinit();
        }
      }
    }
  }

  /** Test method for {@link org.apache.lucene.util.BytesRefHash#get(int, BytesRef)} . */
  public void testGet() {
    BytesRefBuilder ref = new BytesRefBuilder();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Map<String, Integer> strings = new HashMap<>();
      int uniqueCount = 0;
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        int count = hash.size();
        int key = hash.add(ref.get());
        if (key >= 0) {
          assertNull(strings.put(str, Integer.valueOf(key)));
          assertEquals(uniqueCount, key);
          uniqueCount++;
          assertEquals(hash.size(), count + 1);
        } else {
          assertTrue((-key) - 1 < count);
          assertEquals(hash.size(), count);
        }
      }
      for (Entry<String, Integer> entry : strings.entrySet()) {
        ref.copyChars(entry.getKey());
        assertEquals(ref.get(), hash.get(entry.getValue().intValue(), scratch));
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  /** Test method for {@link org.apache.lucene.util.BytesRefHash#compact()}. */
  public void testCompact() {
    BytesRefBuilder ref = new BytesRefBuilder();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      int numEntries = 0;
      final int size = 797;
      BitSet bits = new BitSet(size);
      for (int i = 0; i < size; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        final int key = hash.add(ref.get());
        if (key < 0) {
          assertTrue(bits.get((-key) - 1));
        } else {
          assertFalse(bits.get(key));
          bits.set(key);
          numEntries++;
        }
      }
      assertEquals(hash.size(), bits.cardinality());
      assertEquals(numEntries, bits.cardinality());
      assertEquals(numEntries, hash.size());
      int[] compact = hash.compact();
      assertTrue(numEntries < compact.length);
      for (int i = 0; i < numEntries; i++) {
        bits.set(compact[i], false);
      }
      assertEquals(0, bits.cardinality());
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  /** Test method for {@link org.apache.lucene.util.BytesRefHash#sort()}. */
  public void testSort() {
    BytesRefBuilder ref = new BytesRefBuilder();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {

      // Sorts by unicode code point order (is there a simple way, e.g. a Collator?)
      SortedSet<String> strings = new TreeSet<>(TestUtil.STRING_CODEPOINT_COMPARATOR);
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        hash.add(ref.get());
        strings.add(str);
      }
      for (int iter = 0; iter < 3; iter++) {
        // Test duplicate sort on a BytesRefHash instance work well. This makes no sense but some
        // users need that.
        int[] sort = hash.sort();
        assertTrue(strings.size() < sort.length);
        int i = 0;
        BytesRef scratch = new BytesRef();
        for (String string : strings) {
          ref.copyChars(string);
          assertEquals(ref.get(), hash.get(sort[i++], scratch));
        }
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  /**
   * Test method for {@link
   * org.apache.lucene.util.BytesRefHash#add(org.apache.lucene.util.BytesRef)} .
   */
  public void testAdd() {
    BytesRefBuilder ref = new BytesRefBuilder();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Set<String> strings = new HashSet<>();
      int uniqueCount = 0;
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        int count = hash.size();
        int key = hash.add(ref.get());

        if (key >= 0) {
          assertTrue(strings.add(str));
          assertEquals(uniqueCount, key);
          assertEquals(hash.size(), count + 1);
          uniqueCount++;
        } else {
          assertFalse(strings.add(str));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }

      assertAllIn(strings, hash);
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  public void testFind() throws Exception {
    BytesRefBuilder ref = new BytesRefBuilder();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Set<String> strings = new HashSet<>();
      int uniqueCount = 0;
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        int count = hash.size();
        int key = hash.find(ref.get()); // hash.add(ref);
        if (key >= 0) { // string found in hash
          assertFalse(strings.add(str));
          assertTrue(key < count);
          assertEquals(str, hash.get(key, scratch).utf8ToString());
          assertEquals(count, hash.size());
        } else {
          key = hash.add(ref.get());
          assertTrue(strings.add(str));
          assertEquals(uniqueCount, key);
          assertEquals(hash.size(), count + 1);
          uniqueCount++;
        }
      }

      assertAllIn(strings, hash);
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  public void testConcurrentAccessToBytesRefHash() throws Exception {
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      int numStrings = 797;
      List<String> strings = new ArrayList<>(numStrings);
      for (int i = 0; i < numStrings; i++) {
        final String str = TestUtil.randomRealisticUnicodeString(random(), 1, 1000);
        hash.add(newBytesRef(str));
        assertTrue(strings.add(str));
      }
      int hashSize = hash.size();

      AtomicInteger notFound = new AtomicInteger();
      AtomicInteger notEquals = new AtomicInteger();
      AtomicInteger wrongSize = new AtomicInteger();
      int numThreads = atLeast(3);
      CountDownLatch latch = new CountDownLatch(numThreads);
      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < threads.length; i++) {
        int loops = atLeast(100);
        threads[i] =
            new Thread(
                () -> {
                  BytesRef scratch = new BytesRef();
                  latch.countDown();
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  for (int k = 0; k < loops; k++) {
                    BytesRef find = newBytesRef(strings.get(k % strings.size()));
                    int id = hash.find(find);
                    if (id < 0) {
                      notFound.incrementAndGet();
                    } else {
                      BytesRef get = hash.get(id, scratch);
                      if (!get.bytesEquals(find)) {
                        notEquals.incrementAndGet();
                      }
                    }
                    if (hash.size() != hashSize) {
                      wrongSize.incrementAndGet();
                    }
                  }
                },
                "t" + i);
      }

      for (Thread t : threads) t.start();
      for (Thread t : threads) t.join();

      assertEquals(0, notFound.get());
      assertEquals(0, notEquals.get());
      assertEquals(0, wrongSize.get());
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  public void testLargeValue() {
    int[] sizes =
        new int[] {random().nextInt(5), ByteBlockPool.BYTE_BLOCK_SIZE - 33 + random().nextInt(31)};
    BytesRef ref = new BytesRef();
    for (int i = 0; i < sizes.length; i++) {
      ref.bytes = new byte[sizes[i]];
      ref.offset = 0;
      ref.length = sizes[i];
      assertEquals(i, hash.add(ref));
    }

    ref.bytes = new byte[ByteBlockPool.BYTE_BLOCK_SIZE - 1 + random().nextInt(37)];
    ref.offset = 0;
    ref.length = ref.bytes.length;
    expectThrows(MaxBytesLengthExceededException.class, () -> hash.add(ref));
  }

  /** Test method for {@link org.apache.lucene.util.BytesRefHash#addByPoolOffset(int)} . */
  public void testAddByPoolOffset() {
    BytesRefBuilder ref = new BytesRefBuilder();
    BytesRef scratch = new BytesRef();
    BytesRefHash offsetHash = newHash(pool);
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Set<String> strings = new HashSet<>();
      int uniqueCount = 0;
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = TestUtil.randomRealisticUnicodeString(random(), 1000);
        } while (str.length() == 0);
        ref.copyChars(str);
        int count = hash.size();
        int key = hash.add(ref.get());

        if (key >= 0) {
          assertTrue(strings.add(str));
          assertEquals(uniqueCount, key);
          assertEquals(hash.size(), count + 1);
          int offsetKey = offsetHash.addByPoolOffset(hash.byteStart(key));
          assertEquals(uniqueCount, offsetKey);
          assertEquals(offsetHash.size(), count + 1);
          uniqueCount++;
        } else {
          assertFalse(strings.add(str));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
          int offsetKey = offsetHash.addByPoolOffset(hash.byteStart((-key) - 1));
          assertTrue((-offsetKey) - 1 < count);
          assertEquals(str, hash.get((-offsetKey) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }

      assertAllIn(strings, hash);
      for (String string : strings) {
        ref.copyChars(string);
        int key = hash.add(ref.get());
        BytesRef bytesRef = offsetHash.get((-key) - 1, scratch);
        assertEquals(ref.get(), bytesRef);
      }

      hash.clear();
      assertEquals(0, hash.size());
      offsetHash.clear();
      assertEquals(0, offsetHash.size());
      hash.reinit(); // init for the next round
      offsetHash.reinit();
    }
  }

  private void assertAllIn(Set<String> strings, BytesRefHash hash) {
    BytesRefBuilder ref = new BytesRefBuilder();
    BytesRef scratch = new BytesRef();
    int count = hash.size();
    for (String string : strings) {
      ref.copyChars(string);
      int key = hash.add(ref.get()); // add again to check duplicates
      assertEquals(string, hash.get((-key) - 1, scratch).utf8ToString());
      assertEquals(count, hash.size());
      assertTrue("key: " + key + " count: " + count + " string: " + string, key < count);
    }
  }

  public void testInlineEncodingRoundTrip() {
    // Exercise every length around the inline boundary, including bytes with the high bit set to
    // catch sign-extension bugs in encode/decode.
    byte[][] terms =
        new byte[][] {
          {},
          {0x00},
          {0x61},
          {(byte) 0x80},
          {(byte) 0xFF},
          {0x61, 0x62},
          {(byte) 0xFF, 0x00},
          {0x00, (byte) 0x80},
          {0x61, 0x62, 0x63},
          {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD},
          {0x00, 0x00, 0x00},
          {0x61, 0x62, 0x63, 0x64}, // first non-inline length
          {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        };
    int[] ids = new int[terms.length];
    for (int i = 0; i < terms.length; i++) {
      ids[i] = hash.add(new BytesRef(terms[i]));
      assertTrue("term " + i + " should be new", ids[i] >= 0);
    }
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < terms.length; i++) {
      assertEquals(
          "get() must round-trip term " + i, new BytesRef(terms[i]), hash.get(ids[i], scratch));
      assertEquals("find() must locate term " + i, ids[i], hash.find(new BytesRef(terms[i])));
      int dup = hash.add(new BytesRef(terms[i]));
      assertEquals("re-add must report duplicate for term " + i, -(ids[i] + 1), dup);
    }
    // similar but distinct short terms must not collide with each other
    assertEquals(-1, hash.find(new BytesRef(new byte[] {0x62})));
    assertEquals(-1, hash.find(new BytesRef(new byte[] {0x61, 0x62, 0x64})));
    assertEquals(-1, hash.find(new BytesRef(new byte[] {0x62, 0x62, 0x63, 0x64})));
  }

  public void testInlineEncodingIsCanonical() {
    // The optimization relies on encode being canonical (equal terms <-> equal ints) and always
    // producing a negative value distinct from any pool offset.
    byte[] term = new byte[BytesRefHash.MAX_INLINE_LENGTH];
    for (int len = 0; len <= BytesRefHash.MAX_INLINE_LENGTH; len++) {
      for (int iter = 0; iter < 1000; iter++) {
        random().nextBytes(term);
        BytesRef ref = new BytesRef(term, 0, len);
        int encoded = BytesRefHash.encodeInline(ref);
        assertTrue("inline encoding must be negative", BytesRefHash.isInline(encoded));
        assertEquals(encoded, BytesRefHash.encodeInline(ref.clone()));
        BytesRef decoded = new BytesRef();
        BytesRefHash.decodeInline(encoded, decoded, new byte[BytesRefHash.MAX_INLINE_LENGTH]);
        assertEquals(ref, decoded);
      }
    }
    // length is part of the encoding: all-zero terms of different lengths must encode differently
    Set<Integer> zeroTermEncodings = new HashSet<>();
    for (int len = 0; len <= BytesRefHash.MAX_INLINE_LENGTH; len++) {
      zeroTermEncodings.add(
          BytesRefHash.encodeInline(
              new BytesRef(new byte[BytesRefHash.MAX_INLINE_LENGTH], 0, len)));
    }
    assertEquals(BytesRefHash.MAX_INLINE_LENGTH + 1, zeroTermEncodings.size());
  }

  public void testSortShortTerms() {
    // Bias heavily toward terms at or below the inline boundary so the sort has to interleave
    // inline and pool entries correctly. Sort order is unicode code point order, matching UTF-8
    // byte order.
    SortedSet<String> expected = new TreeSet<>(TestUtil.STRING_CODEPOINT_COMPARATOR);
    BytesRefBuilder ref = new BytesRefBuilder();
    int numTerms = atLeast(1000);
    for (int i = 0; i < numTerms; i++) {
      String str;
      do {
        str = TestUtil.randomRealisticUnicodeString(random(), random().nextInt(6) == 0 ? 20 : 2);
      } while (str.length() == 0);
      ref.copyChars(str);
      hash.add(ref.get());
      expected.add(str);
    }
    int[] sort = hash.sort();
    assertEquals(expected.size(), hash.size());
    BytesRef scratch = new BytesRef();
    int i = 0;
    for (String string : expected) {
      ref.copyChars(string);
      assertEquals(ref.get(), hash.get(sort[i++], scratch));
    }
  }

  public void testAddByPoolOffsetWithInlineTerms() {
    // byteStart() of a short term returns a negative inline sentinel; addByPoolOffset must treat
    // it as an exact key, mirroring how term vectors reuse textStarts from the postings hash.
    BytesRefHash offsetHash = newHash(pool);
    List<BytesRef> terms = new ArrayList<>();
    BytesRefBuilder ref = new BytesRefBuilder();
    int numTerms = atLeast(200);
    for (int i = 0; i < numTerms; i++) {
      String str;
      do {
        str = TestUtil.randomRealisticUnicodeString(random(), random().nextBoolean() ? 1 : 20);
      } while (str.length() == 0);
      ref.copyChars(str);
      if (hash.add(ref.get()) >= 0) {
        terms.add(ref.toBytesRef());
      }
    }
    for (int id = 0; id < hash.size(); id++) {
      int offsetKey = offsetHash.addByPoolOffset(hash.byteStart(id));
      assertEquals("first add must create a new entry", id, offsetKey);
    }
    BytesRef scratch = new BytesRef();
    for (int id = 0; id < hash.size(); id++) {
      // re-adding the same offset must report the duplicate...
      assertEquals(-(id + 1), offsetHash.addByPoolOffset(hash.byteStart(id)));
      // ...and the offset hash must resolve to the identical term bytes
      assertEquals(terms.get(id), offsetHash.get(id, scratch));
    }
  }

  public void testInlineTermsSurviveRehash() {
    // Grow far past several rehash boundaries with a mix of short and long terms and verify
    // everything is still findable with correct bytes.
    Map<String, Integer> reference = new HashMap<>();
    BytesRefBuilder ref = new BytesRefBuilder();
    int numTerms = atLeast(10000);
    for (int i = 0; i < numTerms; i++) {
      String str;
      do {
        str = TestUtil.randomRealisticUnicodeString(random(), random().nextBoolean() ? 3 : 15);
      } while (str.length() == 0);
      ref.copyChars(str);
      int key = hash.add(ref.get());
      if (key >= 0) {
        assertNull(reference.put(str, key));
      } else {
        assertEquals(reference.get(str).intValue(), -(key + 1));
      }
    }
    BytesRef scratch = new BytesRef();
    for (Entry<String, Integer> entry : reference.entrySet()) {
      ref.copyChars(entry.getKey());
      assertEquals(entry.getValue().intValue(), hash.find(ref.get()));
      assertEquals(entry.getKey(), hash.get(entry.getValue(), scratch).utf8ToString());
    }
  }

  public void testGetRefsRemainValidAcrossCalls() {
    // Callers such as MultipassTermFilteredPresearcher#convertHash retain every ref returned by
    // get(). Inline-encoded short terms must therefore not be decoded into shared scratch.
    List<BytesRef> expected = new ArrayList<>();
    List<BytesRef> retained = new ArrayList<>();
    BytesRefBuilder ref = new BytesRefBuilder();
    int numTerms = atLeast(200);
    for (int i = 0; i < numTerms; i++) {
      String str;
      do {
        // bias toward lengths at and around the inline boundary
        str = TestUtil.randomRealisticUnicodeString(random(), 1, 4);
      } while (str.length() == 0);
      ref.copyChars(str);
      if (hash.add(ref.get()) >= 0) {
        expected.add(ref.toBytesRef());
      }
    }
    for (int id = 0; id < hash.size(); id++) {
      retained.add(hash.get(id, new BytesRef()));
    }
    for (int id = 0; id < expected.size(); id++) {
      assertEquals(
          "ref retained from get() was clobbered by a later get()",
          expected.get(id),
          retained.get(id));
    }
  }
}
