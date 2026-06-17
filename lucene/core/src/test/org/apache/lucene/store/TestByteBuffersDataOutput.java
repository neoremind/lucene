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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;

public final class TestByteBuffersDataOutput extends BaseDataOutputTestCase<ByteBuffersDataOutput> {
  @Override
  protected ByteBuffersDataOutput newInstance() {
    return new ByteBuffersDataOutput();
  }

  @Override
  protected byte[] toBytes(ByteBuffersDataOutput instance) {
    return instance.toArrayCopy();
  }

  public void testReuse() throws IOException {
    AtomicInteger allocations = new AtomicInteger(0);
    ByteBuffersDataOutput.ByteBufferRecycler reuser =
        new ByteBuffersDataOutput.ByteBufferRecycler(
            (size) -> {
              allocations.incrementAndGet();
              return ByteBuffer.allocate(size);
            });

    ByteBuffersDataOutput o =
        new ByteBuffersDataOutput(
            ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
            ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
            reuser::allocate,
            reuser::reuse);

    // Add some random data first.
    long genSeed = random().nextLong();
    int addCount = TestUtil.nextInt(random(), 1000, 5000);
    addRandomData(o, new Random(genSeed), addCount);
    byte[] data = o.toArrayCopy();

    // Use the same sequence over reused instance.
    final int expectedAllocationCount = allocations.get();
    o.reset();
    addRandomData(o, new Random(genSeed), addCount);

    assertEquals(expectedAllocationCount, allocations.get());
    assertArrayEquals(data, o.toArrayCopy());
  }

  public void testConstructorWithExpectedSize() {
    {
      ByteBuffersDataOutput o = new ByteBuffersDataOutput(0);
      o.writeByte((byte) 0);
      assertEquals(
          1 << ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
          o.toBufferList().get(0).capacity());
    }

    {
      long MB = 1024 * 1024;
      long expectedSize = TestUtil.nextLong(random(), MB, MB * 1024);
      ByteBuffersDataOutput o = new ByteBuffersDataOutput(expectedSize);
      o.writeByte((byte) 0);
      int cap = o.toBufferList().get(0).capacity();
      assertTrue(
          (cap >> 1) * ByteBuffersDataOutput.MAX_BLOCKS_BEFORE_BLOCK_EXPANSION < expectedSize);
      assertTrue(
          "cap=" + cap + ", exp=" + expectedSize,
          (cap) * ByteBuffersDataOutput.MAX_BLOCKS_BEFORE_BLOCK_EXPANSION >= expectedSize);
    }
  }

  public void testIllegalMinBitsPerBlock() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new ByteBuffersDataOutput(
              ByteBuffersDataOutput.LIMIT_MIN_BITS_PER_BLOCK - 1,
              ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
              ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
              ByteBuffersDataOutput.NO_REUSE);
        });
  }

  public void testIllegalMaxBitsPerBlock() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new ByteBuffersDataOutput(
              ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
              ByteBuffersDataOutput.LIMIT_MAX_BITS_PER_BLOCK + 1,
              ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
              ByteBuffersDataOutput.NO_REUSE);
        });
  }

  public void testIllegalBitsPerBlockRange() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new ByteBuffersDataOutput(
              20, 19, ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP, ByteBuffersDataOutput.NO_REUSE);
        });
  }

  public void testNullAllocator() {
    expectThrows(
        NullPointerException.class,
        () -> {
          new ByteBuffersDataOutput(
              ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
              ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
              null,
              ByteBuffersDataOutput.NO_REUSE);
        });
  }

  public void testNullRecycler() {
    expectThrows(
        NullPointerException.class,
        () -> {
          new ByteBuffersDataOutput(
              ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
              ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
              ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
              null);
        });
  }

  public void testSanity() {
    ByteBuffersDataOutput o = newInstance();
    assertEquals(0, o.size());
    assertEquals(0, o.toArrayCopy().length);
    assertEquals(0, o.ramBytesUsed());

    o.writeByte((byte) 1);
    assertEquals(1, o.size());
    assertTrue(o.ramBytesUsed() > 0);
    assertArrayEquals(new byte[] {1}, o.toArrayCopy());

    o.writeBytes(new byte[] {2, 3, 4}, 3);
    assertEquals(4, o.size());
    assertArrayEquals(new byte[] {1, 2, 3, 4}, o.toArrayCopy());
  }

  public void testWriteByteBuffer() {
    ByteBuffersDataOutput o = new ByteBuffersDataOutput();
    byte[] bytes = new byte[1024 * 8 + 10];
    random().nextBytes(bytes);
    ByteBuffer src = ByteBuffer.wrap(bytes);
    int offset = TestUtil.nextInt(random(), 0, 100);
    int len = bytes.length - offset;
    src.position(offset);
    src.limit(offset + len);
    o.writeBytes(src);
    assertEquals(len, o.size());
    Assert.assertArrayEquals(
        ArrayUtil.copyOfSubArray(bytes, offset, offset + len), o.toArrayCopy());
  }

  public void testLargeArrayAdd() {
    ByteBuffersDataOutput o = new ByteBuffersDataOutput();
    int MB = 1024 * 1024;
    final byte[] bytes;
    if (LuceneTestCase.TEST_NIGHTLY) {
      bytes = new byte[TestUtil.nextInt(random(), 5 * MB, 15 * MB)];
    } else {
      bytes = new byte[TestUtil.nextInt(random(), MB / 2, MB)];
    }
    random().nextBytes(bytes);
    int offset = TestUtil.nextInt(random(), 0, 100);
    int len = bytes.length - offset;
    o.writeBytes(bytes, offset, len);
    assertEquals(len, o.size());
    Assert.assertArrayEquals(
        ArrayUtil.copyOfSubArray(bytes, offset, offset + len), o.toArrayCopy());
  }

  public void testCopyBytesOnHeap() throws IOException {
    byte[] bytes = new byte[1024 * 8 + 10];
    random().nextBytes(bytes);
    int offset = TestUtil.nextInt(random(), 0, 100);
    int len = bytes.length - offset;
    ByteArrayDataInput in = new ByteArrayDataInput(bytes, offset, len);
    ByteBuffersDataOutput o =
        new ByteBuffersDataOutput(
            ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
            ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
            ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
            ByteBuffersDataOutput.NO_REUSE);
    o.copyBytes(in, len);
    Assert.assertArrayEquals(
        o.toArrayCopy(), ArrayUtil.copyOfSubArray(bytes, offset, offset + len));
  }

  public void testCopyBytesOnDirectByteBuffer() throws IOException {
    byte[] bytes = new byte[1024 * 8 + 10];
    random().nextBytes(bytes);
    int offset = TestUtil.nextInt(random(), 0, 100);
    int len = bytes.length - offset;
    ByteArrayDataInput in = new ByteArrayDataInput(bytes, offset, len);
    ByteBuffersDataOutput o =
        new ByteBuffersDataOutput(
            ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
            ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
            ByteBuffer::allocateDirect,
            ByteBuffersDataOutput.NO_REUSE);
    o.copyBytes(in, len);
    Assert.assertArrayEquals(
        o.toArrayCopy(), ArrayUtil.copyOfSubArray(bytes, offset, offset + len));
  }

  public void testToBufferListReturnsReadOnlyBuffers() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    dst.writeBytes(new byte[100]);
    for (ByteBuffer bb : dst.toBufferList()) {
      assertTrue(bb.isReadOnly());
    }
  }

  public void testToWritableBufferListReturnsOriginalBuffers() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    for (ByteBuffer bb : dst.toWritableBufferList()) {
      assertTrue(!bb.isReadOnly());
      assertTrue(bb.hasArray()); // even the empty buffer should have a backing array.
    }

    dst.writeBytes(new byte[100]);
    for (ByteBuffer bb : dst.toWritableBufferList()) {
      assertTrue(!bb.isReadOnly());
      assertTrue(bb.hasArray()); // heap-based by default, so array should be there.
    }
  }

  public void testRamBytesUsed() {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    // Empty output requires no RAM
    assertEquals(0, out.ramBytesUsed());

    // Non-empty buffer requires RAM
    out.writeInt(4);
    assertEquals(out.ramBytesUsed(), computeRamBytesUsed(out));

    // Make sure this keeps working with multiple backing buffers
    while (out.toBufferList().size() < 2) {
      out.writeLong(42);
    }
    assertEquals(out.ramBytesUsed(), computeRamBytesUsed(out));

    // Make sure this keeps working when increasing the block size
    int currentBlockCapacity = out.toBufferList().get(0).capacity();
    do {
      out.writeLong(42);
    } while (out.toBufferList().get(0).capacity() == currentBlockCapacity);
    assertEquals(out.ramBytesUsed(), computeRamBytesUsed(out));

    // Back to zero after a clear
    out.reset();
    assertEquals(0, out.ramBytesUsed());

    // And back to non-empty
    out.writeInt(4);
    assertEquals(out.ramBytesUsed(), computeRamBytesUsed(out));
  }

  private static long computeRamBytesUsed(ByteBuffersDataOutput out) {
    if (out.size() == 0) {
      return 0;
    }
    List<ByteBuffer> buffers = out.toBufferList();
    return buffers.stream().mapToLong(ByteBuffer::capacity).sum()
        + buffers.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  }

  public void testWriteString() throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();

    String shortAscii = "hello world";
    out.writeString(shortAscii);

    String mediumAscii = "a".repeat(100);
    out.writeString(mediumAscii);

    String longAscii = "x".repeat(500);
    out.writeString(longAscii);

    String cjk = "\u4e2d\u6587\u5b57\u7b26\u4e32\u6d4b\u8bd5";
    out.writeString(cjk);

    // String with surrogate pairs (emoji)
    String emoji = "Hello \uD83D\uDE00 World \uD83C\uDF1F!";
    out.writeString(emoji);

    String large = "\u0410".repeat(2000); // Cyrillic А, 2-byte UTF-8
    out.writeString(large);

    String vlarge = "Z".repeat(8000);
    out.writeString(vlarge);

    out.writeString("");

    ByteBuffersDataInput in = out.toDataInput();
    assertEquals(shortAscii, in.readString());
    assertEquals(mediumAscii, in.readString());
    assertEquals(longAscii, in.readString());
    assertEquals(cjk, in.readString());
    assertEquals(emoji, in.readString());
    assertEquals(large, in.readString());
    assertEquals(vlarge, in.readString());
    assertEquals("", in.readString());
  }

  public void testWriteStringWithDefaultStartingBlockSize() throws IOException {
    ByteBuffersDataOutput out =
        new ByteBuffersDataOutput(
            ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
            ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
            ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
            ByteBuffersDataOutput.NO_REUSE);

    int num = atLeast(5000);
    String[] strings = new String[num];
    for (int i = 0; i < num; i++) {
      strings[i] = TestUtil.randomUnicodeString(random(), random().nextInt(20000));
      out.writeString(strings[i]);
    }

    ByteBuffersDataInput in = out.toDataInput();
    for (int i = 0; i < num; i++) {
      assertEquals("String at index " + i, strings[i], in.readString());
    }
  }

  /** Randomized test that writes many strings with small blocks to stress block boundary. */
  public void testWriteStringWithSmallStartingBlockSize() throws IOException {
    // Use smallest possible starting block size to maximize boundary crossings
    ByteBuffersDataOutput out =
        new ByteBuffersDataOutput(
            ByteBuffersDataOutput.LIMIT_MIN_BITS_PER_BLOCK,
            ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
            ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP,
            ByteBuffersDataOutput.NO_REUSE);

    int num = atLeast(50000);
    String[] strings = new String[num];
    for (int i = 0; i < num; i++) {
      strings[i] = TestUtil.randomUnicodeString(random(), random().nextInt(200));
      out.writeString(strings[i]);
    }

    ByteBuffersDataInput in = out.toDataInput();
    for (int i = 0; i < num; i++) {
      assertEquals("String at index " + i, strings[i], in.readString());
    }
  }
}
