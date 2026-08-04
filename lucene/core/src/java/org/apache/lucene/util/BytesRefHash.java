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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.internal.hppc.BitMixer;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure optimized for {@link
 * BytesRef} instances. BytesRefHash maintains mappings of byte arrays to ids
 * (Map&lt;BytesRef,int&gt;) storing the hashed bytes efficiently in continuous storage. The mapping
 * to the id is encapsulated inside {@link BytesRefHash} and is guaranteed to be increased for each
 * added {@link BytesRef}.
 *
 * <p>Note: The maximum capacity {@link BytesRef} instance passed to {@link #add(BytesRef)} must not
 * be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. The internal storage is limited to 2GB
 * total byte storage.
 *
 * @lucene.internal
 */
public final class BytesRefHash implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
          +
          // size of Counter
          RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final BytesRefBlockPool pool;
  int[] bytesStart;

  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  // This mask is used to extract the high bits from a hashcode
  private int highMask;
  private int count;
  private int lastCount = -1;

  /**
   * The <code>ids</code> array serves a dual purpose:
   *
   * <ol>
   *   <li>When the value is <code>-1</code>, it indicates an empty slot in the hash table.
   *   <li>When the value is not <code>-1</code>, it stores:
   *       <ul>
   *         <li>The actual index into the <code>bytesStart</code> array (low bits, masked by <code>
   *             hashMask</code>).
   *         <li>The high bits of the original hashcode (high bits, masked by <code>highMask</code>
   *             ).
   *       </ul>
   * </ol>
   *
   * <p>This "trick" allows us to store both the index and part of the hashcode in a single int,
   * which speeds up hash collisions by quickly rejecting non-matching entries without having to
   * compare the actual byte values. During lookups, we can immediately check if the high bits match
   * before doing the more expensive byte comparison.
   *
   * <p><b>Example:</b>
   *
   * <ul>
   *   <li>hashSize = 16, therefore <code>hashMask = 15</code> (<code>0x0000000F</code>)
   *   <li><code>highMask = ~hashMask = 0xFFFFFFF0</code>
   * </ul>
   *
   * <p>When storing the value 7 with hashcode <code>0x12345678</code>:
   *
   * <ul>
   *   <li>The low bits (index) are 7 (<code>0x00000007</code>)
   *   <li>The high bits of hashcode are <code>0x12345670</code>
   *   <li>The stored value becomes: <code>0x12345677</code>
   * </ul>
   *
   * <p><b>During lookup:</b>
   *
   * <ol>
   *   <li>We compute the hashcode and find the slot.
   *   <li>We extract the stored value's high bits (<code>& highMask</code>).
   *   <li>If they match the lookup hashcode's high bits, we proceed to comparing actual bytes.
   *   <li>Otherwise, we immediately know it's not a match and continue probing.
   * </ol>
   *
   * <p>This significantly improves performance for hash lookups, especially with many collisions.
   */
  private int[] ids;

  private final BytesStartArray bytesStartArray;
  private final Counter bytesUsed;

  /**
   * Maximum length in bytes of a term that is encoded inline into its {@code bytesStart} entry
   * rather than being written to the {@link ByteBlockPool}.
   *
   * <p>Inline entries are identified by their sign bit: valid pool offsets are always &gt;= 0,
   * while inline entries are always negative. The layout of an inline entry is: the top byte holds
   * {@code 0x80 | length}, and the term bytes are packed in little-endian order, i.e. the first
   * term byte occupies the lowest 8 bits. The little-endian placement is deliberate: {@link
   * #addByPoolOffset(int)} and rehashing with {@code hashOnData=false} use the raw entry value as
   * the hash code, so the most-varying byte of the term must land in the low bits to avoid
   * clustering.
   *
   * @lucene.internal
   */
  public static final int MAX_INLINE_LENGTH = 3;

  private static final int INLINE_FLAG = 0x80;

  /**
   * Returns true if the given {@code bytesStart} entry holds an inline-encoded term rather than an
   * offset into the {@link ByteBlockPool}.
   *
   * @lucene.internal
   */
  public static boolean isInline(int bytesStartValue) {
    return bytesStartValue < 0;
  }

  /**
   * Encodes a term of at most {@link #MAX_INLINE_LENGTH} bytes into an inline {@code bytesStart}
   * entry. The encoding is canonical: two terms are equal if and only if their encoded values are
   * equal. The returned value always has its sign bit set, so it is never 0 and never collides with
   * a pool offset.
   *
   * @lucene.internal
   */
  public static int encodeInline(BytesRef bytes) {
    assert bytes.length <= MAX_INLINE_LENGTH;
    int encoded = (INLINE_FLAG | bytes.length) << 24;
    for (int i = 0; i < bytes.length; i++) {
      encoded |= (bytes.bytes[bytes.offset + i] & 0xFF) << (i << 3);
    }
    return encoded;
  }

  /** Returns the term length encoded in the given inline entry. */
  static int inlineLength(int encoded) {
    assert isInline(encoded);
    return (encoded >>> 24) & ~INLINE_FLAG;
  }

  /**
   * Decodes an inline entry into the given {@link BytesRef}, writing the term bytes into {@code
   * scratch} (which must be at least {@link #MAX_INLINE_LENGTH} bytes, or the exact term length)
   * and pointing {@code ref} at it.
   *
   * @lucene.internal
   */
  public static void decodeInline(int encoded, BytesRef ref, byte[] scratch) {
    final int len = inlineLength(encoded);
    assert scratch.length >= len;
    for (int i = 0; i < len; i++) {
      scratch[i] = (byte) (encoded >>> (i << 3));
    }
    ref.bytes = scratch;
    ref.offset = 0;
    ref.length = len;
  }

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a {@link
   * DirectAllocator}.
   */
  public BytesRefHash() {
    this(new ByteBlockPool(new DirectAllocator()));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be greater than 0");
    }

    if (BitUtil.isZeroOrPowerOfTwo(capacity) == false) {
      throw new IllegalArgumentException("capacity must be a power of two, got " + capacity);
    }
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    highMask = ~hashMask;
    this.pool = new BytesRefBlockPool(pool);
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    final Counter bytesUsed = bytesStartArray.bytesUsed();
    this.bytesUsed = bytesUsed == null ? Counter.newCounter() : bytesUsed;
    this.bytesUsed.addAndGet(hashSize * (long) Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   *
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given bytesID.
   *
   * <p>Note: the given bytesID must be a positive integer less than the current size ({@link
   * #size()})
   *
   * <p>Note: for terms of at most {@link #MAX_INLINE_LENGTH} bytes the returned ref points at a
   * freshly allocated array rather than into the internal pool. A fresh array (instead of a shared
   * scratch) is required to keep previously returned refs valid, which callers rely on.
   *
   * @param bytesID the id
   * @param ref the {@link BytesRef} to populate
   * @return the given BytesRef instance populated with the bytes for the given bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length : "bytesID exceeds byteStart len: " + bytesStart.length;
    final int bytesStartValue = bytesStart[bytesID];
    if (isInline(bytesStartValue)) {
      decodeInline(bytesStartValue, ref, new byte[inlineLength(bytesStartValue)]);
    } else {
      pool.fillBytesRef(ref, bytesStartValue);
    }
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0 and end at a limit of
   * {@link #size()} - 1
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   *
   * @lucene.internal
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";

    // id is the sequence number when bytes added to the pool
    for (int i = 0; i < count; i++) {
      ids[i] = i;
    }
    Arrays.fill(ids, count, hashSize, -1);

    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   */
  public int[] sort() {
    final int[] compact = compact();
    assert count * 2 <= compact.length : "We need load factor <= 0.5f to speed up this sort";
    final int tmpOffset = count;
    new StringSorter(BytesRefComparator.NATURAL) {

      @Override
      protected Sorter radixSorter(BytesRefComparator cmp) {
        return new MSBStringRadixSorter(cmp) {

          private int k;

          @Override
          protected void buildHistogram(
              int prefixCommonBucket,
              int prefixCommonLen,
              int from,
              int to,
              int k,
              int[] histogram) {
            this.k = k;
            histogram[prefixCommonBucket] = prefixCommonLen;
            Arrays.fill(
                compact, tmpOffset + from - prefixCommonLen, tmpOffset + from, prefixCommonBucket);
            for (int i = from; i < to; ++i) {
              int b = getBucket(i, k);
              compact[tmpOffset + i] = b;
              histogram[b]++;
            }
          }

          @Override
          protected boolean shouldFallback(int from, int to, int l) {
            // We lower the fallback threshold because the bucket cache speeds up the reorder
            return to - from <= LENGTH_THRESHOLD / 2 || l >= LEVEL_THRESHOLD;
          }

          private void swapBucketCache(int i, int j) {
            swap(i, j);
            int tmp = compact[tmpOffset + i];
            compact[tmpOffset + i] = compact[tmpOffset + j];
            compact[tmpOffset + j] = tmp;
          }

          @Override
          protected void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
            assert this.k == k;
            for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
              final int limit = endOffsets[i];
              for (int h1 = startOffsets[i]; h1 < limit; h1 = startOffsets[i]) {
                final int b = compact[tmpOffset + from + h1];
                final int h2 = startOffsets[b]++;
                swapBucketCache(from + h1, from + h2);
              }
            }
          }
        };
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected void get(BytesRefBuilder builder, BytesRef result, int i) {
        final int bytesStartValue = bytesStart[compact[i]];
        if (isInline(bytesStartValue)) {
          // Decode into the builder's owned bytes: this method is called for every byte
          // inspection of the radix sorter, so it must not allocate.
          builder.growNoCopy(MAX_INLINE_LENGTH);
          decodeInline(bytesStartValue, result, builder.bytes());
        } else {
          pool.fillBytesRef(result, bytesStartValue);
        }
      }
    }.sort(0, count);
    Arrays.fill(compact, tmpOffset, compact.length, -1);
    return compact;
  }

  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * (long) -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      highMask = ~hashMask;
      return true;
    } else {
      return false;
    }
  }

  /** Clears the {@link BytesRef} which maps to the given {@link BytesRef} */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.reset();
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }

  /** Closes the BytesRefHash and releases all internally used memory */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * (long) -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   *
   * @param bytes the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the given bytes,
   *     otherwise <code>(-(id)-1)</code>. This guarantees that the return value will always be
   *     &gt;= 0 if the given bytes haven't been hashed before.
   * @throws MaxBytesLengthExceededException if the given bytes are {@code > 2 +} {@link
   *     ByteBlockPool#BYTE_BLOCK_SIZE}
   */
  public int add(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    final int hashcode = doHash(bytes.bytes, bytes.offset, bytes.length);
    // Pre-encode short terms once: the encoding is canonical, so probe comparisons reduce to a
    // single int comparison and never have to touch the pool. 0 means "not inlinable" and can
    // never clash with an encoded value, which always has its sign bit set.
    final int inlined = bytes.length <= MAX_INLINE_LENGTH ? encodeInline(bytes) : 0;
    // final position
    final int hashPos = findHash(bytes, hashcode, inlined);
    int e = ids[hashPos];

    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      bytesStart[count] = inlined != 0 ? inlined : pool.addBytesRef(bytes);
      e = count++;
      assert ids[hashPos] == -1;
      ids[hashPos] = e | (hashcode & highMask);

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }
    e = e & hashMask;
    return -(e + 1);
  }

  /**
   * Returns the id of the given {@link BytesRef}.
   *
   * @param bytes the bytes to look for
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the given bytes.
   */
  public int find(BytesRef bytes) {
    final int hashcode = doHash(bytes.bytes, bytes.offset, bytes.length);
    final int inlined = bytes.length <= MAX_INLINE_LENGTH ? encodeInline(bytes) : 0;
    final int id = ids[findHash(bytes, hashcode, inlined)];
    return id == -1 ? -1 : id & hashMask;
  }

  private int findHash(BytesRef bytes, int hashcode, int inlined) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert hashcode == doHash(bytes.bytes, bytes.offset, bytes.length);

    int code = hashcode;
    // final position
    int hashPos = code & hashMask;
    int e = ids[hashPos];
    final int highBits = hashcode & highMask;

    // Conflict; use linear probe to find an open slot
    // (see LUCENE-5604):
    while (e != -1
        && ((e & highMask) != highBits
            || termEquals(bytesStart[e & hashMask], bytes, inlined) == false)) {
      code++;
      hashPos = code & hashMask;
      e = ids[hashPos];
    }

    return hashPos;
  }

  /**
   * Compares the term stored at the given {@code bytesStart} entry against the query term. {@code
   * inlined} must be {@link #encodeInline(BytesRef)} of {@code bytes} if the query term fits
   * inline, else 0.
   */
  private boolean termEquals(int bytesStartValue, BytesRef bytes, int inlined) {
    if (inlined != 0) {
      // Short query term: every stored term of length <= MAX_INLINE_LENGTH is inlined (see add)
      // and the encoding is canonical, so a single int comparison is an exact equality check. It
      // also rejects pool entries, which are >= 0 while inlined is negative.
      return bytesStartValue == inlined;
    }
    // Query term longer than MAX_INLINE_LENGTH: inline entries can never match.
    return bytesStartValue >= 0 && pool.equals(bytesStartValue, bytes);
  }

  /**
   * Adds a "arbitrary" int offset instead of a BytesRef term. This is used in the indexer to hold
   * the hash for term vectors, because they do not redundantly store the byte[] term directly and
   * instead reference the byte[] term already stored by the postings BytesRefHash. See add(int
   * textStart) in TermsHashPerField.
   *
   * <p>Note: the given offset may also be a negative inline-encoded term (see {@link
   * #isInline(int)}) taken from another hash's {@code bytesStart} entry. Since the inline encoding
   * is canonical, the int equality used here remains an exact term equality check.
   */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    // final position
    int code = offsetCode(offset);
    int hashPos = code & hashMask;
    int e = ids[hashPos];

    // Conflict; use linear probe to find an open slot
    // (see LUCENE-5604):
    while (e != -1 && bytesStart[e] != offset) {
      code++;
      hashPos = code & hashMask;
      e = ids[hashPos];
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Derives the probe code for a {@code bytesStart} entry that {@link #addByPoolOffset(int)} uses
   * as an exact key. Pool offsets increase monotonically and already spread perfectly over the
   * table, so they are used as-is. Inline-encoded entries instead pack the term's first byte into
   * the low bits, so using them raw clusters every short term sharing a first byte into one slot;
   * they are mixed to recover a uniform distribution. {@link #rehash} with {@code hashOnData=false}
   * must derive codes the same way.
   */
  private static int offsetCode(int bytesStartValue) {
    return isInline(bytesStartValue) ? BitMixer.mix(bytesStartValue) : bytesStartValue;
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%} occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    final int newHighMask = ~newMask;
    bytesUsed.addAndGet(Integer.BYTES * (long) (newSize - ids.length));

    ids = new int[newSize];
    Arrays.fill(ids, -1);

    // scratch for hashing inline-encoded terms without allocating per entry
    final byte[] scratch = hashOnData ? new byte[MAX_INLINE_LENGTH] : null;

    // rebuild ids from terms in pool pointed by bytesStart
    for (int id = 0; id < count; id++) {
      final int hashcode;
      int code;
      if (hashOnData) {
        final int bytesStartValue = bytesStart[id];
        if (isInline(bytesStartValue)) {
          final int len = inlineLength(bytesStartValue);
          for (int i = 0; i < len; i++) {
            scratch[i] = (byte) (bytesStartValue >>> (i << 3));
          }
          hashcode = code = doHash(scratch, 0, len);
        } else {
          hashcode = code = pool.hash(bytesStartValue);
        }
      } else {
        code = offsetCode(bytesStart[id]);
        hashcode = 0;
      }

      int hashPos = code & newMask;
      assert hashPos >= 0;

      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      while (ids[hashPos] != -1) {
        code++;
        hashPos = code & newMask;
      }

      ids[hashPos] = id | (hashcode & newHighMask);
    }

    hashMask = newMask;
    highMask = newHighMask;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  static int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()} call. If {@link
   * #clear()} has not been called previously this method has no effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }

    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * (long) hashSize);
    }
  }

  /**
   * Returns the {@code bytesStart} entry for the given bytesID. For terms longer than {@link
   * #MAX_INLINE_LENGTH} bytes this is the term's offset into the internally used {@link
   * ByteBlockPool}; for shorter terms it is a negative value holding the term encoded inline (see
   * {@link #isInline(int)}). Either form may be passed to {@link #addByPoolOffset(int)} of a hash
   * sharing the same pool.
   *
   * @param bytesID the id to look up
   * @return the bytesStart entry for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(bytesStart)
            + RamUsageEstimator.sizeOfObject(ids)
            + RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of {@link
   * ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     *
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     *
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     *
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this {@link BytesStartArray}.
     * The {@link BytesRefHash} uses this reference to track it memory usage
     *
     * @return a {@link AtomicLong} reference holding the number of bytes used by this {@link
     *     BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /**
   * A simple {@link BytesStartArray} that tracks memory allocation using a private {@link Counter}
   * instance.
   */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;

    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;
    }

    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
