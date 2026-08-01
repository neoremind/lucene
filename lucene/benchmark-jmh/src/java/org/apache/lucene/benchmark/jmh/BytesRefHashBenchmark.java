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
package org.apache.lucene.benchmark.jmh;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RecyclingByteBlockAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks {@link BytesRefHash} on workloads modeled after the indexing hot path (see
 * TermsHashPerField): a Zipf-distributed stream of term lookups where the vast majority of {@code
 * add()} calls hit an already-seen term (~98.6% on wikipedia), and where terms of at most 3 UTF-8
 * bytes account for roughly a third of all lookups.
 *
 * <p>Workload dimensions:
 *
 * <ul>
 *   <li>{@code shortTermRatio} — occurrence-weighted share of lookups whose term is at most 3
 *       bytes, i.e. eligible for inline encoding. {@code 0.32} matches the measured wikipedia
 *       distribution; {@code 0} is a pure long-term workload (regression check: e.g. ID/UUID fields
 *       must not get slower); {@code 1} isolates the inline fast path.
 *   <li>{@code vocabSize} — number of unique terms, which drives the working-set size (pool +
 *       bytesStart + ids arrays). {@code 4096} fits comfortably in L2, so the pool indirection is
 *       nearly free and any win shown here is pure instruction count. {@code 1048576} pushes the
 *       working set past typical L3 shares, exposing the cache misses that motivated the inline
 *       optimization.
 *   <li>JMH threads ({@code -t}) — state is {@link Scope#Thread}: each thread owns its hash and
 *       pool, exactly like concurrent DWPTs during indexing. Run with {@code -t 1} for a quiet
 *       cache and {@code -t <physical cores>} to model the aggregate cache/memory-bandwidth
 *       pressure of real concurrent indexing, where the indirection penalty is largest.
 * </ul>
 *
 * <p>Benchmarks:
 *
 * <ul>
 *   <li>{@link #addSeen} — steady-state hit path: every add() finds an existing term. This is the
 *       flat top of indexing flamegraphs.
 *   <li>{@link #fillSegment} — full segment lifecycle: clear/reinit, then the whole stream
 *       including new-term inserts, pool writes and rehashes.
 *   <li>{@link #fillSegmentAndSort} — fillSegment plus the destructive {@code sort()} that runs at
 *       flush. Run with {@code -prof gc} to compare allocation rates of the sort path.
 * </ul>
 *
 * <p>To compare baseline vs optimization, cherry-pick the commit adding this file onto the baseline
 * branch (it only uses stable BytesRefHash API), then on each branch:
 *
 * <pre>
 * ./gradlew -p lucene/benchmark-jmh assemble
 * java --module-path lucene/benchmark-jmh/build/benchmarks \
 *   --module org.apache.lucene.benchmark.jmh BytesRefHashBenchmark -t 1
 * java --module-path lucene/benchmark-jmh/build/benchmarks \
 *   --module org.apache.lucene.benchmark.jmh BytesRefHashBenchmark.addSeen -t 8 -prof gc
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class BytesRefHashBenchmark {

  /** Number of add() calls per benchmark invocation. */
  private static final int ACCESSES = 1 << 20;

  /** Zipf exponent for term popularity; ~1.07 is typical for natural language. */
  private static final double ZIPF_EXPONENT = 1.07;

  /** Occurrence-weighted share of lookups for terms of at most 3 bytes. 0.32 matches wikipedia. */
  @Param({"0", "0.32", "1"})
  public double shortTermRatio;

  /** Number of unique terms. 4096 is cache-resident, 1048576 spills far beyond L2. */
  @Param({"4096", "1048576"})
  public int vocabSize;

  private BytesRefHash hash;
  private BytesRef[] terms;
  private int[] accessOrder;

  @Setup(Level.Trial)
  public void setUp() {
    // fixed seed: identical term sets and access patterns on every branch/run
    Random random = new Random(0xBADD_F00DL);

    // Build the vocabulary. Distinct short terms are naturally scarce (there are only 256
    // one-byte terms) and few thousand distinct short terms is realistic for natural language,
    // so cap the short tier and fill the rest of the vocabulary with longer terms.
    int shortVocabSize;
    if (shortTermRatio == 0) {
      shortVocabSize = 0;
    } else if (shortTermRatio == 1) {
      shortVocabSize = Math.min(vocabSize, 8192);
    } else {
      shortVocabSize = Math.min(Math.max(vocabSize / 16, 256), 4096);
    }
    int longVocabSize = shortTermRatio == 1 ? 0 : vocabSize - shortVocabSize;

    Set<BytesRef> unique = new HashSet<>();
    BytesRef[] shortTerms = new BytesRef[shortVocabSize];
    for (int i = 0; i < shortVocabSize; i++) {
      shortTerms[i] = generateTerm(random, shortLength(random), unique);
    }
    BytesRef[] longTerms = new BytesRef[longVocabSize];
    for (int i = 0; i < longVocabSize; i++) {
      longTerms[i] = generateTerm(random, longLength(random), unique);
    }

    // Zipf samplers over each tier: hot terms exist in both tiers, but short terms as a class
    // get exactly shortTermRatio of all accesses.
    int[] shortZipf = zipfCumulativeSampler(random, shortVocabSize);
    int[] longZipf = zipfCumulativeSampler(random, longVocabSize);

    // Interleave tiers into one vocabulary insertion order, decorrelating popularity from
    // insertion order (and thus from pool layout), as in real indexing.
    terms = new BytesRef[shortVocabSize + longVocabSize];
    int[] shortIndex = new int[shortVocabSize];
    int[] longIndex = new int[longVocabSize];
    shuffleInterleave(random, shortTerms, longTerms, terms, shortIndex, longIndex);

    accessOrder = new int[ACCESSES];
    for (int i = 0; i < ACCESSES; i++) {
      if (random.nextDouble() < shortTermRatio) {
        accessOrder[i] = shortIndex[shortZipf[random.nextInt(shortZipf.length)]];
      } else {
        accessOrder[i] = longIndex[longZipf[random.nextInt(longZipf.length)]];
      }
    }

    hash = new BytesRefHash(new ByteBlockPool(new RecyclingByteBlockAllocator()));
    populate();
  }

  private void populate() {
    hash.clear(true);
    hash.reinit();
    for (BytesRef term : terms) {
      hash.add(term);
    }
  }

  /** Steady-state hit path: every add() finds an already-seen term. */
  @Benchmark
  @OperationsPerInvocation(ACCESSES)
  public void addSeen(Blackhole bh) {
    final BytesRefHash hash = this.hash;
    final BytesRef[] terms = this.terms;
    final int[] accessOrder = this.accessOrder;
    long sum = 0;
    for (int i = 0; i < accessOrder.length; i++) {
      sum += hash.add(terms[accessOrder[i]]);
    }
    bh.consume(sum);
  }

  /** Segment lifecycle: clear, then the whole stream including inserts and rehashes. */
  @Benchmark
  @OperationsPerInvocation(ACCESSES)
  public void fillSegment(Blackhole bh) {
    final BytesRefHash hash = this.hash;
    hash.clear(true);
    hash.reinit();
    final BytesRef[] terms = this.terms;
    final int[] accessOrder = this.accessOrder;
    long sum = 0;
    for (int i = 0; i < accessOrder.length; i++) {
      sum += hash.add(terms[accessOrder[i]]);
    }
    bh.consume(sum);
  }

  /** Segment lifecycle plus the flush-time sort. Compare with -prof gc. */
  @Benchmark
  @OperationsPerInvocation(ACCESSES)
  public void fillSegmentAndSort(Blackhole bh) {
    fillSegment(bh);
    bh.consume(hash.sort());
  }

  /** Lengths 1..3, weighted by their relative shares within wikipedia's <=3 byte lookups. */
  private static int shortLength(Random random) {
    double r = random.nextDouble();
    if (r < 0.11) {
      return 1;
    } else if (r < 0.51) {
      return 2;
    } else {
      return 3;
    }
  }

  /** Lengths 4..16, roughly following the tail of the wikipedia term length histogram. */
  private static int longLength(Random random) {
    double r = random.nextDouble();
    if (r < 0.21) {
      return 4;
    } else if (r < 0.36) {
      return 5;
    } else if (r < 0.49) {
      return 6;
    } else if (r < 0.61) {
      return 7;
    } else if (r < 0.70) {
      return 8;
    } else {
      return 9 + random.nextInt(8);
    }
  }

  private static BytesRef generateTerm(Random random, int length, Set<BytesRef> unique) {
    int attempts = 0;
    while (true) {
      byte[] bytes = new byte[length];
      random.nextBytes(bytes);
      BytesRef term = new BytesRef(bytes);
      if (unique.add(term)) {
        return term;
      }
      // the requested length may be saturated (there are only 256 distinct 1-byte terms);
      // fall back to the next longer length
      if (++attempts > 1000) {
        length++;
        attempts = 0;
      }
    }
  }

  /**
   * Returns a lookup table such that indexing it with a uniform random value yields Zipf-
   * distributed ranks over [0, vocab). Table-based to keep per-access sampling at setup cheap.
   */
  private static int[] zipfCumulativeSampler(Random random, int vocab) {
    if (vocab == 0) {
      return new int[0];
    }
    double[] cumulative = new double[vocab];
    double sum = 0;
    for (int rank = 0; rank < vocab; rank++) {
      sum += 1.0 / Math.pow(rank + 1, ZIPF_EXPONENT);
      cumulative[rank] = sum;
    }
    int tableSize = Math.max(vocab * 4, 1 << 16);
    int[] table = new int[tableSize];
    for (int i = 0; i < tableSize; i++) {
      double target = random.nextDouble() * sum;
      int lo = 0;
      int hi = vocab - 1;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (cumulative[mid] < target) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      table[i] = lo;
    }
    return table;
  }

  /**
   * Randomly interleaves both tiers into {@code out} and records, per tier, where each tier rank
   * ended up ({@code shortIndex[rank]} = position in {@code out}).
   */
  private static void shuffleInterleave(
      Random random,
      BytesRef[] shortTerms,
      BytesRef[] longTerms,
      BytesRef[] out,
      int[] shortIndex,
      int[] longIndex) {
    int total = out.length;
    // permutation of output slots
    int[] slots = new int[total];
    for (int i = 0; i < total; i++) {
      slots[i] = i;
    }
    for (int i = total - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int tmp = slots[i];
      slots[i] = slots[j];
      slots[j] = tmp;
    }
    int s = 0;
    for (int rank = 0; rank < shortTerms.length; rank++, s++) {
      out[slots[s]] = shortTerms[rank];
      shortIndex[rank] = slots[s];
    }
    for (int rank = 0; rank < longTerms.length; rank++, s++) {
      out[slots[s]] = longTerms[rank];
      longIndex[rank] = slots[s];
    }
  }
}
