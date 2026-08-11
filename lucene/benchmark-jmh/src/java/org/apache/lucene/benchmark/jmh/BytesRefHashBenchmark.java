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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for {@link BytesRefHash#add(BytesRef)} measuring the effect of load factor on the
 * indexing hot path. Designed so that the same JAR can be run on two source branches (e.g. baseline
 * 1/2 LF vs candidate 3/4 LF) and the reports diffed.
 *
 * <h2>Workload model</h2>
 *
 * A single unified vocabulary of {@code vocabSize} terms, accessed with a power-law skew. Hot terms
 * (low indices) are accessed far more often than cold terms (high indices). The stream starts with
 * an empty hash — hot terms get inserted early and become hits on subsequent accesses; cold tail
 * terms get inserted later and rarely repeat. This naturally produces the ~98% hit rate and
 * diminishing new-term rate observed in real indexing, without needing separate hit/miss pools.
 *
 * <p>Knobs:
 *
 * <ul>
 *   <li>{@code vocabSize} — total unique terms. Controls the ids[] working set size.
 *   <li>{@code skew} — power-law exponent. {@code skew=2} means the hottest 10% of terms get ~32%
 *       of accesses; {@code skew=3} gives ~46%. {@code skew=1} is uniform (no hot spot).
 *   <li>{@code shortRatio} — fraction of vocabulary that is short (1..8 bytes); the rest is long
 *       (9..32 bytes).
 * </ul>
 *
 * <h2>Benchmarks</h2>
 *
 * <ul>
 *   <li>{@link #add} — models a full DWPT segment lifecycle: empty hash, stream of adds with
 *       natural growth and rehashes. This is the most realistic benchmark.
 * </ul>
 *
 * <pre>
 * ./gradlew -p lucene/benchmark-jmh assemble
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-11.0.0-SNAPSHOT.jar \
 *   BytesRefHashBenchmark -t 1
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
@OperationsPerInvocation(BytesRefHashBenchmark.STREAM_LEN)
public class BytesRefHashBenchmark {

  /** Number of add() calls per benchmark invocation. */
  static final int STREAM_LEN = 1 << 21;

  private static final long SEED = 42L;

  /** Total unique terms. Controls ids[] table size and cache behavior. */
  @Param({"65536", "262144", "1048576", "4194304"})
  int vocabSize;

  /**
   * Fraction of vocabulary that is short (1..8 bytes). The rest is long (9..32 bytes). 0.75 matches
   * typical natural language where most distinct terms are short.
   */
  @Param({"0.75"})
  double shortRatio;

  /**
   * Power-law skew. Higher values concentrate accesses on hot terms. skew=2 is moderate (hottest
   * 10% get 32% of accesses); skew=3 is heavy (hottest 10% get 46%).
   */
  @Param({"1.0", "3.0", "5.0"})
  double skew;

  /** The vocabulary: indexed 0 = hottest. */
  private BytesRef[] terms;

  /** Pre-computed access plan: each entry is an index into terms[]. */
  private int[] plan;

  private BytesRefHash hash;

  @Setup(Level.Trial)
  public void generateData() {
    Random rng = new Random(SEED);

    // Build vocabulary
    terms = new BytesRef[vocabSize];
    for (int i = 0; i < vocabSize; i++) {
      terms[i] = randomTerm(rng, shortRatio);
    }

    // Build access plan with power-law skew
    plan = new int[STREAM_LEN];
    for (int i = 0; i < STREAM_LEN; i++) {
      plan[i] = skewedIndex(rng, vocabSize, skew);
    }
  }

  @Setup(Level.Invocation)
  public void createHash() {
    hash = new BytesRefHash();
  }

  /** Close the old hash and hint GC between invocations to avoid mid-measurement pauses. */
  @TearDown(Level.Invocation)
  public void cleanup() {
    hash.close();
  }

  @Benchmark
  @OperationsPerInvocation(STREAM_LEN)
  public void add(Blackhole bh) {
    final BytesRefHash hash = this.hash;
    final BytesRef[] terms = this.terms;
    final int[] plan = this.plan;
    long sink = 0;
    for (int i = 0; i < STREAM_LEN; i++) {
      sink += hash.add(terms[plan[i]]);
    }
    bh.consume(sink);
  }

  /**
   * Picks a vocabulary index biased toward 0 (hottest). index = (int)(vocabSize *
   * pow(uniform,skew)). skew=1 is uniform; larger values concentrate on low indices.
   */
  private static int skewedIndex(Random rng, int vocabSize, double skew) {
    int idx = (int) (vocabSize * Math.pow(rng.nextDouble(), skew));
    if (idx >= vocabSize) {
      idx = vocabSize - 1;
    }
    return idx;
  }

  /**
   * A random term: short (1..8 bytes) with probability shortRatio, otherwise long (9..32 bytes).
   */
  private static BytesRef randomTerm(Random rng, double shortRatio) {
    int len = (rng.nextDouble() < shortRatio) ? 1 + rng.nextInt(8) : 9 + rng.nextInt(24);
    byte[] bytes = new byte[len];
    rng.nextBytes(bytes);
    return new BytesRef(bytes, 0, len);
  }
}
