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

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.FreqProxTermsWriter;
import org.apache.lucene.index.FreqProxTermsWriterPerField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermVectorsConsumer;
import org.apache.lucene.index.TermsHashPerField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
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
 * Benchmark for {@link TermsHashPerField#add(BytesRef, int)} using real {@link
 * FreqProxTermsWriter} and {@link org.apache.lucene.index.FreqProxTermsWriterPerField}.
 *
 * <p>Measures the isolated TermsHashPerField hot path: term deduplication (BytesRefHash), stream
 * slice allocation, and VInt-encoded postings writes (doc deltas + frequencies + positions).
 *
 * <pre>
 * ./gradlew -p lucene/benchmark-jmh assemble
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-11.0.0-SNAPSHOT.jar \
 *   TermsHashPerFieldBenchmark -t 1
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2)
@OperationsPerInvocation(TermsHashPerFieldBenchmark.STREAM_LEN)
public class TermsHashPerFieldBenchmark {

  /** Number of add() calls per benchmark invocation. */
  static final int STREAM_LEN = 1 << 20; // ~1M tokens

  private static final long SEED = 42L;

  /** Total unique terms. Controls ids[] table size and cache behavior. */
  @Param({"65536", "262144", "1048576"})
  int vocabSize;

  /**
   * Fraction of vocabulary that is short (1..8 bytes). Rest is long (9..32 bytes). Ignored for
   * uuid.
   */
  @Param({"0.75"})
  double shortRatio;

  /**
   * Power-law skew or "UUID" for the UUID workload. skew=1.0 = sequential cycling; skew=3.0+ =
   * hot terms; "UUID" = 16-byte unique terms, 1 per doc, sequential (primary key pattern).
   */
  @Param({"1.0", "3.0", "6.0", "UUID"})
  String skew;

  /** Number of documents (for random: ~1024 tokens/doc; uuid always 1 token/doc). */
  @Param({"1000"})
  int numDocs;

  private BytesRef[] stream;
  private int tokensPerDoc;

  private FreqProxTermsWriter termsHash;
  private TermsHashPerField perField;

  @Setup(Level.Trial)
  public void generateData() {
    Random rng = new Random(SEED);
    boolean isUUID = "UUID".equals(skew);

    // Generate vocabSize unique terms directly into the first batch of stream[].
    // Then fill remaining batches by copying + shuffling — each batch contains the
    // same vocabSize terms in a random order. This gives:
    // - Sequential memory access (no indirection in the hot loop)
    // - Random term order within each batch (realistic hash access pattern)
    // - Controlled term reuse: each term seen STREAM_LEN/vocabSize times across batches
    stream = new BytesRef[STREAM_LEN];

    // First batch: generate unique terms
    if (isUUID) {
      for (int i = 0; i < vocabSize; i++) {
        stream[i] = randomUUIDTerm(rng);
      }
      tokensPerDoc = 1;
    } else {
      for (int i = 0; i < vocabSize; i++) {
        stream[i] = randomTerm(rng, shortRatio);
      }
      tokensPerDoc = STREAM_LEN / numDocs;
    }

    // Shuffle the first batch
    for (int i = vocabSize - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      BytesRef tmp = stream[i];
      stream[i] = stream[j];
      stream[j] = tmp;
    }

    // Fill remaining batches: copy first batch then shuffle each
    for (int batch = 1; batch * vocabSize < STREAM_LEN; batch++) {
      int base = batch * vocabSize;
      int len = Math.min(vocabSize, STREAM_LEN - base);
      System.arraycopy(stream, 0, stream, base, len);
      // Shuffle this batch
      for (int i = len - 1; i > 0; i--) {
        int j = rng.nextInt(i + 1);
        BytesRef tmp = stream[base + i];
        stream[base + i] = stream[base + j];
        stream[base + j] = tmp;
      }
    }
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    termsHash = createTermsHash();
    perField = createPerField(termsHash);
    perField.start(null, true);
  }

  @Benchmark
  public void indexSegment(Blackhole bh) throws IOException {
    final BytesRef[] stream = this.stream;
    final int tokensPerDoc = this.tokensPerDoc;
    final FreqProxTermsWriter termsHash = this.termsHash;
    final TermsHashPerField perField = this.perField;

    int docID = 0;
    int tokenInDoc = 0;
    for (int i = 0; i < STREAM_LEN; i++) {
      if (tokenInDoc == tokensPerDoc) {
        perField.finish();
        termsHash.startDocument();
        docID++;
        tokenInDoc = 0;
      }
      perField.add(stream[i], docID);
      tokenInDoc++;
    }
    perField.finish();
    bh.consume(perField.getNumTerms());
  }

  @Benchmark
  public void indexSegmentAndSort(Blackhole bh) throws IOException {
    final BytesRef[] stream = this.stream;
    final int tokensPerDoc = this.tokensPerDoc;
    final FreqProxTermsWriter termsHash = this.termsHash;
    final TermsHashPerField perField = this.perField;

    int docID = 0;
    int tokenInDoc = 0;
    for (int i = 0; i < STREAM_LEN; i++) {
      if (tokenInDoc == tokensPerDoc) {
        perField.finish();
        termsHash.startDocument();
        docID++;
        tokenInDoc = 0;
      }
      perField.add(stream[i], docID);
      tokenInDoc++;
    }
    perField.finish();
    perField.sortTerms();
    bh.consume(perField.getSortedTermIDs());
  }

  // ===== Factory methods =====

  private static FreqProxTermsWriter createTermsHash() {
    IntBlockPool.Allocator intAllocator = new IntBlockPool.DirectAllocator();
    ByteBlockPool.Allocator byteAllocator = new ByteBlockPool.DirectAllocator();
    Counter bytesUsed = Counter.newCounter();

    Directory dir = new ByteBuffersDirectory();
    SegmentInfo segInfo =
        new SegmentInfo(
            dir, Version.LATEST, Version.LATEST, "_0", 0, false, false,
            Codec.getDefault(), Collections.emptyMap(), StringHelper.randomId(),
            Collections.emptyMap(), null);

    TermVectorsConsumer termVectors =
        new TermVectorsConsumer(intAllocator, byteAllocator, dir, segInfo, Codec.getDefault());

    return new FreqProxTermsWriter(intAllocator, byteAllocator, bytesUsed, termVectors);
  }

  private static TermsHashPerField createPerField(FreqProxTermsWriter termsHash) {
    FieldInvertState invertState =
        new FieldInvertState(
            Version.LATEST.major, "body", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    FieldInfo fieldInfo =
        new FieldInfo(
            "body", 0, false, false, false,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            DocValuesType.NONE, DocValuesSkipIndexType.NONE, -1,
            Collections.emptyMap(), 0, 0, 0, 0,
            VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN,
            false, false);

    // Construct directly with null nextPerField to avoid TermVectorsConsumer overhead
    return new FreqProxTermsWriterPerField(invertState, termsHash, fieldInfo, null);
  }

  // ===== Helpers =====

  private static BytesRef randomTerm(Random rng, double shortRatio) {
    int len = (rng.nextDouble() < shortRatio) ? 1 + rng.nextInt(8) : 9 + rng.nextInt(24);
    byte[] bytes = new byte[len];
    rng.nextBytes(bytes);
    return new BytesRef(bytes, 0, len);
  }

  /** A 16-byte UUID term (simulates document IDs, transaction IDs, session tokens, etc.). */
  private static BytesRef randomUUIDTerm(Random rng) {
    byte[] bytes = new byte[16];
    rng.nextBytes(bytes);
    return new BytesRef(bytes, 0, 16);
  }
}
