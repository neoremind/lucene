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
@OperationsPerInvocation(TermsHashPerFieldBenchmark.STREAM_LEN)
@Fork(
    value = 2,
    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
public class TermsHashPerFieldBenchmark {

  /** Number of add() calls per benchmark invocation. */
  static final int STREAM_LEN = 1 << 20; // 2M tokens

  /** Fixed vocabulary size = STREAM_LEN / 8. */
  private static final int VOCAB_SIZE = STREAM_LEN / 8; // 256K unique terms

  private static final long SEED = 42L;

  /**
   * Fraction of vocabulary that is short (1..8 bytes). Rest is long (9..32 bytes). Ignored for
   * uuid.
   */
  @Param({"0.75"})
  double shortRatio;

  /**
   * Workload type:
   * <ul>
   *   <li>"1.0" — mild skew, most vocab terms accessed relatively evenly
   *   <li>"3.0" — moderate Zipf: hottest 10% of vocab get ~46% of accesses
   *   <li>"6.0" — heavy Zipf: hottest 10% of vocab get ~68% of accesses
   *   <li>"UUID" — 16-byte unique terms, 1 per doc (primary key pattern, all newTerm)
   * </ul>
   */
  @Param({"1.0", "4.0", "8.0", "UUID"})
  String skew;

  /** Number of documents (for random: ~2048 tokens/doc; uuid always 1 token/doc). */
  @Param({"1000"})
  int numDocs;

  private BytesRef[] stream;
  private int tokensPerDoc;

  private FreqProxTermsWriter termsHash;
  private TermsHashPerField perField;

  private static final int CHUNK_SIZE = 1 << 16; // 64KB per chunk

  @Setup(Level.Trial)
  public void generateData() {
    Random rng = new Random(SEED);
    boolean isUUID = "UUID".equals(skew);

    stream = new BytesRef[STREAM_LEN];

    if (isUUID) {
      // UUID: every term is unique 16 bytes, 1 per doc.
      // Pack into 64K chunks, slice into BytesRef views.
      int termLen = 16;
      int termsPerChunk = CHUNK_SIZE / termLen;
      byte[] chunk = new byte[CHUNK_SIZE];
      int posInChunk = CHUNK_SIZE; // force first chunk allocation
      for (int i = 0; i < STREAM_LEN; i++) {
        if (posInChunk + termLen > CHUNK_SIZE) {
          chunk = new byte[CHUNK_SIZE];
          posInChunk = 0;
        }
        // Write random 16 bytes directly into chunk
        for (int b = 0; b < termLen; b++) {
          chunk[posInChunk + b] = (byte) rng.nextInt(256);
        }
        stream[i] = new BytesRef(chunk, posInChunk, termLen);
        posInChunk += termLen;
      }
      tokensPerDoc = 1;
    } else {
      double skewValue = Double.parseDouble(skew);
      // Generate vocab terms (one-off, simple allocation)
      BytesRef[] vocab = new BytesRef[VOCAB_SIZE];
      for (int i = 0; i < VOCAB_SIZE; i++) {
        int len = (rng.nextDouble() < shortRatio) ? 1 + rng.nextInt(8) : 9 + rng.nextInt(24);
        byte[] bytes = new byte[len];
        rng.nextBytes(bytes);
        vocab[i] = new BytesRef(bytes, 0, len);
      }

      // Fill stream: pick from vocab by skew, hard copy into chunked storage
      byte[] sChunk = new byte[CHUNK_SIZE];
      int sPos = CHUNK_SIZE; // force first chunk allocation
      for (int i = 0; i < STREAM_LEN; i++) {
        BytesRef src = vocab[skewedIndex(rng, VOCAB_SIZE, skewValue)];
        if (sPos + src.length > CHUNK_SIZE) {
          sChunk = new byte[CHUNK_SIZE];
          sPos = 0;
        }
        System.arraycopy(src.bytes, src.offset, sChunk, sPos, src.length);
        stream[i] = new BytesRef(sChunk, sPos, src.length);
        sPos += src.length;
      }
      vocab = null; // eligible for GC
      tokensPerDoc = STREAM_LEN / numDocs;
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

  /**
   * Picks a vocabulary index biased toward 0 (hottest). Models Zipf's law: index = (int)(vocabSize
   * * pow(uniform, skew)). Hot terms (low indices) get exponentially more accesses.
   */
  private static int skewedIndex(Random rng, int vocabSize, double skew) {
    int idx = (int) (vocabSize * Math.pow(rng.nextDouble(), skew));
    return Math.min(idx, vocabSize - 1);
  }
}
