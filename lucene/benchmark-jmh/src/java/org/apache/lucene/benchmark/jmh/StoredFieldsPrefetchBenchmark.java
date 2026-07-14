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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the {@code StoredFields#prefetch(docID)} path end-to-end. This simulates the common
 * "collect top-N hits, then retrieve their stored fields" pattern, which under memory pressure is a
 * batch of random, large-block reads in the {@code .fdt} file.
 *
 * <pre>
 * java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar \
 *   StoredFieldsPrefetchBenchmark \
 *   -jvmArgs "--enable-native-access=ALL-UNNAMED -Xms2g -Xmx2g \
 *             -Dbench.indexPath=/data/sf-index \
 *             -Dbench.numDocs=2000000 -Dbench.docSizeBytes=4096 \
 *             -Dbench.readAdvice=RANDOM -Dbench.prefetchWindow=16" \
 *   -p topK=100
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(
    value = 2,
    jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
public class StoredFieldsPrefetchBenchmark {

  private static final String FIELD = "body";

  /** Number of top hits retrieved per op. */
  @Param({"10", "100"})
  public int topK;

  private static final String INDEX_PATH =
      getConfig("bench.indexPath", System.getProperty("java.io.tmpdir") + "/sf-prefetch-index");
  private static final int NUM_DOCS = Integer.parseInt(getConfig("bench.numDocs", "1000000"));
  private static final int DOC_SIZE_BYTES =
      Integer.parseInt(getConfig("bench.docSizeBytes", "4096"));
  private static final int PREFETCH_WINDOW =
      Integer.parseInt(getConfig("bench.prefetchWindow", "16"));
  private static final ReadAdvice READ_ADVICE =
      ReadAdvice.valueOf(
          getConfig("bench.readAdvice", "RANDOM").toUpperCase(java.util.Locale.ROOT));
  private static final boolean DROP_PAGE_CACHE =
      Boolean.parseBoolean(getConfig("bench.dropPageCache", "false"));

  private MMapDirectory directory;
  private DirectoryReader reader;
  LeafReader leaf;
  int maxDoc;

  private static String getConfig(String propKey, String defaultValue) {
    return System.getProperty(propKey, defaultValue);
  }

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Path path = Path.of(INDEX_PATH);
    Files.createDirectories(path);

    directory = new MMapDirectory(path);
    directory.setReadAdvice((name, ctx) -> Optional.of(READ_ADVICE));

    if (needsBuild(directory)) {
      buildIndex(directory);
    }

    reader = DirectoryReader.open(directory);
    if (reader.leaves().size() != 1) {
      throw new IllegalStateException(
          "expected a single segment (force-merged) but got " + reader.leaves().size());
    }
    leaf = reader.leaves().get(0).reader();
    maxDoc = leaf.maxDoc();
    if (maxDoc != NUM_DOCS) {
      throw new IllegalStateException("expected " + NUM_DOCS + " docs but index has " + maxDoc);
    }
  }

  private static boolean needsBuild(MMapDirectory dir) {
    try {
      if (DirectoryReader.indexExists(dir) == false) {
        return true;
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        return r.maxDoc() != NUM_DOCS;
      }
    } catch (IOException e) {
      return true;
    }
  }

  private static void buildIndex(MMapDirectory dir) throws IOException {
    // .fdt size is (~ numDocs * docSizeBytes), which we can size the working set against RAM.
    Random random = new Random(42);
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setUseCompoundFile(false);
    iwc.setRAMBufferSizeMB(1024);
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      byte[] payload = new byte[DOC_SIZE_BYTES];
      for (int i = 0; i < NUM_DOCS; i++) {
        random.nextBytes(payload);
        Document doc = new Document();
        doc.add(new StoredField(FIELD, payload.clone()));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      writer.commit();
    }
  }

  @Setup(Level.Iteration)
  public void maybeDropCaches() throws IOException {
    if (DROP_PAGE_CACHE) {
      dropPageCaches();
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (reader != null) {
      reader.close();
    }
    if (directory != null) {
      directory.close();
    }
  }

  @State(Scope.Thread)
  public static class ThreadState {
    StoredFields storedFields;
    ConsumingVisitor visitor;
    int[] docIDs;

    @Setup(Level.Trial)
    public void setup(StoredFieldsPrefetchBenchmark bench) throws IOException {
      storedFields = bench.leaf.storedFields();
      visitor = new ConsumingVisitor();
      docIDs = new int[bench.topK];
    }

    void pickRandomDocIDs(int maxDoc) {
      ThreadLocalRandom rng = ThreadLocalRandom.current();
      for (int i = 0; i < docIDs.length; i++) {
        docIDs[i] = rng.nextInt(maxDoc);
      }
    }
  }

  // ---------- baseline: no prefetch ----------

  @Benchmark
  @Threads(1)
  public void retrieveNoPrefetch_T01(ThreadState ts, Blackhole bh) throws IOException {
    doRetrieveNoPrefetch(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void retrieveNoPrefetch_T08(ThreadState ts, Blackhole bh) throws IOException {
    doRetrieveNoPrefetch(ts, bh);
  }

  // ---------- batched prefetch then read ----------

  @Benchmark
  @Threads(1)
  public void retrievePrefetchThenRead_T01(ThreadState ts, Blackhole bh) throws IOException {
    doRetrievePrefetchThenRead(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void retrievePrefetchThenRead_T08(ThreadState ts, Blackhole bh) throws IOException {
    doRetrievePrefetchThenRead(ts, bh);
  }

  // ---------- prefetch only, no read ----------

  @Benchmark
  @Threads(1)
  public void prefetchOnly_T01(ThreadState ts, Blackhole bh) throws IOException {
    doPrefetchOnly(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void prefetchOnly_T08(ThreadState ts, Blackhole bh) throws IOException {
    doPrefetchOnly(ts, bh);
  }

  // ---------- implementations ----------

  private void doRetrieveNoPrefetch(ThreadState ts, Blackhole bh) throws IOException {
    ts.pickRandomDocIDs(maxDoc);
    ts.visitor.bh = bh;
    for (int docID : ts.docIDs) {
      ts.storedFields.document(docID, ts.visitor);
    }
  }

  private void doRetrievePrefetchThenRead(ThreadState ts, Blackhole bh) throws IOException {
    ts.pickRandomDocIDs(maxDoc);
    ts.visitor.bh = bh;
    final int[] docIDs = ts.docIDs;
    final int window = Math.max(1, Math.min(PREFETCH_WINDOW, docIDs.length));
    for (int start = 0; start < docIDs.length; start += window) {
      int end = Math.min(start + window, docIDs.length);
      for (int i = start; i < end; i++) {
        ts.storedFields.prefetch(docIDs[i]);
      }
      for (int i = start; i < end; i++) {
        ts.storedFields.document(docIDs[i], ts.visitor);
      }
    }
  }

  private void doPrefetchOnly(ThreadState ts, Blackhole bh) throws IOException {
    ts.pickRandomDocIDs(maxDoc);
    for (int docID : ts.docIDs) {
      ts.storedFields.prefetch(docID);
    }
    bh.consume(ts.docIDs.length);
  }

  private static final class ConsumingVisitor extends StoredFieldVisitor {
    Blackhole bh;

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
      bh.consume(value.length);
      if (value.length > 0) {
        bh.consume(value[0]);
        bh.consume(value[value.length - 1]);
      }
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      return Status.YES;
    }
  }

  private static void dropPageCaches() throws IOException {
    if (Constants.MAC_OS_X) {
      exec("/usr/bin/sudo", "purge");
    } else if (Constants.LINUX) {
      exec("/usr/bin/sync");
      exec("/usr/bin/sudo", "/usr/bin/bash", "-c", "echo 3 > /proc/sys/vm/drop_caches");
    }
  }

  private static void exec(String... command) throws IOException {
    Process proc = new ProcessBuilder(command).inheritIO().start();
    try {
      int exitCode = proc.waitFor();
      if (exitCode != 0) {
        throw new IOException(
            "Command failed with exit code " + exitCode + ": " + String.join(" ", command));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for: " + String.join(" ", command), e);
    }
  }
}
