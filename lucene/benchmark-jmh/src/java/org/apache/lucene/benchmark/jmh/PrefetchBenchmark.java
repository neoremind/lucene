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
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Prerequisites: Create a test file externally before running. Example (16GB file):
 *
 * <pre>
 *   dd if=/dev/urandom of=/tmp/prefetch_bench_data bs=4096 count=4194304  # 16GB file
 * </pre>
 *
 * <p>Build with `./gradlew -p lucene/benchmark-jmh assemble` and run with:
 *
 * <pre>
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-11.0.0-SNAPSHOT.jar \
 *   PrefetchBenchmark \
 *   -p dataDir=/tmp -p fileName=prefetch_bench_data -p fileSizeGB=4
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class PrefetchBenchmark {

  @Param("/tmp")
  public String dataDir;

  @Param("prefetch_bench_data")
  public String fileName;

  @Param({"4"})
  public int fileSizeGB;

  /** Number of bytes to prefetch per call. */
  @Param({"8192", "16384", "65536"})
  public int prefetchLength;

  private MMapDirectory dir;
  private IndexInput inputRandom;
  private IndexInput inputNormal;
  private IndexInput inputSequential;
  private IndexInput inputNoPrefetch;
  private long accessRange;
  private byte[] readBuf;

  private static final int READ_CHUNK = 4096;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    dir = new MMapDirectory(Path.of(dataDir));
    dir.setReadAdvice(MMapDirectory.ADVISE_BY_CONTEXT);

    accessRange = (long) fileSizeGB * 1024L * 1024L * 1024L;

    inputRandom = dir.openInput(fileName, IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
    inputNormal = dir.openInput(fileName, IOContext.DEFAULT);
    inputSequential =
        dir.openInput(fileName, IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL));
    inputNoPrefetch = dir.openInput(fileName, IOContext.DEFAULT);
    readBuf = new byte[READ_CHUNK];
  }

  @Setup(Level.Iteration)
  public void dropPageCache() throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder(
            "sudo", "bash", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches");
    pb.inheritIO();
    Process p = pb.start();
    p.waitFor();
    System.out.println("Clearing page cache done");
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    IOUtils.close(inputRandom, inputNormal, inputSequential, inputNoPrefetch, dir);
    inputRandom = null;
    inputNormal = null;
    inputSequential = null;
    inputNoPrefetch = null;
    dir = null;
  }

  @Benchmark
  public void prefetchRandomThenRead(Blackhole bh) throws IOException {
    long offset = ThreadLocalRandom.current().nextLong(accessRange - prefetchLength);
    bh.consume(inputRandom.prefetch(offset, prefetchLength));
    inputRandom.seek(offset);
    int remaining = prefetchLength;
    while (remaining > 0) {
      int toRead = Math.min(READ_CHUNK, remaining);
      inputRandom.readBytes(readBuf, 0, toRead);
      remaining -= toRead;
    }
    bh.consume(readBuf);
  }

  @Benchmark
  public void prefetchNormalThenRead(Blackhole bh) throws IOException {
    long offset = ThreadLocalRandom.current().nextLong(accessRange - prefetchLength);
    bh.consume(inputNormal.prefetch(offset, prefetchLength));
    inputNormal.seek(offset);
    int remaining = prefetchLength;
    while (remaining > 0) {
      int toRead = Math.min(READ_CHUNK, remaining);
      inputNormal.readBytes(readBuf, 0, toRead);
      remaining -= toRead;
    }
    bh.consume(readBuf);
  }

  @Benchmark
  public void prefetchSequentialThenRead(Blackhole bh) throws IOException {
    long offset = ThreadLocalRandom.current().nextLong(accessRange - prefetchLength);
    bh.consume(inputSequential.prefetch(offset, prefetchLength));
    inputSequential.seek(offset);
    int remaining = prefetchLength;
    while (remaining > 0) {
      int toRead = Math.min(READ_CHUNK, remaining);
      inputSequential.readBytes(readBuf, 0, toRead);
      remaining -= toRead;
    }
    bh.consume(readBuf);
  }

  @Benchmark
  public void noPrefetchReadRandom(Blackhole bh) throws IOException {
    long offset = ThreadLocalRandom.current().nextLong(accessRange - prefetchLength);
    inputNoPrefetch.seek(offset);
    int remaining = prefetchLength;
    while (remaining > 0) {
      int toRead = Math.min(READ_CHUNK, remaining);
      inputNoPrefetch.readBytes(readBuf, 0, toRead);
      remaining -= toRead;
    }
    bh.consume(readBuf);
  }
}
