package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 3)
@Fork(
    value = 2,
    jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "-Xms1g", "-Xmx1g"})
public class MMapPrefetchAndIsLoadBenchmark {

  @Param({"4096", "16384"})
  public int readSize;

  protected static final long FILE_SIZE =
      Long.parseLong(getConfigFromEnvOrProp("BENCH_FILE_SIZE_MB", "bench.fileSizeMB", "1024"))
          * 1024L
          * 1024L;

  protected static final String BENCH_FILE =
      getConfigFromEnvOrProp("BENCH_FILE", "bench.file", "/tmp/pread-bench.dat");

  protected static final boolean DROP_PAGE_CACHE =
      Boolean.parseBoolean(
          getConfigFromEnvOrProp("BENCH_DROP_CACHES", "bench.dropPageCache", "false"));

  protected static final int POSIX_MADV_RANDOM = 1;
  protected static final int POSIX_MADV_SEQUENTIAL = 2;
  protected static final int POSIX_MADV_WILLNEED = 3;

  // FFI handles
  protected static final MethodHandle POSIX_MADVISE;
  protected static final int PAGE_SIZE;

  static {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup stdlib = linker.defaultLookup();

    POSIX_MADVISE =
        findFunction(
            linker,
            stdlib,
            "posix_madvise",
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));

    try {
      PAGE_SIZE =
          (int)
              findFunction(
                      linker, stdlib, "getpagesize", FunctionDescriptor.of(ValueLayout.JAVA_INT))
                  .invokeExact();
    } catch (Throwable e) {
      throw new RuntimeException("getpagesize() failed", e);
    }
  }

  @SuppressWarnings("restricted") // unsafe functionality is used
  private static MethodHandle findFunction(
      Linker linker, SymbolLookup lookup, String name, FunctionDescriptor desc) {
    final MemorySegment symbol =
        lookup
            .find(name)
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Platform has no symbol for '" + name + "' in libc."));
    return linker.downcallHandle(symbol, desc);
  }

  protected Path benchFile;
  protected FileChannel fileChannel;
  protected MemorySegment mmapSegmentNormal;
  protected MemorySegment mmapSegmentRandom;
  protected Arena arena;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    benchFile = Path.of(BENCH_FILE);
    if (!Files.exists(benchFile)) {
      throw new IOException(
          "Benchmark file not found: "
              + benchFile
              + "\nCreate it with: dd if=/dev/urandom of="
              + BENCH_FILE
              + " bs=1M count="
              + (FILE_SIZE / (1024 * 1024)));
    }

    long size = Files.size(benchFile);
    if (size < FILE_SIZE) {
      throw new IOException(
          "Benchmark file too small: "
              + size
              + " bytes, expected at least "
              + FILE_SIZE
              + "\nRecreate with: dd if=/dev/urandom of="
              + BENCH_FILE
              + " bs=1M count="
              + (FILE_SIZE / (1024 * 1024)));
    }

    arena = Arena.ofShared();
    fileChannel = FileChannel.open(benchFile, StandardOpenOption.READ);

    mmapSegmentNormal = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    mmapSegmentRandom = fileChannel.map(MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    madvise(mmapSegmentRandom, POSIX_MADV_RANDOM);

    maxOffset = FILE_SIZE - readSize;
    maxAlignedOffset = (maxOffset / PAGE_SIZE) * PAGE_SIZE;
  }

  protected int madvise(MemorySegment segment, int advice) throws IOException {
    final int rc;
    try {
      rc = (int) POSIX_MADVISE.invokeExact(segment, segment.byteSize(), advice);
    } catch (Throwable t) {
      throw new RuntimeException("posix_madvise failed", t);
    }
    if (rc != 0) {
      throw new IOException("posix_madvise(" + advice + ") returned " + rc);
    }
    return rc;
  }

  @TearDown(Level.Trial)
  @SuppressWarnings({"restricted", "unused"})
  public void tearDown() throws Exception {
    if (fileChannel != null) {
      fileChannel.close();
    }
    if (arena != null) {
      arena.close();
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    if (DROP_PAGE_CACHE) {
      dropPageCaches();
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

  /** Reads a config value from env var first, then system property, then default. */
  protected static String getConfigFromEnvOrProp(
      String envKey, String propKey, String defaultValue) {
    String env = System.getenv(envKey);
    if (env != null && !env.isEmpty()) {
      return env;
    }
    return System.getProperty(propKey, defaultValue);
  }

  private long maxOffset;
  private long maxAlignedOffset;

  @Benchmark
  @Threads(1)
  public void mmapRandomPrefetch_T01(Blackhole bh) throws IOException {
    doPrefetch(mmapSegmentRandom, bh);
  }

  @Benchmark
  @Threads(1)
  public void mmapRandomIsLoad_T01(Blackhole bh) throws IOException {
    isLoad(mmapSegmentRandom, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapRandomPrefetch_T08(Blackhole bh) throws IOException {
    doPrefetch(mmapSegmentRandom, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapRandomIsLoad_T08(Blackhole bh) throws IOException {
    isLoad(mmapSegmentRandom, bh);
  }

  private void doPrefetch(MemorySegment memorySegment, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    long offsets = rng.nextLong(maxOffset);
    long offsetInPage = (memorySegment.address() + offsets) % PAGE_SIZE;
    long alignedOffset = offsets - offsetInPage;
    long alignedLength = readSize + offsetInPage;
    MemorySegment slice = memorySegment.asSlice(alignedOffset, alignedLength);
    int rc = madvise(slice, POSIX_MADV_WILLNEED);
    bh.consume(rc);
  }

  private void isLoad(MemorySegment memorySegment, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    long offsets = rng.nextLong(maxOffset);
    long offsetInPage = (memorySegment.address() + offsets) % PAGE_SIZE;
    long alignedOffset = offsets - offsetInPage;
    long alignedLength = readSize + offsetInPage;
    MemorySegment slice = memorySegment.asSlice(alignedOffset, alignedLength);
    bh.consume(slice.isLoaded());
  }
}
