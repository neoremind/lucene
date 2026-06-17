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
import org.apache.lucene.store.ByteBuffersDataOutput;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class ByteBuffersDataOutputWriteStringBenchmark {

  @Param({
      "ascii_1",
      "ascii_10",
      "ascii_20",
      "ascii_30",
      "ascii_40",
      "ascii_medium",
      "ascii_long",
      "ascii_vlarge",
      "cjk_1",
      "cjk_10",
      "cjk_20",
      "cjk_30",
      "cjk_40",
      "cjk_medium",
      "cjk_long",
      "cjk_vlarge",
      "latin_ext_1",
      "latin_ext_10",
      "latin_ext_20",
      "latin_ext_30",
      "latin_ext_40",
      "latin_ext_medium",
      "latin_ext_long",
      "latin_ext_vlarge"
  })
  public String stringType;

  /** Pre-generated strings to write, cycled through during each invocation. */
  private String[] testStrings;

  /** Target bytes to write per invocation. Matches stored fields chunk sizes. */
  @Param({"81920", "491520"})
  public int targetBytes;

  /** Number of strings to write per invocation to reach targetBytes total output. */
  private int stringsPerInvocation;

  private static final int STRING_POOL_SIZE = 8192;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    testStrings = new String[STRING_POOL_SIZE];

    int avgBytesPerString;
    switch (stringType) {
      case "ascii_1":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 1);
        }
        avgBytesPerString = 2;
        break;
      case "ascii_10":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 8 + random.nextInt(5));
        }
        avgBytesPerString = 11;
        break;
      case "ascii_20":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 18 + random.nextInt(5));
        }
        avgBytesPerString = 21;
        break;
      case "ascii_30":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 28 + random.nextInt(5));
        }
        avgBytesPerString = 31;
        break;
      case "ascii_40":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 38 + random.nextInt(5));
        }
        avgBytesPerString = 41;
        break;
      case "ascii_medium":
        // ~100 bytes avg
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 50 + random.nextInt(100));
        }
        avgBytesPerString = 100;
        break;
      case "ascii_long":
        // ~1024 bytes avg
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 900 + random.nextInt(250));
        }
        avgBytesPerString = 1024;
        break;
      case "ascii_vlarge":
        // ~8192 bytes avg
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 7000 + random.nextInt(2400));
        }
        avgBytesPerString = 8192;
        break;
      case "cjk_1":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 1);
        }
        avgBytesPerString = 4;
        break;
      case "cjk_10":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 8 + random.nextInt(5));
        }
        avgBytesPerString = 32;
        break;
      case "cjk_20":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 18 + random.nextInt(5));
        }
        avgBytesPerString = 62;
        break;
      case "cjk_30":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 28 + random.nextInt(5));
        }
        avgBytesPerString = 92;
        break;
      case "cjk_40":
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 38 + random.nextInt(5));
        }
        avgBytesPerString = 122;
        break;
      case "cjk_medium":
        // ~100 chars CJK = ~300 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 50 + random.nextInt(100));
        }
        avgBytesPerString = 300;
        break;
      case "cjk_long":
        // ~500 chars CJK = ~1500 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 400 + random.nextInt(200));
        }
        avgBytesPerString = 1500;
        break;
      case "cjk_vlarge":
        // ~6000 chars CJK = ~18000 UTF-8 bytes (beyond 5461 char threshold for 2-byte VInt)
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 5500 + random.nextInt(1000));
        }
        avgBytesPerString = 18000;
        break;
      case "latin_ext_short":
        // ~10 chars Latin extended = ~20 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 5 + random.nextInt(15));
        }
        avgBytesPerString = 20;
        break;
      case "latin_ext_medium":
        // ~100 chars Latin extended (Cyrillic, Greek, accented) = ~200 UTF-8 bytes (2 bytes/char)
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 50 + random.nextInt(100));
        }
        avgBytesPerString = 200;
        break;
      case "latin_ext_long":
        // ~500 chars Latin extended = ~1000 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 400 + random.nextInt(200));
        }
        avgBytesPerString = 1000;
        break;
      case "latin_ext_vlarge":
        // ~6000 chars Latin extended = ~12000 UTF-8 bytes (beyond 5461 char threshold)
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 5500 + random.nextInt(1000));
        }
        avgBytesPerString = 12000;
        break;
      default:
        throw new IllegalArgumentException("Unknown stringType: " + stringType);
    }

    stringsPerInvocation = targetBytes / avgBytesPerString;

    reusableOutput = ByteBuffersDataOutput.newResettableInstance();
  }

  private ByteBuffersDataOutput reusableOutput;

  // --- Benchmarks ---

  private ByteBuffersDataOutput getOutput() {
    reusableOutput.reset();
    return reusableOutput;
  }

  @Benchmark
  public void newImpl(Blackhole bh) {
    // New optimized implementation (now the default writeString)
    ByteBuffersDataOutput output = getOutput();
    for (int i = 0; i < stringsPerInvocation; i++) {
      output.writeString(testStrings[i % STRING_POOL_SIZE]);
    }
    bh.consume(output.size());
  }

  @Benchmark
  public void prevImpl(Blackhole bh) {
    // Previous implementation (PR#13863): calcUTF16toUTF8Length + writeVInt + direct encode
    ByteBuffersDataOutput output = getOutput();
    for (int i = 0; i < stringsPerInvocation; i++) {
      output.writeStringPrev(testStrings[i % STRING_POOL_SIZE]);
    }
    bh.consume(output.size());
  }

  private static String randomAscii(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      chars[i] = (char) (32 + random.nextInt(95));
    }
    return new String(chars);
  }

  /**
   * Generates realistic CJK text: ~90% CJK Unified Ideographs (3-byte UTF-8), ~9% ASCII
   * punctuation/digits (1-byte), ~1% surrogate pairs (emoji, rare CJK-B characters, 4-byte UTF-8).
   */
  private static String randomCjk(Random random, int length) {
    char[] chars = new char[length + 1]; // +1 room for potential surrogate pair expansion
    int pos = 0;
    for (int i = 0; i < length && pos < chars.length - 1; i++) {
      int roll = random.nextInt(100);
      if (roll < 90) {
        // CJK Unified Ideographs: U+4E00–U+9FFF (3 bytes in UTF-8)
        chars[pos++] = (char) (0x4E00 + random.nextInt(0x9FFF - 0x4E00));
      } else if (roll < 99) {
        // ASCII punctuation/digits mixed in
        chars[pos++] = (char) (0x30 + random.nextInt(10)); // 0-9
      } else {
        // Surrogate pair (emoji, rare chars): 4 bytes in UTF-8
        if (pos < chars.length - 1) {
          chars[pos++] = (char) (0xD800 + random.nextInt(0x400)); // high surrogate
          chars[pos++] = (char) (0xDC00 + random.nextInt(0x400)); // low surrogate
        } else {
          chars[pos++] = (char) (0x4E00 + random.nextInt(0x9FFF - 0x4E00));
        }
      }
    }
    return new String(chars, 0, pos);
  }

  /**
   * Generates realistic Latin-extended text: ~80% 2-byte chars (Cyrillic, Greek, accented Latin:
   * U+0080–U+07FF), ~15% ASCII, ~5% 3-byte (rare symbols).
   */
  private static String randomLatinExtended(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      int roll = random.nextInt(100);
      if (roll < 80) {
        // 2-byte UTF-8: Cyrillic (U+0400–U+04FF), Greek (U+0370–U+03FF), accented (U+00C0–U+024F)
        chars[i] = (char) (0x0080 + random.nextInt(0x0700));
      } else if (roll < 95) {
        // ASCII
        chars[i] = (char) (0x41 + random.nextInt(26)); // A-Z
      } else {
        // 3-byte (misc symbols U+2000–U+2BFF)
        chars[i] = (char) (0x2000 + random.nextInt(0xBFF));
      }
    }
    return new String(chars);
  }
}
