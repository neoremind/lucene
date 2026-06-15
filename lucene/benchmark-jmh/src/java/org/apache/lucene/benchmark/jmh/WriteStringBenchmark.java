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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
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
public class WriteStringBenchmark {

  private static final int TARGET_BYTES = 8 * 1024 * 1024; // 1 MB
  private static final int STRING_POOL_SIZE = 8192;

  @Param({
    "ascii_short",
    "ascii_medium",
    "ascii_long",
    "ascii_vlarge",
    "cjk_short",
    "cjk_medium",
    "cjk_long",
    "cjk_vlarge",
    "latin_ext_short",
    "latin_ext_medium",
    "latin_ext_long",
    "latin_ext_vlarge"
  })
  public String stringType;

  @Param({"true", "false"})
  public boolean resettable;

  /** Pre-generated strings to write, cycled through during each invocation. */
  private String[] testStrings;

  /** Number of strings to write per invocation to reach TARGET_BYTES total output. */
  private int stringsPerInvocation;

  private ByteBuffersDataOutput reusableOutput;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    testStrings = new String[STRING_POOL_SIZE];

    int avgBytesPerString;
    switch (stringType) {
      case "ascii_short":
        // ~12 bytes avg (10 chars + VInt)
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomAscii(random, 5 + random.nextInt(15));
        }
        avgBytesPerString = 12;
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
      case "cjk_short":
        // ~10 chars CJK = ~30 UTF-8 bytes (3 bytes/char), with rare surrogates (~1%)
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 5 + random.nextInt(15));
        }
        avgBytesPerString = 32;
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
        // ~3000 chars CJK = ~9000 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomCjk(random, 2500 + random.nextInt(1000));
        }
        avgBytesPerString = 9000;
        break;
      case "latin_ext_short":
        // ~10 chars Latin extended = ~20 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 5 + random.nextInt(15));
        }
        avgBytesPerString = 20;
        break;
      case "latin_ext_medium":
        // ~100 chars Latin extended = ~200 UTF-8 bytes (2 bytes/char)
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
        // ~3000 chars Latin extended = ~6000 UTF-8 bytes
        for (int i = 0; i < STRING_POOL_SIZE; i++) {
          testStrings[i] = randomLatinExtended(random, 2500 + random.nextInt(1000));
        }
        avgBytesPerString = 6000;
        break;
      default:
        throw new IllegalArgumentException("Unknown stringType: " + stringType);
    }

    stringsPerInvocation = TARGET_BYTES / avgBytesPerString;

    if (resettable) {
      reusableOutput = ByteBuffersDataOutput.newResettableInstance();
    }
  }

  // --- Benchmarks ---

  private ByteBuffersDataOutput getOutput() {
    if (resettable) {
      reusableOutput.reset();
      return reusableOutput;
    }
    return new ByteBuffersDataOutput();
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

  @Benchmark
  public void oldImpl(Blackhole bh) throws IOException {
    // Old implementation (pre-PR#13863): BytesRef allocation + writeBytes
    ByteBuffersDataOutput output = getOutput();
    for (int i = 0; i < stringsPerInvocation; i++) {
      writeStringOld(output, testStrings[i % STRING_POOL_SIZE]);
    }
    bh.consume(output.size());
  }

  // --- Old implementation (pre-PR#13863) ---

  private static final int MAX_CHARS_PER_WINDOW = 1024;

  /**
   * Reproduces the old writeString logic: for short strings (<=1024 chars), allocate a BytesRef and
   * copy through it. For long strings, use chunked writeLongString.
   */
  private static void writeStringOld(ByteBuffersDataOutput out, String v) throws IOException {
    if (v.length() <= MAX_CHARS_PER_WINDOW) {
      final BytesRef utf8 = new BytesRef(v);
      out.writeVInt(utf8.length);
      out.writeBytes(utf8.bytes, utf8.offset, utf8.length);
    } else {
      writeLongStringOld(out, v);
    }
  }

  /** Old writeLongString: computes length, writes VInt, then encodes in chunks. */
  private static void writeLongStringOld(ByteBuffersDataOutput out, String s) throws IOException {
    final int byteLen = UnicodeUtil.calcUTF16toUTF8Length(s, 0, s.length());
    out.writeVInt(byteLen);
    final byte[] buf =
        new byte[Math.min(byteLen, UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * MAX_CHARS_PER_WINDOW)];
    for (int i = 0, end = s.length(); i < end; ) {
      int step = Math.min(end - i, MAX_CHARS_PER_WINDOW - 1);
      if (i + step < end && Character.isHighSurrogate(s.charAt(i + step - 1))) {
        step++;
      }
      int upTo = UnicodeUtil.UTF16toUTF8(s, i, step, buf);
      out.writeBytes(buf, 0, upTo);
      i += step;
    }
  }

  // --- String generators ---

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
