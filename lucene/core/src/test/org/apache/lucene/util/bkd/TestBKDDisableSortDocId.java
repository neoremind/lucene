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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

/** This is temporary testcase, should be removed before merging into main branch. */
@Ignore
public class TestBKDDisableSortDocId extends LuceneTestCase {

  List<Integer> docIdList = new ArrayList<>();

  public void testPerformance() throws Exception {
    boolean disableSortDocId = false;
    int warmUpTimes = 5;
    int runTimes = 10;
    int bytesPerDim = 4;
    int docNum = 2000000;
    System.out.println("warm up");
    for (int i = 0; i < warmUpTimes; i++) {
      doTestSort(bytesPerDim, docNum, disableSortDocId);
    }

    System.gc();
    Thread.sleep(5000);

    System.out.println("start benchmark");
    long sortDocIdTotalTime = 0L;
    for (int i = 0; i < runTimes; i++) {
      sortDocIdTotalTime += doTestSort(bytesPerDim, docNum, disableSortDocId);
      Thread.sleep(2000);
    }

    System.out.println(
        "disableSortDocId="
            + disableSortDocId
            + ", avg time(us): "
            + sortDocIdTotalTime * 1.0f / runTimes / 1000.0f);
  }

  private long doTestSort(int bytesPerDim, int docNum, boolean disableSortDocId) {
    final int maxDoc = docNum;

    BKDConfig config =
        new BKDConfig(
            1, 1, bytesPerDim, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE, disableSortDocId);
    boolean isDocIdIncremental = disableSortDocId;
    Point[] points = createRandomPoints(config, maxDoc, new int[1], isDocIdIncremental);
    DummyPointsReader reader = new DummyPointsReader(points);
    long start = System.nanoTime();
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, points.length);
    long end = System.nanoTime();
    System.out.println("time used: " + (end - start));
    return end - start;
  }

  private Point[] createRandomPoints(
      BKDConfig config, int maxDoc, int[] commonPrefixLengths, boolean isDocIdIncremental) {
    assertTrue(commonPrefixLengths.length == config.numDims);
    final int numPoints = maxDoc;
    Point[] points = new Point[numPoints];
    if (docIdList.isEmpty()) {
      for (int i = 0; i < maxDoc; i++) {
        docIdList.add(i);
      }
    }
    Collections.shuffle(docIdList, random());
    for (int i = 0; i < numPoints; ++i) {
      byte[] value = new byte[config.packedBytesLength];
      random().nextBytes(value);
      points[i] = new Point(value, isDocIdIncremental ? i : docIdList.get(i));
    }
    return points;
  }

  static class Point {
    final BytesRef packedValue;
    final int doc;

    Point(byte[] packedValue, int doc) {
      // use a non-null offset to make sure MutablePointsReaderUtils does not ignore it
      this.packedValue = new BytesRef(packedValue.length + 1);
      this.packedValue.offset = 1;
      this.packedValue.length = packedValue.length;
      System.arraycopy(packedValue, 0, this.packedValue.bytes, 0, packedValue.length);
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof Point == false) {
        return false;
      }
      Point that = (Point) obj;
      return packedValue.equals(that.packedValue) && doc == that.doc;
    }

    @Override
    public int hashCode() {
      return 31 * packedValue.hashCode() + doc;
    }

    @Override
    public String toString() {
      return "value=" + packedValue + " doc=" + doc;
    }
  }

  static class DummyPointsReader extends MutablePointValues {

    private final Point[] points;

    private Point[] temp;

    DummyPointsReader(Point[] points) {
      this.points = points.clone();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      packedValue.bytes = points[i].packedValue.bytes;
      packedValue.offset = points[i].packedValue.offset;
      packedValue.length = points[i].packedValue.length;
    }

    @Override
    public byte getByteAt(int i, int k) {
      BytesRef packedValue = points[i].packedValue;
      return packedValue.bytes[packedValue.offset + k];
    }

    @Override
    public int getDocID(int i) {
      return points[i].doc;
    }

    @Override
    public void swap(int i, int j) {
      ArrayUtil.swap(points, i, j);
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumDimensions() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void assign(int from, int to) {
      if (temp == null) {
        temp = new Point[points.length];
      }
      temp[to] = points[from];
    }

    @Override
    public void finalizeAssign(int from, int to) {
      if (temp != null) {
        System.arraycopy(temp, from, points, from, to - from);
      }
    }
  }
}
