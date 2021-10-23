package io.github.richardstartin.range;

import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;

public class BinarySearch implements RangeEvaluator {

  private final long[] data;

  public BinarySearch(long[] data) {
    this.data = data;
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    int start = Arrays.binarySearch(data, min);
    int begin = start >= 0 ? start : -start - 1;
    while (begin - 1 >= 0 && data[begin - 1] == min) {
      begin--;
    }
    int end = Arrays.binarySearch(data, begin, data.length, max);
    int finish = end >= 0 ? end : -end - 1;
    while (finish + 1 < data.length && data[finish + 1] == max) {
      finish++;
    }
    return RoaringBitmap.bitmapOfRange(begin, finish + 1);
  }

  @Override
  public int serializedSize() {
    return 0;
  }

}
