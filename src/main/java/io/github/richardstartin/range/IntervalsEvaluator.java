package io.github.richardstartin.range;

import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;

public class IntervalsEvaluator implements RangeEvaluator {

  private final long[] uniqueValues;
  private final int[] ranges;

  public IntervalsEvaluator(long[] values) {
    long[] unique = new long[16];
    int[] ranges = new int[16];
    int numRanges = 0;
    int start = 0;
    long current = values[0];
    for (int i = 1; i < values.length; i++) {
      long value = values[i];
      if (current != value) {
        if (numRanges == unique.length) {
          unique = Arrays.copyOf(unique, numRanges * 2);
          ranges = Arrays.copyOf(ranges, numRanges * 2);
        }
        unique[numRanges] = current;
        ranges[numRanges] = start;
        numRanges++;
        current = value;
        start = i;
      }
    }
    unique[numRanges] = current;
    ranges[numRanges] = start;
    numRanges++;
    this.uniqueValues = Arrays.copyOf(unique, numRanges);
    this.ranges = Arrays.copyOf(ranges, numRanges);
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    int start = Arrays.binarySearch(uniqueValues, min);
    int begin = start >= 0 ? start : -start - 1;
    int end = Arrays.binarySearch(uniqueValues, begin, uniqueValues.length, max + 1);
    int finish = end >= 0 ? end : -end - 1;
    return RoaringBitmap.bitmapOfRange(ranges[start], ranges[finish]);
  }

  @Override
  public int serializedSize() {
    return uniqueValues.length * 8 + ranges.length * 4;
  }
}
