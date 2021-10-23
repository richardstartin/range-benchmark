package io.github.richardstartin.range;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import java.util.Arrays;

public class InvertedIndexEvaluator implements RangeEvaluator {

  private final long[] uniqueValues;
  private final RoaringBitmap[] bitmaps;
  private final int serializedSize;

  public InvertedIndexEvaluator(long[] values, long[] sortedValues) {
    long[] unique = new long[16];
    int numRanges = 0;
    long current = sortedValues[0];
    for (int i = 1; i < sortedValues.length; i++) {
      long value = sortedValues[i];
      if (current != value) {
        if (numRanges == unique.length) {
          unique = Arrays.copyOf(unique, numRanges * 2);
        }
        unique[numRanges] = current;
        numRanges++;
        current = value;
      }
    }
    unique[numRanges] = current;
    numRanges++;
    this.uniqueValues = Arrays.copyOf(unique, numRanges);
    RoaringBitmapWriter<RoaringBitmap>[] writers = new RoaringBitmapWriter[numRanges];
    Arrays.setAll(writers, i -> RoaringBitmapWriter.writer().get());
    for (int i = 0; i < values.length; i++) {
      writers[Arrays.binarySearch(uniqueValues, values[i])].add(i);
    }
    RoaringBitmap[] bitmaps = new RoaringBitmap[writers.length];
    Arrays.setAll(bitmaps, i -> writers[i].get());
    this.bitmaps = bitmaps;
    int ss = uniqueValues.length * 8 + bitmaps.length * 4;
    for (RoaringBitmap bitmap : bitmaps) {
      ss += bitmap.serializedSizeInBytes();
    }
    this.serializedSize = ss;
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    int start = Arrays.binarySearch(uniqueValues, min);
    int begin = start >= 0 ? start : -start - 1;
    int end = Arrays.binarySearch(uniqueValues, begin, uniqueValues.length, max + 1);
    int finish = end >= 0 ? end : -end - 1;
    RoaringBitmap bitmap = bitmaps[begin].clone();
    for (int i = begin + 1; i <= finish & i < bitmaps.length; i++) {
      bitmap.or(bitmaps[i]);
    }
    return bitmap;
  }

  @Override
  public int serializedSize() {
    return serializedSize;
  }
}
