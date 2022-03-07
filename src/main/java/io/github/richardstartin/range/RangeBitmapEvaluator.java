package io.github.richardstartin.range;

import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

public class RangeBitmapEvaluator implements RangeEvaluator {

  private final RangeBitmap bitmap;
  private final int serializedSize;

  public RangeBitmapEvaluator(long[] data) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long datum : data) {
      min = Math.min(min, datum);
      max = Math.max(max, datum);
    }
    var appender = RangeBitmap.appender(max);
    for (long datum : data) {
      appender.add(datum - min);
    }
    serializedSize = appender.serializedSizeInBytes();
    this.bitmap = appender.build();
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    return bitmap.between(min, max);
  }

  @Override
  public int serializedSize() {
    return serializedSize;
  }
}
