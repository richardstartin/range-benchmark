package io.github.richardstartin.range;

import org.roaringbitmap.RoaringBitmap;

public interface RangeEvaluator {

  RoaringBitmap between(long min, long max);

  int serializedSize();

}
