package io.github.richardstartin.range;

import org.roaringbitmap.IntIterator;

final class RangeIterator implements IntIterator {

  private final int min;
  private final int max;
  private int i;

  private RangeIterator(int min, int max, int i) {
    this.min = min;
    this.max = max;
    this.i = i;
  }

  RangeIterator(int min, int max) {
    this(min, max, min);
  }

  @Override
  public IntIterator clone() {
    return new RangeIterator(min, max, i);
  }

  @Override
  public boolean hasNext() {
    return i <= max;
  }

  @Override
  public int next() {
    return i++;
  }
}
