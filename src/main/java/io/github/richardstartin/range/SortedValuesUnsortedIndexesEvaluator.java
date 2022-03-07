package io.github.richardstartin.range;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.roaringbitmap.RoaringBitmap;


public class SortedValuesUnsortedIndexesEvaluator implements RangeEvaluator {

  private final long[] sortedValues;
  private final int[] indexes;

  public SortedValuesUnsortedIndexesEvaluator(long[] data) {
    List<LongIntPair> pairs = IntStream.range(0, data.length)
        .mapToObj(i -> new LongIntPair(data[i], i))
        .sorted()
        .collect(Collectors.toList());
    sortedValues = pairs.stream().mapToLong(pair -> pair.value).toArray();
    indexes = pairs.stream().mapToInt(pair -> pair.index).toArray();
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    int start = Arrays.binarySearch(sortedValues, min);
    int begin = start >= 0 ? start : -start - 1;
    while (begin - 1 >= 0 && sortedValues[begin - 1] == min) {
      begin--;
    }
    int end = Arrays.binarySearch(sortedValues, begin, sortedValues.length, max);
    int finish = end >= 0 ? end : -end - 1;
    while (finish + 1 < sortedValues.length && sortedValues[finish + 1] == max) {
      finish++;
    }
    RoaringBitmap result = new RoaringBitmap();
    for (int i = begin; i < finish; i++) {
      result.add(indexes[i]);
    }
    return result;
  }

  @Override
  public int serializedSize() {
    return sortedValues.length * Long.BYTES + indexes.length * Integer.BYTES;
  }

  private static final class LongIntPair implements Comparable<LongIntPair> {

    private final long value;
    private final int index;

    private LongIntPair(long value, int index) {
      this.value = value;
      this.index = index;
    }

    @Override
    public int compareTo(LongIntPair o) {
      return Long.compare(value, o.value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LongIntPair that = (LongIntPair) o;

      if (value != that.value) {
        return false;
      }
      return index == that.index;
    }

    @Override
    public int hashCode() {
      int result = (int) (value ^ (value >>> 32));
      result = 31 * result + index;
      return result;
    }
  }
}
