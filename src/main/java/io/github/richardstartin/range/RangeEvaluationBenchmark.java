package io.github.richardstartin.range;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.function.LongSupplier;

  public class RangeEvaluationBenchmark {

  @State(Scope.Benchmark)
  public static abstract class BaseState<T extends RangeEvaluator> {

    @Param({"EXP(0.5)", "EXP(0.01)", "EXP(0.0001)", "UNIFORM(1635012703,1635016303)"})
    String distribution;

    @Param("10000000")
    int size;

    long[] values;
    long[] sortedValues;

    T evaluator;

    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;

    long min;
    long max;

    int cardinality = 0;

    @Setup(Level.Trial)
    public void init() {
      values = new long[size];
      sortedValues = new long[size];
      LongSupplier supplier = Distribution.parse(42, distribution);
      for (int i = 0; i < values.length; i++) {
        values[i] = supplier.getAsLong();
        sortedValues[i] = values[i];
        minValue = Math.min(minValue, values[i]);
        maxValue = Math.max(maxValue, values[i]);
      }
      Arrays.sort(sortedValues);
      min = sortedValues[sortedValues.length / 2];
      max = sortedValues[sortedValues.length / 2 + sortedValues.length / 20];
      evaluator = create();
      long current = sortedValues[0];
      for (int i = 1; i < sortedValues.length; i++) {
        if (values[i] != current) {
          current = values[i];
          cardinality++;
        }
      }
      cardinality++;
    }


    protected abstract T create();
  }

  @State(Scope.Benchmark)
  public static class ScanState extends BaseState<Scan> {

    @Override
    protected Scan create() {
      return new Scan(values);
    }
  }

  @State(Scope.Benchmark)
  public static class InvertedIndexState extends BaseState<InvertedIndexEvaluator> {

    @Override
    protected InvertedIndexEvaluator create() {
      return new InvertedIndexEvaluator(values, sortedValues);
    }
  }

  @State(Scope.Benchmark)
  public static class BinarySearchState extends BaseState<BinarySearch> {

    @Override
    protected BinarySearch create() {
      return new BinarySearch(sortedValues);
    }
  }

  @State(Scope.Benchmark)
  public static class RangeBitmapState extends BaseState<RangeBitmapEvaluator> {

    @Override
    protected RangeBitmapEvaluator create() {
      return new RangeBitmapEvaluator(values);
    }
  }

  @State(Scope.Benchmark)
  public static class IntervalsState extends BaseState<IntervalsEvaluator> {

    @Override
    protected IntervalsEvaluator create() {
      return new IntervalsEvaluator(sortedValues);
    }
  }

  @State(Scope.Benchmark)
  public static class SortedValuesUnsortedIndexesState extends BaseState<SortedValuesUnsortedIndexesEvaluator> {

    @Override
    protected SortedValuesUnsortedIndexesEvaluator create() {
      return new SortedValuesUnsortedIndexesEvaluator(values);
    }
  }


  @Benchmark
  public void rangeBitmap(RangeBitmapState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min - state.minValue, state.max - state.minValue, state.cardinality, counters);
  }

  @Benchmark
  public void binarySearch(BinarySearchState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min, state.max, state.cardinality, counters);
  }

  @Benchmark
  public void scan(ScanState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min, state.max, state.cardinality, counters);
  }

  @Benchmark
  public void intervals(IntervalsState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min, state.max, state.cardinality, counters);
  }

  @Benchmark
  public void invertedIndex(InvertedIndexState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min, state.max, state.cardinality, counters);
  }

  @Benchmark
  public void sortedValuesUnsortedIndexes(SortedValuesUnsortedIndexesState state, Blackhole bh, Counters counters) {
    evaluate(bh, state.evaluator, state.min, state.max, state.cardinality, counters);
  }

  protected static void evaluate(Blackhole bh, RangeEvaluator evaluator, long min, long max, int cardinality, Counters counters) {
    RoaringBitmap bitmap = evaluator.between(min, max);
    counters.rows += bitmap.getCardinality();
    counters.cardinality += cardinality;
    counters.serializedSize += evaluator.serializedSize();
    bh.consume(bitmap);
  }
}
