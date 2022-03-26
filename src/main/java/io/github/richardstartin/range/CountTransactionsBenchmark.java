package io.github.richardstartin.range;

import java.util.Collections;
import java.util.Comparator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


public class CountTransactionsBenchmark {

  @State(Scope.Benchmark)
  public static class PollutedTransactions extends Transactions {

    @Setup(Level.Trial)
    public void setup() {
      super.setup();
      transactions.stream()
          .filter(transaction -> transaction.quantity < 0 && transaction.price > 1
              && transaction.timestamp >= 1 && transaction.timestamp <= Long.MAX_VALUE - 1)
          .count();
      transactions.stream()
          .filter(transaction -> transaction.quantity < 10 && transaction.price > 1000
              && transaction.timestamp >= -1 && transaction.timestamp <= Long.MAX_VALUE - 1)
          .count();
    }
  }

  @Benchmark
  public long stream1(Transactions state) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    return state.transactions.stream()
            .filter(transaction -> transaction.quantity >= qty && transaction.price <= price
                    && transaction.timestamp >= begin && transaction.timestamp <= end)
            .count();
  }

  @Benchmark
  public long stream2(Transactions state) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    return state.transactions.stream()
            .filter(transaction -> transaction.timestamp >= begin && transaction.timestamp <= end
                    && transaction.quantity >= qty && transaction.price <= price)
        .count();
  }

  @Benchmark
  public long stream2Polluted(PollutedTransactions state) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    return state.transactions.stream()
        .filter(transaction -> transaction.timestamp >= begin && transaction.timestamp <= end
            && transaction.quantity >= qty && transaction.price <= price)
        .count();
  }

  @Benchmark
  public long binarySearch(Transactions state) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    int first = Collections.binarySearch(state.transactions, new Transaction(0, 0, begin),
            Comparator.comparingLong(Transaction::getTimestamp));
    int last = Collections.binarySearch(state.transactions, new Transaction(0, 0, end),
            Comparator.comparingLong(Transaction::getTimestamp));
    long count = 0;
    for (int i = first; i <= last; i++) {
      Transaction transaction = state.transactions.get(i);
      if (transaction.quantity >= qty && transaction.price <= price) {
        count++;
      }
    }
    return count;
  }

  @Benchmark
  public long binarySearchBranchFreeScan(Transactions state) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    int first = Collections.binarySearch(state.transactions, new Transaction(0, 0, begin),
        Comparator.comparingLong(Transaction::getTimestamp));
    int last = Collections.binarySearch(state.transactions, new Transaction(0, 0, end),
        Comparator.comparingLong(Transaction::getTimestamp));
    long count = 0;
    for (int i = first; i <= last; i++) {
      Transaction transaction = state.transactions.get(i);
      count += (Math.max(transaction.quantity - qty, 0) + Math.max(price - transaction.price, 0)) >>> 1;
    }
    return count;
  }

  @Benchmark
  public long index(Transactions state) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    long price = state.maxPriceThreshold - state.minPrice;
    long begin = state.minTimeThreshold - state.minTime;
    long end = state.maxTimeThreshold - state.minTime;
    RoaringBitmap inTimeRange = state.timestampIndex.between(begin, end);
    RoaringBitmap matchesQuantity = state.quantityIndex.gte(qty, inTimeRange);
    return state.priceIndex.lteCardinality(price, matchesQuantity);
  }

  @Benchmark
  public long binarySearchThenIndex(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    long price = state.maxPriceThreshold - state.minPrice;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    int first = Collections.binarySearch(state.transactions, new Transaction(0, 0, begin),
            Comparator.comparingLong(Transaction::getTimestamp));
    int last = Collections.binarySearch(state.transactions, new Transaction(0, 0, end),
            Comparator.comparingLong(Transaction::getTimestamp));
    RoaringBitmap inTimeRange = RoaringBitmap.bitmapOfRange(first, last + 1);
    RoaringBitmap matchesQuantity = state.quantityIndex.gte(qty, inTimeRange);
    return state.priceIndex.lteCardinality(price, matchesQuantity);
  }
}
