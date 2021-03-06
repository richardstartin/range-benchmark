package io.github.richardstartin.range;

import java.util.Collections;
import java.util.Comparator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


public class FindTransactionsBenchmark {

  @Benchmark
  public void stream1(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    state.transactions.stream()
            .filter(transaction -> transaction.quantity >= qty && transaction.price <= price
                    && transaction.timestamp >= begin && transaction.timestamp <= end)
            .forEach(bh::consume);
  }

  @Benchmark
  public void stream2(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    state.transactions.stream()
            .filter(transaction -> transaction.timestamp >= begin && transaction.timestamp <= end
                    && transaction.quantity >= qty && transaction.price <= price)
            .forEach(bh::consume);
  }

  @Benchmark
  public void binarySearch(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    long begin = state.minTimeThreshold;
    long end = state.maxTimeThreshold;
    int first = Collections.binarySearch(state.transactions, new Transaction(0, 0, begin),
            Comparator.comparingLong(Transaction::getTimestamp));
    int last = Collections.binarySearch(state.transactions, new Transaction(0, 0, end),
            Comparator.comparingLong(Transaction::getTimestamp));
    for (int i = first; i <= last; i++) {
      Transaction transaction = state.transactions.get(i);
      if (transaction.quantity >= qty && transaction.price <= price) {
        bh.consume(transaction);
      }
    }
  }

  @Benchmark
  public void index(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    long price = state.maxPriceThreshold - state.minPrice;
    long begin = state.minTimeThreshold - state.minTime;
    long end = state.maxTimeThreshold - state.minTime;
    RoaringBitmap inTimeRange = state.timestampIndex.between(begin, end);
    RoaringBitmap matchesQuantity = state.quantityIndex.gte(qty, inTimeRange);
    RoaringBitmap matchesPrice = state.priceIndex.lte(price, matchesQuantity);
    matchesPrice.forEach((IntConsumer) i -> bh.consume(state.transactions.get(i)));
  }

  @Benchmark
  public void binarySearchThenIndex(Transactions state, Blackhole bh) {
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
    RoaringBitmap matchesPrice = state.priceIndex.lte(price, matchesQuantity);
    matchesPrice.forEach((IntConsumer) i -> bh.consume(state.transactions.get(i)));
  }
}
