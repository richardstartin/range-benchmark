package io.github.richardstartin.range;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;


public class FindTransactionsBenchmark {

  @State(Scope.Benchmark)
  public static class Transactions {
    @Param("1000000")
    int size;

    @Param("100")
    long minPrice;

    @Param("1")
    int minQuantity;

    long minTime;

    private final SplittableRandom random = new SplittableRandom(42);

    List<Transaction> transactions;

    RangeBitmap timestampIndex;
    RangeBitmap quantityIndex;
    RangeBitmap priceIndex;

    long minTimeThreshold;
    long maxTimeThreshold;

    int minQuantityThreshold;
    long maxPriceThreshold;

    @Setup(Level.Trial)
    public void setup() {
      transactions = new ArrayList<>(size);
      long time = LocalDate.of(2022, 3, 12).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
      minTime = time;
      for (int i = 0; i < size; i++) {
        transactions.add(randomTransaction(time));
        time += nextTransactionTime();
      }

      minTimeThreshold = transactions.get((size * 5) / 10).timestamp;
      maxTimeThreshold = transactions.get((size * 6) / 10).timestamp;
      minQuantityThreshold = (minQuantity + transactions.get(size / 2).quantity) / 2;
      maxPriceThreshold = (minPrice + transactions.get(size / 2).quantity) / 2;

      index();
    }

    private void index() {
      long minTimestamp = Long.MAX_VALUE;
      long maxTimestamp = Long.MIN_VALUE;
      long minPrice = Long.MAX_VALUE;
      long maxPrice = Long.MIN_VALUE;
      int minQty = Integer.MAX_VALUE;
      int maxQty = Integer.MIN_VALUE;
      for (Transaction transaction : transactions) {
        minTimestamp = Math.min(minTimestamp, transaction.getTimestamp());
        maxTimestamp = Math.max(maxTimestamp, transaction.getTimestamp());
        minPrice = Math.min(minPrice, transaction.getPrice());
        maxPrice = Math.max(maxPrice, transaction.getPrice());
        minQty = Math.min(minQty, transaction.getQuantity());
        maxQty = Math.max(maxQty, transaction.getQuantity());
      }
      var timestampAppender = RangeBitmap.appender(maxTimestamp - minTimestamp);
      var priceAppender = RangeBitmap.appender(maxPrice - minPrice);
      var qtyAppender = RangeBitmap.appender(maxQty - minQty);
      for (Transaction transaction : transactions) {
        timestampAppender.add(transaction.getTimestamp() - minTimestamp);
        priceAppender.add(transaction.getPrice() - minPrice);
        qtyAppender.add(transaction.getQuantity() - minQty);
      }
      timestampIndex = timestampAppender.build();
      priceIndex = priceAppender.build();
      quantityIndex = qtyAppender.build();
      this.minTime = minTimestamp;
      this.minQuantity = minQty;
      this.minPrice = minPrice;
    }

    private long nextTransactionTime() {
      return (long) -(Math.log(random.nextDouble()) / 0.95);
    }

    private Transaction randomTransaction(long timestamp) {
      return new Transaction(random.nextInt(minQuantity, 10000),
              random.nextLong(minQuantity, 1000000), timestamp);
    }
  }


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


  public static final class Transaction {
    private final int quantity;
    private final long price;
    private final long timestamp;

    public Transaction(int quantity, long price, long timestamp) {
      this.quantity = quantity;
      this.price = price;
      this.timestamp = timestamp;
    }

    public int getQuantity() {
      return quantity;
    }

    public long getPrice() {
      return price;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}
