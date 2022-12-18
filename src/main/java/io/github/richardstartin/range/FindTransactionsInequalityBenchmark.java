package io.github.richardstartin.range;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


public class FindTransactionsInequalityBenchmark {

  @Benchmark
  public void stream(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold;
    long price = state.maxPriceThreshold;
    state.transactions.stream()
            .filter(transaction -> transaction.quantity == qty && transaction.price != price)
            .forEach(bh::consume);
  }

  @Benchmark
  public void equals(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    long price = state.maxPriceThreshold - state.minPrice;
    RoaringBitmap matchesQuantity = state.quantityIndex.eq(qty);
    RoaringBitmap mismatchesPrice = state.priceIndex.neq(price, matchesQuantity);
    mismatchesPrice.forEach((IntConsumer) i -> bh.consume(state.transactions.get(i)));
  }
}
