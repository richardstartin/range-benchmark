package io.github.richardstartin.range;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.Comparator;


public class FindTransactionsEqualityBenchmark {

  @Benchmark
  public void stream(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold;
    state.transactions.stream()
            .filter(transaction -> transaction.quantity == qty)
            .forEach(bh::consume);
  }

  @Benchmark
  public void between(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    RoaringBitmap matchesQuantity = state.quantityIndex.between(qty, qty);
    matchesQuantity.forEach((IntConsumer) i -> bh.consume(state.transactions.get(i)));
  }

  @Benchmark
  public void equals(Transactions state, Blackhole bh) {
    int qty = state.minQuantityThreshold - state.minQuantity;
    RoaringBitmap matchesQuantity = state.quantityIndex.eq(qty);
    matchesQuantity.forEach((IntConsumer) i -> bh.consume(state.transactions.get(i)));
  }
}
