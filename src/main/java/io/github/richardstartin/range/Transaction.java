package io.github.richardstartin.range;

public final class Transaction {
  final int quantity;
  final long price;
  final long timestamp;

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
