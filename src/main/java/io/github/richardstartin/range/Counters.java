package io.github.richardstartin.range;

import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class Counters {

  int rows;
  int cardinality;
  int serializedSize;

  public int rows() {
    return rows;
  }

  public int cardinality() {
    return cardinality;
  }

  public int serializedSize() {
    return serializedSize;
  }

  @Setup(Level.Invocation)
  public void reset() {
    rows = 0;
    cardinality = 0;
    serializedSize = 0;
  }
}
