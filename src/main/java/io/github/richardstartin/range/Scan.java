package io.github.richardstartin.range;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

public class Scan implements RangeEvaluator {

  private final long[] data;

  public Scan(long[] data) {
    this.data = data;
  }

  @Override
  public RoaringBitmap between(long min, long max) {
    RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer().get();
    for (int i = 0; i < data.length; i++) {
      if (data[i] >= min && data[i] <= max) {
        writer.add(i);
      }
    }
    return writer.get();
  }

  @Override
  public int serializedSize() {
    return 0;
  }
}
