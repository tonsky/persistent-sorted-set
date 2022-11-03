package me.tonsky.persistent_sorted_set;

public class Stitch {
  Object[] target;
  int offset;

  public Stitch(Object[] target, int offset) {
    this.target = target;
    this.offset = offset;
  }

  public Stitch copyAll(Object[] source, int from, int to) {
    if (to >= from) {
      if (source != null) {
        System.arraycopy(source, from, target, offset, to - from);
      }
      offset += to - from;
    }
    return this;
  }

  public Stitch copyOne(Object val) {
    target[offset] = val;
    ++offset;
    return this;
  }
}