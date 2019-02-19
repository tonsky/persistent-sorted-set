package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

class JavaIter implements Iterator {
  final Seq _seq;
  boolean _over;

  JavaIter(Seq seq) {
    _seq = seq;
    _over = seq == null;
  }
  public boolean hasNext() { return !_over; }
  public Object next() {
    Object res = _seq.first();
    _over = false == _seq.advance();
    return res;
  }
}
