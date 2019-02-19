package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.lang.reflect.Array;
import clojure.lang.*;

public class ArrayUtil {
  public static <T> T[] copy(T[] src, int from, int to, T[] target, int offset) {
    System.arraycopy(src, from, target, offset, to-from);
    return target;
  }

  public static Object indexedToArray(Class type, Indexed coll, int from, int to) {
    int len = to - from;
    Object ret = Array.newInstance(type, len);
    for (int i = 0; i < len; ++i)
      Array.set(ret, i, coll.nth(i+from));
    return ret;
  }

  public static int distinct(Comparator<Object> cmp, Object[] arr) {
    int to = 0;
    for (int idx = 1; idx < arr.length; ++idx) {
      if (cmp.compare(arr[idx], arr[to]) != 0) {
        ++to;
        if (to != idx) arr[to] = arr[idx];
      }
    }
    return to + 1;
  }
}