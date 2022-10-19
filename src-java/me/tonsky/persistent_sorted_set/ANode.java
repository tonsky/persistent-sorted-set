package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

@SuppressWarnings("unchecked")
public abstract class ANode<Key, Address> {
  // >= 0
  public int _len;

  // NotNull
  // Only valid [0 ... _len-1]
  public final Key[] _keys;

  // Nullable
  public final AtomicBoolean _edit;

  public ANode(int len, Key[] keys, AtomicBoolean edit) {
    assert keys.length >= len;

    _len   = len;
    _keys  = keys;
    _edit  = edit;
  }

  public int len() {
    return _len;
  }

  public Key maxKey() {
    return _keys[_len - 1];
  }

  public boolean editable() {
    return _edit != null && _edit.get();
  }

  public int search(Key key, Comparator<Key> cmp) {
    return Arrays.binarySearch(_keys, 0, _len, key, cmp);

    // int low = 0, high = _len;
    // while (high - low > 16) {
    //   int mid = (high + low) >>> 1;
    //   int d = cmp.compare(_keys[mid], key);
    //   if (d == 0) return mid;
    //   else if (d > 0) high = mid;
    //   else low = mid;
    // }

    // // linear search
    // for (int i = low; i < high; ++i) {
    //   int d = cmp.compare(_keys[i], key);
    //   if (d == 0) return i;
    //   else if (d > 0) return -i - 1; // i
    // }

    // return -high - 1; // high
  }

  public int searchFirst(Key key, Comparator<Key> cmp) {
    int low = 0, high = _len;
    while (low < high) {
      int mid = (high + low) >>> 1;
      int d = cmp.compare(_keys[mid], key);
      if (d < 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  public int searchLast(Key key, Comparator<Key> cmp) {
    int low = 0, high = _len;
    while (low < high) {
      int mid = (high + low) >>> 1;
      int d = cmp.compare(_keys[mid], key);
      if (d <= 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low - 1;
  }

  public static <Key, Address> ANode restore(Key[] keys, Address[] addresses) {
    if (addresses == null) {
      return new Leaf(keys.length, keys, null);
    } else {
      return new Branch(keys.length, keys, addresses, null, null);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb, null, "");
    return sb.toString();
  }

  public abstract int count(IRestore storage);
  public abstract boolean contains(IRestore storage, Key key, Comparator<Key> cmp);
  public abstract ANode[] add(IRestore storage, Key key, Comparator<Key> cmp, AtomicBoolean edit);
  public abstract ANode[] remove(IRestore storage, Key key, ANode left, ANode right, Comparator<Key> cmp, AtomicBoolean edit);
  public abstract String str(IRestore storage, int lvl);
  public abstract void walk(IRestore storage, Address address, BiConsumer<Address, ANode> consumer);
  public abstract Address store(IStore<Key, Address> storage);
  public abstract void toString(StringBuilder sb, Address address, String indent);

  protected static int newLen(int len, AtomicBoolean edit) {
    if (edit != null && edit.get())
        return Math.min(PersistentSortedSet.MAX_LEN, len + PersistentSortedSet.EXPAND_LEN);
    else
        return len;
  }

  protected static int safeLen(ANode node) {
    return node == null ? -1 : node._len;
  }
}