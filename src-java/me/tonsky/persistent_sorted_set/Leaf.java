package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Leaf {
  public Object[] _keys;
  public int _len;
  public final Edit _edit;

  public Leaf(Object[] keys, int len, Edit edit) {
    _keys = keys;
    _len  = len;
    _edit = edit;
  }

  public void ensureLoaded(IStorage storage) {
  }

  public Object maxKey(IStorage storage) {
    ensureLoaded(storage);
    return _keys[_len-1];
  }

  public Leaf newLeaf(int len, Edit edit) {
    if (edit.editable())
      return new Leaf(new Object[Math.min(PersistentSortedSet.MAX_LEN, len + PersistentSortedSet.EXPAND_LEN)], len, edit);
    else
      return new Leaf(new Object[len], len, edit);
  }

  public int search(IStorage storage, Object key, Comparator cmp) {
    ensureLoaded(storage);
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
    //   else if (d > 0) return -i-1; // i
    // }
    // return -high-1; // high
  }

  public int searchFirst(IStorage storage, Object key, Comparator cmp) {
    ensureLoaded(storage);
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

  public int searchLast(IStorage storage, Object key, Comparator cmp) {
    ensureLoaded(storage);
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

  public boolean contains(IStorage storage, Object key, Comparator cmp) {
    return search(storage, key, cmp) >= 0;
  }

  public Leaf[] add(IStorage storage, Object key, Comparator cmp, Edit edit) {
    int idx = search(storage, key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;
    
    int ins = -idx-1;

    // modifying array in place
    if (_edit.editable() && _len < _keys.length) {
      if (ins == _len) {
        _keys[_len++] = key;
        return new Leaf[]{this}; // maxKey needs updating
      } else {
        ArrayUtil.copy(_keys, ins, _len, _keys, ins+1);
        _keys[ins] = key;
        ++_len;
        return PersistentSortedSet.EARLY_EXIT;
      }
    }

    // simply adding to array
    if (_len < PersistentSortedSet.MAX_LEN) {
      Leaf n = newLeaf(_len+1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, _len);
      return new Leaf[]{n};
    }

    // splitting
    int half1 = (_len+1) >>> 1,
        half2 = _len+1-half1;

    // goes to first half
    if (ins < half1) {
      Leaf n1 = newLeaf(half1, edit),
           n2 = newLeaf(half2, edit);
      new Stitch(n1._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, half1-1);
      ArrayUtil.copy(_keys, half1-1, _len, n2._keys, 0);
      return new Leaf[]{n1, n2};
    }

    // copy first, insert to second
    Leaf n1 = newLeaf(half1, edit),
         n2 = newLeaf(half2, edit);
    ArrayUtil.copy(_keys, 0, half1, n1._keys, 0);
    new Stitch(n2._keys, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(key)
      .copyAll(_keys, ins, _len);
    return new Leaf[]{n1, n2};
  }

  public Leaf[] remove(IStorage storage, Object key, Leaf left, Leaf right, Comparator cmp, Edit edit) {
    int idx = search(storage, key, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    int newLen = _len-1;

    // nothing to merge
    if (newLen >= PersistentSortedSet.MIN_LEN || (left == null && right == null)) {

      // transient, can edit in place
      if (_edit.editable()) {
        ArrayUtil.copy(_keys, idx+1, _len, _keys, idx);
        _len = newLen;
        if (idx == newLen) // removed last, need to signal new maxKey
          return new Leaf[]{left, this, right};
        return PersistentSortedSet.EARLY_EXIT;        
      }

      // persistent
      Leaf center = newLeaf(newLen, edit);
      new Stitch(center._keys, 0) 
        .copyAll(_keys, 0, idx)
        .copyAll(_keys, idx+1, _len);
      return new Leaf[] { left, center, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Leaf join = newLeaf(left._len + newLen, edit);
      new Stitch(join._keys, 0)
        .copyAll(left._keys, 0,     left._len)
        .copyAll(_keys,      0,     idx)
        .copyAll(_keys,      idx+1, _len);
      return new Leaf[] { null, join, right };
    }
    
    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Leaf join = newLeaf(newLen + right._len, edit);
      new Stitch(join._keys, 0)
        .copyAll(_keys,       0,     idx)
        .copyAll(_keys,       idx+1, _len)
        .copyAll(right._keys, 0,     right._len);
      return new Leaf[]{ left, join, null };
    }

    // borrow from left
    if (left != null && (left._edit.editable() || right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen,
          leftTail     = left._len - newLeftLen;

      Leaf newLeft, newCenter;

      // prepend to center
      if (_edit.editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        ArrayUtil.copy(_keys,      idx+1,      _len,      _keys, leftTail + idx);
        ArrayUtil.copy(_keys,      0,          idx,      _keys, leftTail);
        ArrayUtil.copy(left._keys, newLeftLen, left._len, _keys, 0);
        _len = newCenterLen;
      } else {
        newCenter = newLeaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(left._keys, newLeftLen, left._len)
          .copyAll(_keys,      0,          idx)
          .copyAll(_keys,      idx+1,      _len);
      }

      // shrink left
      if (left._edit.editable()) {
        newLeft  = left;
        left._len = newLeftLen;
      } else {
        newLeft = newLeaf(newLeftLen, edit);
        ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
      }

      return new Leaf[]{ newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;
      
      Leaf newCenter, newRight;
      
      // append to center
      if (_edit.editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        new Stitch(_keys, idx)
          .copyAll(_keys,       idx+1, _len)
          .copyAll(right._keys, 0,     rightHead);
        _len = newCenterLen;
      } else {
        newCenter = newLeaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(_keys,       0,     idx)
          .copyAll(_keys,       idx+1, _len)
          .copyAll(right._keys, 0,     rightHead);
      }

      // cut head from right
      if (right._edit.editable()) {
        newRight = right;
        ArrayUtil.copy(right._keys, rightHead, right._len, right._keys, 0);
        right._len = newRightLen;
      } else {
        newRight = newLeaf(newRightLen, edit);
        ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);
      }

      return new Leaf[]{ left, newCenter, newRight };
    }
    throw new RuntimeException("Unreachable");
  }

  public int count(IStorage storage) {
    // ensureLoaded(storage);
    return _len;
  }

  public String str(IStorage storage, int lvl) {
    // ensureLoaded(storage);
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < _len; ++i) {
      if (i > 0) sb.append(" ");
      sb.append(_keys[i].toString());
    }
    return sb.append("}").toString();
  }
}