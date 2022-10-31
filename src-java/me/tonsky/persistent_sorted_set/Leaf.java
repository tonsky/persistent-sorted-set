package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Leaf<Key, Address> extends ANode<Key, Address> {
  public Leaf(int len, Key[] keys, AtomicBoolean edit) {
    super(len, keys, edit);
  }

  public Leaf(int len, AtomicBoolean edit) {
    super(len, (Key[]) new Object[ANode.newLen(len, edit)], edit);
  }

  public Leaf(List<Key> keys) {
    this(keys.size(), (Key[]) keys.toArray(), null);
  }

  @Override
  public int level() {
    return 0;
  }

  @Override
  public int count(IStorage storage) {
    return _len;
  }

  @Override
  public boolean contains(IStorage storage, Key key, Comparator<Key> cmp) {
    return search(key, cmp) >= 0;
  }

  @Override
  public ANode[] add(IStorage storage, Key key, Comparator<Key> cmp, AtomicBoolean edit) {
    int idx = search(key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;

    int ins = -idx - 1;
    assert 0 <= ins && ins <= _len;

    // can modify array in place
    if (editable() && _len < _keys.length) {
      if (ins == _len) {
        _keys[_len] = key;
        _len += 1;
        return new ANode[]{this}; // maxKey needs updating
      } else {
        ArrayUtil.copy(_keys, ins, _len, _keys, ins+1);
        _keys[ins] = key;
        _len += 1;
        return PersistentSortedSet.EARLY_EXIT;
      }
    }

    // simply adding to array
    if (_len < PersistentSortedSet.MAX_LEN) {
      ANode n = new Leaf(_len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, _len);
      return new ANode[]{n};
    }

    // splitting
    int half1 = (_len + 1) >>> 1,
        half2 = _len + 1 - half1;

    // goes to first half
    if (ins < half1) {
      Leaf n1 = new Leaf(half1, edit),
           n2 = new Leaf(half2, edit);
      new Stitch(n1._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, half1 - 1);
      ArrayUtil.copy(_keys, half1 - 1, _len, n2._keys, 0);
      return new ANode[]{n1, n2};
    }

    // copy first, insert to second
    Leaf n1 = new Leaf(half1, edit),
         n2 = new Leaf(half2, edit);
    ArrayUtil.copy(_keys, 0, half1, n1._keys, 0);
    new Stitch(n2._keys, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(key)
      .copyAll(_keys, ins, _len);
    return new ANode[]{n1, n2};
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, AtomicBoolean edit) {
    Leaf left = (Leaf) _left;
    Leaf right = (Leaf) _right;

    int idx = search(key, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    int newLen = _len - 1;

    // nothing to merge
    if (newLen >= PersistentSortedSet.MIN_LEN || (left == null && right == null)) {

      // transient, can edit in place
      if (editable()) {
        ArrayUtil.copy(_keys, idx + 1, _len, _keys, idx);
        _len = newLen;
        if (idx == newLen) // removed last, need to signal new maxKey
          return new ANode[]{left, this, right};
        return PersistentSortedSet.EARLY_EXIT;
      }

      // persistent
      Leaf center = new Leaf(newLen, edit);
      new Stitch(center._keys, 0)
        .copyAll(_keys, 0, idx)
        .copyAll(_keys, idx + 1, _len);
      return new ANode[] { left, center, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Leaf join = new Leaf(left._len + newLen, edit);
      new Stitch(join._keys, 0)
        .copyAll(left._keys, 0,       left._len)
        .copyAll(_keys,      0,       idx)
        .copyAll(_keys,      idx + 1, _len);
      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right.len() <= PersistentSortedSet.MAX_LEN) {
      Leaf join = new Leaf(newLen + right._len, edit);
      new Stitch(join._keys, 0)
        .copyAll(_keys,       0,       idx)
        .copyAll(_keys,       idx + 1, _len)
        .copyAll(right._keys, 0,       right._len);
      return new ANode[]{ left, join, null };
    }

    // borrow from left
    if (left != null && (left.editable() || right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen,
          leftTail     = left._len - newLeftLen;

      Leaf newLeft, newCenter;

      // prepend to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        ArrayUtil.copy(_keys,      idx + 1,    _len,     _keys, leftTail + idx);
        ArrayUtil.copy(_keys,      0,          idx,      _keys, leftTail);
        ArrayUtil.copy(left._keys, newLeftLen, left._len, _keys, 0);
        _len = newCenterLen;
      } else {
        newCenter = new Leaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(left._keys, newLeftLen, left._len)
          .copyAll(_keys,      0,          idx)
          .copyAll(_keys,      idx+1,      _len);
      }

      // shrink left
      if (left.editable()) {
        newLeft  = left;
        left._len = newLeftLen;
      } else {
        newLeft = new Leaf(newLeftLen, edit);
        ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
      }

      return new ANode[]{ newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Leaf newCenter, newRight;

      // append to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        new Stitch(_keys, idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
        _len = newCenterLen;
      } else {
        newCenter = new Leaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(_keys,       0,       idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
      }

      // cut head from right
      if (right.editable()) {
        newRight = right;
        ArrayUtil.copy(right._keys, rightHead, right._len, right._keys, 0);
        right._len = newRightLen;
      } else {
        newRight = new Leaf(newRightLen, edit);
        ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);
      }

      return new ANode[]{ left, newCenter, newRight };
    }
    throw new RuntimeException("Unreachable");
  }

  @Override
  public void walkAddresses(IStorage storage, IFn onAddress) {
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    return storage.store(this);
  }

  @Override
  public String str(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < _len; ++i) {
      if (i > 0) sb.append(" ");
      sb.append(_keys[i].toString());
    }
    return sb.append("}").toString();
  }

  @Override
  public void toString(StringBuilder sb, Address address, String indent) {
    sb.append(indent);
    sb.append("Leaf   addr: " + address + " len: " + _len + " ");
  }
}
