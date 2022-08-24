package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Node extends Leaf {
  public volatile Leaf[] _children;
  public AtomicBoolean _isLoaded = new AtomicBoolean(false);
  public volatile UUID _address;
  public final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
  public final Lock _read  = _readWriteLock.readLock();
  public final Lock _write = _readWriteLock.writeLock();

  public Node(StorageBackend storage, Object[] keys, Leaf[] children, int len, Edit edit) {
    super(storage, keys, len, edit);
    _children = children;
    _isLoaded.set(true);
    _address = null;
  }

    Node newNode(int len, Edit edit) {
      return new Node(_storage, new Object[len], new Leaf[len], len, edit);
    }

    // used by storage only
    public Node(StorageBackend storage, Object[] keys, int len, Edit edit, UUID address) {
      super(storage, keys, len, edit);
      _address = address;
      _children = new Leaf[len];
    }

    public void ensureChildren() {
      if (!_isLoaded.get()) {
        _storage.load(this);
        _isLoaded.set(true);
      } else {
        _storage.hitCache(this);
      }
    }

  boolean contains(Object key, Comparator cmp) {
    int idx = search(key, cmp);
    if (idx >= 0) return true;
    int ins = -idx-1;
    if (ins == _len) return false;
    _write.lock();
    ensureChildren();
    _read.lock();
    _write.unlock();
    boolean contained = _children[ins].contains(key, cmp);
    _read.unlock();
    return contained;
  }

  Leaf[] add(Object key, Comparator cmp, Edit edit) {
    int idx = search(key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;

    int ins = -idx-1;
    if (ins == _len) ins = _len-1;
    _write.lock();
    ensureChildren();
    _read.lock();
    _write.unlock();
    Leaf[] nodes = _children[ins].add(key, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) { // child signalling already in set
      _read.unlock();
      return PersistentSortedSet.UNCHANGED;
    }

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      _read.unlock();
      return PersistentSortedSet.EARLY_EXIT;
    }

    // same len
    if (1 == nodes.length) {
      Leaf node = nodes[0];
      if (_edit.editable()) {
        _keys[ins] = node.maxKey();
        _children[ins] = node;
        Leaf[] added = ins==_len-1 && node.maxKey() == maxKey() ? new Leaf[]{this} : PersistentSortedSet.EARLY_EXIT;
        _read.unlock();
        return added;
      }

      Object[] newKeys;
      if (0 == cmp.compare(node.maxKey(), _keys[ins]))
        newKeys = _keys;
      else {
        newKeys = Arrays.copyOfRange(_keys, 0, _len);
        newKeys[ins] = node.maxKey();
      }

      Leaf[] newChildren;
      if (node == _children[ins])
        newChildren = _children;
      else {
        newChildren = Arrays.copyOfRange(_children, 0, _len);
        newChildren[ins] = node;
      }

      Leaf[] added = new Leaf[]{new Node(_storage, newKeys, newChildren, _len, edit)};
      _read.unlock();
      return added;
    }

    // len + 1
    if (_len < PersistentSortedSet.MAX_LEN) {
      Node n = newNode(_len+1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins+1, _len);

      new Stitch(n._children, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins+1, _len);
      Leaf [] added = new Leaf[]{n};
      _read.unlock();
      return added;
    }

    // split
    int half1 = (_len+1) >>> 1;
    if (ins+1 == half1) ++half1;
    int half2 = _len+1-half1;

    // add to first half
    if (ins < half1) {
      Object keys1[] = new Object[half1];
      new Stitch(keys1, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins+1, half1-1);
      Object keys2[] = new Object[half2];
      ArrayUtil.copy(_keys, half1-1, _len, keys2, 0);

      Leaf children1[] = new Leaf[half1];
      new Stitch(children1, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins+1, half1-1);
      Leaf children2[] = new Leaf[half2];
      ArrayUtil.copy(_children, half1-1, _len, children2, 0);
      Leaf[] added = new Leaf[]{new Node(_storage, keys1, children1, half1, edit),
          new Node(_storage, keys2, children2, half2, edit)};
      _read.unlock();
      return added;
    }

    // add to second half
    Object keys1[] = new Object[half1],
           keys2[] = new Object[half2];
    ArrayUtil.copy(_keys, 0, half1, keys1, 0);

    new Stitch(keys2, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(nodes[0].maxKey())
      .copyOne(nodes[1].maxKey())
      .copyAll(_keys, ins+1, _len);

    Leaf children1[] = new Leaf[half1],
         children2[] = new Leaf[half2];
    ArrayUtil.copy(_children, 0, half1, children1, 0);

    new Stitch(children2, 0)
      .copyAll(_children, half1, ins)
      .copyOne(nodes[0])
      .copyOne(nodes[1])
      .copyAll(_children, ins+1, _len);
    Leaf[] added = new Leaf[]{new Node(_storage, keys1, children1, half1, edit),
        new Node(_storage, keys2, children2, half2, edit)};
    _read.unlock();
    return added;
  }

  Leaf[] remove(Object key, Leaf left, Leaf right, Comparator cmp, Edit edit) {
    return remove(key, (Node) left, (Node) right, cmp, edit);
  }

  Leaf[] remove(Object key, Node left, Node right, Comparator cmp, Edit edit) {
    int idx = search(key, cmp);
    if (idx < 0) idx = -idx-1;

    if (idx == _len) // not in set
      return PersistentSortedSet.UNCHANGED;

    _write.lock();
    ensureChildren();
    _read.lock();
    _write.unlock();
    Leaf leftChild  = idx > 0      ? _children[idx-1] : null,
         rightChild = idx < _len-1 ? _children[idx+1] : null;
    Leaf[] nodes = _children[idx].remove(key, leftChild, rightChild, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) { // child signalling element not in set
      _read.unlock();
      return PersistentSortedSet.UNCHANGED;
    }

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      _read.unlock();
      return PersistentSortedSet.EARLY_EXIT;
    }

    // nodes[1] always not nil
    int newLen = _len - 1
                 - (leftChild  != null ? 1 : 0)
                 - (rightChild != null ? 1 : 0)
                 + (nodes[0] != null ? 1 : 0)
                 + 1
                 + (nodes[2] != null ? 1 : 0);

    // no rebalance needed
    if (newLen >= PersistentSortedSet.MIN_LEN || (left == null && right == null)) {
      // can update in place
      if (_edit.editable() && idx < _len-2) {
        Stitch<Object> ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        Stitch<Leaf> cs = new Stitch(_children, Math.max(idx-1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(_children, idx+2, _len);

        _len = newLen;
        _read.unlock();
        return PersistentSortedSet.EARLY_EXIT;
      }

      Node newCenter = newNode(newLen, edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx-1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx+2, _len);

      Stitch<Leaf> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx-1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx+2, _len);

      Leaf[] removed = new Leaf[] { left, newCenter, right };
      _read.unlock();
      return removed;
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Node join = newNode(left._len + newLen, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx-1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,     idx+2, _len);

      Stitch<Leaf> cs = new Stitch(join._children, 0);
      left._write.lock();
      left.ensureChildren();
      left._read.lock();
      left._write.unlock();
      cs.copyAll(left._children, 0, left._len);
      left._read.unlock();
      cs.copyAll(_children,      0, idx-1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx+2, _len);

      Leaf[] removed = new Leaf[] { null, join, right };
      _read.unlock();
      return removed;
    }

    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Node join = newNode(newLen + right._len, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx-1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,       idx+2, _len);
      ks.copyAll(right._keys, 0, right._len);

      Stitch<Leaf> cs = new Stitch(join._children, 0);
      cs.copyAll(_children, 0, idx-1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children,     idx+2, _len);
      right._write.lock();
      right.ensureChildren();
      right._read.lock();
      right._write.unlock();
      cs.copyAll(right._children, 0, right._len);
      right._read.unlock();

      Leaf[] removed = new Leaf[] { left, join, null };
      _read.unlock();
      return removed;
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen;

      Node newLeft   = newNode(newLeftLen,   edit),
           newCenter = newNode(newCenterLen, edit);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx-1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx+2, _len);

      left._write.lock();
      left.ensureChildren();
      left._read.lock();
      left._write.unlock();
      ArrayUtil.copy(left._children, 0, newLeftLen, newLeft._children, 0);

      Stitch<Leaf> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(left._children, newLeftLen, left._len);
      left._read.unlock();
      cs.copyAll(_children, 0, idx-1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx+2, _len);

      Leaf[] removed = new Leaf[] { newLeft, newCenter, right };
      _read.unlock();
      return removed;
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Node newCenter = newNode(newCenterLen, edit),
           newRight  = newNode(newRightLen,  edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx-1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx+2, _len);
      ks.copyAll(right._keys, 0, rightHead);

      ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);

      Stitch<Object> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx-1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx+2, _len);
      right._write.lock();
      right.ensureChildren();
      right._read.lock();
      right._write.unlock();
      cs.copyAll(right._children, 0, rightHead);

      ArrayUtil.copy(right._children, rightHead, right._len, newRight._children, 0);
      right._read.unlock();

      Leaf[] removed = new Leaf[] { left, newCenter, newRight };
      _read.unlock();
      return removed;
    }

    throw new RuntimeException("Unreachable");
  }

  public String str(int lvl) {
    StringBuilder sb = new StringBuilder();
    _read.lock();
    for (int i=0; i < _len; ++i) {
      sb.append("\n");
      for (int j=0; j < lvl; ++j)
        sb.append("| ");
      ensureChildren();
      if (_isLoaded.get()) {
        sb.append(_keys[i] + ": " + _children[i].str(lvl+1));
      } else {
        sb.append(_keys[i] + ": not loaded");
      }
    }
    _read.unlock();

    sb.append(" address: ");
    sb.append(_address);
    return sb.toString();
  }
}
