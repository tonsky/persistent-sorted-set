package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import clojure.lang.*;

/**
 * Represents both branches and leaves.
 * 
 * WARNING! When edited through transients, might allocate extra space
 * in _keys/_children for further edit. Only read _keys/_children up to _len,
 * even if _keys.lenght might be greater.
 * 
 * Leaf:
 * 
 *   _len         :: int
 *   _keys        :: Object[]
 *   _keys.lenght >= _len
 *   _children    :: null
 * 
 * Branch:
 * 
 *   _len             :: int
 *   _keys            :: Object[]
 *   _keys.lenght     >= _len
 *   _keys[i]         == _children[i].maxKey()
 *   _children        :: Object[]
 *   _children.lenght == _keys.lenght >= _len
 * 
 * When transient:
 * 
 *   _edit :: AtomicBoolean
 *   _edit.get() == true
 * 
 * When persistent:
 * 
 *   _edit == null || _edit.get() == false
 * 
 * Durability/lazy loading:
 * 
 * 1. Not durable
 * 
 *    _address  == null
 *    _keys     :: Object[]
 * 
 * 2. Durable, not loaded
 * 
 *    _address  :: Object
 *    _keys     == null
 * 
 * 3. Just loaded, not changed yet
 * 4. Changed but already persisted
 * 
 *    _address  :: Object
 *    _keys     :: Object[]
 * 
 * 5. Loaded + changed afterwards
 *
 *    _address  == null
 *    _keys     :: Object[]
 */
@SuppressWarnings("unchecked")
public class Node {
  // Only valid [0 ... _len-1]
  public volatile Object[] _keys;

  // Optional, only in branches
  // Only valid [0 ... _len-1]
  public volatile Node[] _children;

  public volatile int _len;

  public volatile Object _address;

  public final AtomicBoolean _edit;

  public Node(Object address) {
    _keys     = null;
    _children = null;
    _len      = -1;
    _address  = address;
    _edit     = null;
  }

  public Node(Object[] keys, int len, AtomicBoolean edit) {
    assert keys.length >= len;

    _keys     = keys;
    _children = null;
    _len      = len;
    _address  = null;
    _edit     = edit;
  }

  public Node(Object[] keys, Node[] children, int len, AtomicBoolean edit) {
    assert keys.length >= len;
    assert children.length >= len;

    _keys     = keys;
    _children = children;
    _len      = len;
    _address  = null;
    _edit     = edit;
  }

  public boolean loaded() {
    return _keys != null;
  }

  public boolean durable() {
    return _address != null;
  }

  public void ensureLoaded(IStorage storage) {
    if (storage == null || loaded())
      return;

    synchronized (this) {
      if (!loaded()) {
        storage.load(this);
      }
    }

    assert loaded();
    assert _len >= 0;
  }

  public void onLoadLeaf(Object[] keys) {
    onLoadLeaf(keys, keys.length);
  }

  public void onLoadLeaf(Object[] keys, int len) {
    // TODO sync?
    assert keys.length >= len;

    _keys  = keys;
    _len   = len;
  }

  public void onLoadBranch(Object[] keys, Node[] children) {
    onLoadBranch(keys, children, keys.length);
  }

  public void onLoadBranch(Object[] keys, Node[] children, int len) {
    // TODO sync?
    assert keys.length >= len;
    assert children.length >= len;

    _keys     = keys;
    _children = children;
    _len      = len;
  }

  // onPersist?

  public Object address() {
    return _address;
  }

  public Object[] keys(IStorage storage) {
    ensureLoaded(storage);
    return _keys;
  }

  public Object key(IStorage storage, int idx) {
    ensureLoaded(storage);
    assert 0 <= idx && idx < _len;
    return _keys[idx];
  }

  public Object maxKey(IStorage storage) {
    ensureLoaded(storage);
    return _keys[_len - 1];
  }

  public Node[] children(IStorage storage) {
    ensureLoaded(storage);
    return _children;
  }

  public Node child(IStorage storage, int idx) {
    ensureLoaded(storage);
    assert 0 <= idx && idx < _len;
    return _children[idx];
  }

  public boolean branch(IStorage storage) {
    ensureLoaded(storage);
    return _children != null;
  }

  public boolean leaf(IStorage storage) {
    ensureLoaded(storage);
    return _children == null;
  }

  public int len(IStorage storage) {
    ensureLoaded(storage);
    return _len;
  }

  public int count(IStorage storage) {
    if (leaf(storage))
      return _len;

    int count = 0;
    for (int i = 0; i < _len; ++i) {
      count += _children[i].count(storage);
    }
    return count;
  }

  public boolean editable() {
    return _edit != null && _edit.get();
  }

  private Node newBranch(int len, AtomicBoolean edit) {
    return new Node(new Object[len], new Node[len], len, edit);
  }

  private Node newLeaf(int len, AtomicBoolean edit) {
    if (editable())
      return new Node(new Object[Math.min(PersistentSortedSet.MAX_LEN, len + PersistentSortedSet.EXPAND_LEN)], len, edit);
    else
      return new Node(new Object[len], len, edit);
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
    //   else if (d > 0) return -i - 1; // i
    // }

    // return -high - 1; // high
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
    if (branch(storage))
      return containsNode(storage, key, cmp);
    else
      return containsLeaf(storage, key, cmp);
  }

  public boolean containsNode(IStorage storage, Object key, Comparator cmp) {
    int idx = search(storage, key, cmp);
    if (idx >= 0) return true;
    int ins = -idx - 1; 
    if (ins == _len) return false;
    assert 0 <= ins && ins < _len;
    return _children[ins].contains(storage, key, cmp);
  }

  public boolean containsLeaf(IStorage storage, Object key, Comparator cmp) {
    return search(storage, key, cmp) >= 0;
  }

  public Node[] add(IStorage storage, Object key, Comparator cmp, AtomicBoolean edit) {
    if (branch(storage))
      return addBranch(storage, key, cmp, edit);
    else
      return addLeaf(storage, key, cmp, edit);
  }

  public Node[] addBranch(IStorage storage, Object key, Comparator cmp, AtomicBoolean edit) {
    int idx = search(storage, key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;
    
    int ins = -idx - 1;
    if (ins == _len) ins = _len - 1;
    assert 0 <= ins && ins < _len;
    Node[] nodes = _children[ins].add(storage, key, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling already in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }
    
    // same len
    if (1 == nodes.length) {
      Node node = nodes[0];
      if (editable()) {
        _keys[ins] = node.maxKey(storage);
        _children[ins] = node;
        if (ins == _len - 1 && node.maxKey(storage) == maxKey(storage)) // TODO why maxKey check?
          return new Node[]{this}; // update maxKey
        else
          return PersistentSortedSet.EARLY_EXIT;
      }

      Object[] newKeys;
      if (0 == cmp.compare(node.maxKey(storage), _keys[ins]))
        newKeys = _keys;
      else {
        newKeys = Arrays.copyOfRange(_keys, 0, _len);
        newKeys[ins] = node.maxKey(storage);
      }

      Node[] newChildren;
      if (node == _children[ins])
        newChildren = _children;
      else {
        newChildren = Arrays.copyOfRange(_children, 0, _len);
        newChildren[ins] = node;
      }

      return new Node[]{new Node(newKeys, newChildren, _len, edit)};
    }

    // len + 1
    if (_len < PersistentSortedSet.MAX_LEN) {
      Node n = newBranch(_len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey(storage))
        .copyOne(nodes[1].maxKey(storage))
        .copyAll(_keys, ins + 1, _len);

      new Stitch(n._children, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, _len);
      return new Node[]{n};
    }

    // split
    int half1 = (_len+1) >>> 1;
    if (ins+1 == half1) ++half1;
    int half2 = _len + 1 - half1;

    // add to first half
    if (ins < half1) {
      Object[] keys1 = new Object[half1];
      new Stitch(keys1, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey(storage))
        .copyOne(nodes[1].maxKey(storage))
        .copyAll(_keys, ins+1, half1-1);
      Object[] keys2 = new Object[half2];
      ArrayUtil.copy(_keys, half1 - 1, _len, keys2, 0);

      Node[] children1 = new Node[half1];
      new Stitch(children1, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, half1 - 1);
      Node[] children2 = new Node[half2];
      ArrayUtil.copy(_children, half1 - 1, _len, children2, 0);
      return new Node[]{new Node(keys1, children1, half1, edit),
                        new Node(keys2, children2, half2, edit)};
    }

    // add to second half
    Object[] keys1 = new Object[half1],
             keys2 = new Object[half2];
    ArrayUtil.copy(_keys, 0, half1, keys1, 0);

    new Stitch(keys2, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(nodes[0].maxKey(storage))
      .copyOne(nodes[1].maxKey(storage))
      .copyAll(_keys, ins + 1, _len);

    Node children1[] = new Node[half1],
         children2[] = new Node[half2];
    ArrayUtil.copy(_children, 0, half1, children1, 0);

    new Stitch(children2, 0)
      .copyAll(_children, half1, ins)
      .copyOne(nodes[0])
      .copyOne(nodes[1])
      .copyAll(_children, ins + 1, _len);
    return new Node[]{new Node(keys1, children1, half1, edit),
                      new Node(keys2, children2, half2, edit)};
  }

  public Node[] addLeaf(IStorage storage, Object key, Comparator cmp, AtomicBoolean edit) {
    int idx = search(storage, key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;
    
    int ins = -idx - 1;
    assert 0 <= ins && ins <= _len;

    // can modify array in place
    if (editable() && _len < _keys.length) {
      if (ins == _len) {
        _keys[_len] = key;
        _len += 1;
        return new Node[]{this}; // maxKey needs updating
      } else {
        ArrayUtil.copy(_keys, ins, _len, _keys, ins+1);
        _keys[ins] = key;
        _len += 1;
        return PersistentSortedSet.EARLY_EXIT;
      }
    }

    // simply adding to array
    if (_len < PersistentSortedSet.MAX_LEN) {
      Node n = newLeaf(_len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, _len);
      return new Node[]{n};
    }

    // splitting
    int half1 = (_len + 1) >>> 1,
        half2 = _len + 1 - half1;

    // goes to first half
    if (ins < half1) {
      Node n1 = newLeaf(half1, edit),
           n2 = newLeaf(half2, edit);
      new Stitch(n1._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, half1 - 1);
      ArrayUtil.copy(_keys, half1 - 1, _len, n2._keys, 0);
      return new Node[]{n1, n2};
    }

    // copy first, insert to second
    Node n1 = newLeaf(half1, edit),
         n2 = newLeaf(half2, edit);
    ArrayUtil.copy(_keys, 0, half1, n1._keys, 0);
    new Stitch(n2._keys, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(key)
      .copyAll(_keys, ins, _len);
    return new Node[]{n1, n2};
  }

  public Node[] remove(IStorage storage, Object key, Node left, Node right, Comparator cmp, AtomicBoolean edit) {
    if (branch(storage))
      return removeBranch(storage, key, left, right, cmp, edit);
    else
      return removeLeaf(storage, key, left, right, cmp, edit);
  }

  public Node[] removeBranch(IStorage storage, Object key, Node left, Node right, Comparator cmp, AtomicBoolean edit) {
    int idx = search(storage, key, cmp);
    if (idx < 0) idx = -idx - 1;

    if (idx == _len) // not in set
      return PersistentSortedSet.UNCHANGED;

    assert 0 <= idx && idx < _len;
    
    Node leftChild  = idx > 0      ? _children[idx - 1] : null,
         rightChild = idx < _len-1 ? _children[idx + 1] : null;
    Node[] nodes = _children[idx].remove(storage, key, leftChild, rightChild, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling element not in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
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
      if (editable() && idx < _len-2) {
        Stitch<Object> ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                              ks.copyOne(nodes[1].maxKey(storage));
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        Stitch<Node> cs = new Stitch(_children, Math.max(idx-1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(_children, idx+2, _len);

        _address = null;
        _len = newLen;
        return PersistentSortedSet.EARLY_EXIT;
      }

      Node newCenter = newBranch(newLen, edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                            ks.copyOne(nodes[1].maxKey(storage));
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
      ks.copyAll(_keys, idx + 2, _len);

      Stitch<Node> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Node[] { left, newCenter, right };
    }

    if (left != null)
      left.ensureLoaded(storage);

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Node join = newBranch(left._len + newLen, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                            ks.copyOne(nodes[1].maxKey(storage));
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
      ks.copyAll(_keys,     idx + 2, _len);

      Stitch<Node> cs = new Stitch(join._children, 0);
      cs.copyAll(left._children, 0, left._len);
      cs.copyAll(_children,      0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Node[] { null, join, right };
    }

    if (right != null)
      right.ensureLoaded(storage);

    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Node join = newBranch(newLen + right._len, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                            ks.copyOne(nodes[1].maxKey(storage));
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
      ks.copyAll(_keys,       idx + 2, _len);
      ks.copyAll(right._keys, 0, right._len);

      Stitch<Node> cs = new Stitch(join._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children,     idx + 2, _len);
      cs.copyAll(right._children, 0, right._len);
      
      return new Node[] { left, join, null };
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen;

      Node newLeft   = newBranch(newLeftLen, edit),
           newCenter = newBranch(newCenterLen, edit);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                            ks.copyOne(nodes[1].maxKey(storage));
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
      ks.copyAll(_keys, idx + 2, _len);

      ArrayUtil.copy(left._children, 0, newLeftLen, newLeft._children, 0);

      Stitch<Node> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(left._children, newLeftLen, left._len);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Node[] { newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Node newCenter = newBranch(newCenterLen, edit),
           newRight  = newBranch(newRightLen, edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey(storage));
                            ks.copyOne(nodes[1].maxKey(storage));
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey(storage));
      ks.copyAll(_keys, idx + 2, _len);
      ks.copyAll(right._keys, 0, rightHead);

      ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);

      Stitch<Object> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);
      cs.copyAll(right._children, 0, rightHead);

      ArrayUtil.copy(right._children, rightHead, right._len, newRight._children, 0);        

      return new Node[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  public Node[] removeLeaf(IStorage storage, Object key, Node left, Node right, Comparator cmp, AtomicBoolean edit) {
    int idx = search(storage, key, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    int newLen = _len - 1;

    // nothing to merge
    if (newLen >= PersistentSortedSet.MIN_LEN || (left == null && right == null)) {

      // transient, can edit in place
      if (editable()) {
        ArrayUtil.copy(_keys, idx + 1, _len, _keys, idx);
        _address = null;
        _len = newLen;
        if (idx == newLen) // removed last, need to signal new maxKey
          return new Node[]{left, this, right};
        return PersistentSortedSet.EARLY_EXIT;        
      }

      // persistent
      Node center = newLeaf(newLen, edit);
      new Stitch(center._keys, 0) 
        .copyAll(_keys, 0, idx)
        .copyAll(_keys, idx + 1, _len);
      return new Node[] { left, center, right };
    }

    if (left != null)
      left.ensureLoaded(storage);

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Node join = newLeaf(left._len + newLen, edit);
      new Stitch(join._keys, 0)
        .copyAll(left._keys, 0,       left._len)
        .copyAll(_keys,      0,       idx)
        .copyAll(_keys,      idx + 1, _len);
      return new Node[] { null, join, right };
    }

    if (right != null)
      right.ensureLoaded(storage);
    
    // can join with right
    if (right != null && newLen + right.len(storage) <= PersistentSortedSet.MAX_LEN) {
      Node join = newLeaf(newLen + right._len, edit);
      new Stitch(join._keys, 0)
        .copyAll(_keys,       0,       idx)
        .copyAll(_keys,       idx + 1, _len)
        .copyAll(right._keys, 0,       right._len);
      return new Node[]{ left, join, null };
    }

    // borrow from left
    if (left != null && (left.editable() || right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen,
          leftTail     = left._len - newLeftLen;

      Node newLeft, newCenter;

      // prepend to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        ArrayUtil.copy(_keys,      idx + 1,    _len,     _keys, leftTail + idx);
        ArrayUtil.copy(_keys,      0,          idx,      _keys, leftTail);
        ArrayUtil.copy(left._keys, newLeftLen, left._len, _keys, 0);
        _address = null;
        _len = newCenterLen;
      } else {
        newCenter = newLeaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(left._keys, newLeftLen, left._len)
          .copyAll(_keys,      0,          idx)
          .copyAll(_keys,      idx+1,      _len);
      }

      // shrink left
      if (left.editable()) {
        newLeft  = left;
        left._address = null;
        left._len = newLeftLen;
      } else {
        newLeft = newLeaf(newLeftLen, edit);
        ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
      }

      return new Node[]{ newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;
      
      Node newCenter, newRight;
      
      // append to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        new Stitch(_keys, idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
        _address = null;
        _len = newCenterLen;
      } else {
        newCenter = newLeaf(newCenterLen, edit);
        new Stitch(newCenter._keys, 0)
          .copyAll(_keys,       0,       idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
      }

      // cut head from right
      if (right.editable()) {
        newRight = right;
        ArrayUtil.copy(right._keys, rightHead, right._len, right._keys, 0);
        right._address = null;
        right._len = newRightLen;
      } else {
        newRight = newLeaf(newRightLen, edit);
        ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);
      }

      return new Node[]{ left, newCenter, newRight };
    }
    throw new RuntimeException("Unreachable");
  }

  public String str(IStorage storage, int lvl) {
    ensureLoaded(storage);
    if (branch(storage))
      return strBranch(storage, lvl);
    else
      return strLeaf(storage, lvl);
  }

  public String strBranch(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      for (int j = 0; j < lvl; ++j)
        sb.append("| ");
      sb.append(_keys[i] + ": " + _children[i].str(storage, lvl+1));
    }
    return sb.toString();
  }

  public String strLeaf(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < _len; ++i) {
      if (i > 0) sb.append(" ");
      sb.append(_keys[i].toString());
    }
    return sb.append("}").toString();
  }
}