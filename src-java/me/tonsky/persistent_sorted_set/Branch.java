package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.*;

@SuppressWarnings("unchecked")
public class Branch extends ANode {
  // Optional, only in branches
  // Only valid [0 ... _len-1]
  public final Object[] _addresses;

  // Optional, only in branches
  // Only valid [0 ... _len-1]
  public final ANode[] _children;

  public Branch(int len, Object[] keys, Object[] addresses, ANode[] children, AtomicBoolean edit) {
    super(len, keys, edit);
    assert addresses.length >= len;
    assert children.length >= len;
    _addresses = addresses;
    _children  = children;
  }

  public Branch(int len, AtomicBoolean edit) {
    super(len, new Object[ANode.newLen(len, edit)], edit);
    _addresses = new Object[ANode.newLen(len, edit)];
    _children  = new ANode[ANode.newLen(len, edit)];
  }

  public ANode child(IStorage storage, int idx) {
    assert 0 <= idx && idx < _len;
    ANode child = _children[idx];
    if (child == null) {
      child = storage.load(_addresses[idx]);
      _children[idx] = child;
    }
    return child;
  }

  @Override
  public int count(IStorage storage) {
    int count = 0;
    for (int i = 0; i < _len; ++i) {
      count += child(storage, i).count(storage);
    }
    return count;
  }

  public Object onPersist(int idx, Object address) {
    assert 0 <= idx && idx < _len;

    _addresses[idx] = address;
    return address;
  }

  @Override
  public boolean contains(IStorage storage, Object key, Comparator cmp) {
    int idx = search(key, cmp);
    if (idx >= 0) return true;
    int ins = -idx - 1; 
    if (ins == _len) return false;
    assert 0 <= ins && ins < _len;
    return child(storage, ins).contains(storage, key, cmp);
  }

  @Override
  public ANode[] add(IStorage storage, Object key, Comparator cmp, AtomicBoolean edit) {
    int idx = search(key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;
    
    int ins = -idx - 1;
    if (ins == _len) ins = _len - 1;
    assert 0 <= ins && ins < _len;
    ANode[] nodes = child(storage, ins).add(storage, key, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling already in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }
    
    // same len, editable
    if (1 == nodes.length && editable()) {
      ANode node = nodes[0];
      _keys[ins] = node.maxKey();
      _addresses[ins] = null;
      _children[ins] = node;
      if (ins == _len - 1 && node.maxKey() == maxKey()) // TODO why maxKey check?
        return new ANode[]{this}; // update maxKey
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // same len, not editable
    if (1 == nodes.length) {
      ANode node = nodes[0];
      Object[] newKeys;
      if (0 == cmp.compare(node.maxKey(), _keys[ins])) {
        newKeys = _keys;
      } else {
        newKeys = Arrays.copyOfRange(_keys, 0, _len);
        newKeys[ins] = node.maxKey();
      }

      Object[] newAddresses;
      ANode[] newChildren;
      if (node == _children[ins]) { // TODO how is this possible?
        newAddresses = _addresses;
        newChildren = _children;
      } else {
        newAddresses = Arrays.copyOfRange(_addresses, 0, _len);
        newAddresses[ins] = null;

        newChildren = Arrays.copyOfRange(_children, 0, _len);
        newChildren[ins] = node;
      }

      return new ANode[]{new Branch(_len, newKeys, newAddresses, newChildren, edit)};
    }

    // len + 1
    if (_len < PersistentSortedSet.MAX_LEN) {
      Branch n = new Branch(_len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins + 1, _len);

      new Stitch(n._addresses, 0)
        .copyAll(_addresses, 0, ins)
        .copyOne(null)
        .copyOne(null)
        .copyAll(_addresses, ins + 1, _len);

      new Stitch(n._children, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, _len);

      return new ANode[]{n};
    }

    // split
    int half1 = (_len + 1) >>> 1;
    if (ins+1 == half1) ++half1;
    int half2 = _len + 1 - half1;

    // add to first half
    if (ins < half1) {
      Object[] keys1 = new Object[half1];
      new Stitch(keys1, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins+1, half1-1);
      Object[] keys2 = new Object[half2];
      ArrayUtil.copy(_keys, half1 - 1, _len, keys2, 0);

      Object[] addresses1 = new Object[half1];
      new Stitch(addresses1, 0)
        .copyAll(_addresses, 0, ins)
        .copyOne(null)
        .copyOne(null)
        .copyAll(_addresses, ins + 1, half1 - 1);
      Object[] addresses2 = new Object[half2];
      ArrayUtil.copy(_addresses, half1 - 1, _len, addresses2, 0);

      ANode[] children1 = new ANode[half1];
      new Stitch(children1, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, half1 - 1);
      ANode[] children2 = new ANode[half2];
      ArrayUtil.copy(_children, half1 - 1, _len, children2, 0);

      return new ANode[] {
        new Branch(half1, keys1, addresses1, children1, edit),
        new Branch(half2, keys2, addresses2, children2, edit)
      };
    }

    // add to second half
    Object[] keys1 = new Object[half1],
             keys2 = new Object[half2];
    ArrayUtil.copy(_keys, 0, half1, keys1, 0);

    new Stitch(keys2, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(nodes[0].maxKey())
      .copyOne(nodes[1].maxKey())
      .copyAll(_keys, ins + 1, _len);

    Object addresses1[] = new Object[half1];
    ArrayUtil.copy(_addresses, 0, half1, addresses1, 0);
    Object addresses2[] = new Object[half2];
    new Stitch(addresses2, 0)
      .copyAll(_addresses, half1, ins)
      .copyOne(null)
      .copyOne(null)
      .copyAll(_addresses, ins + 1, _len);

    ANode[] children1 = new ANode[half1];
    ArrayUtil.copy(_children, 0, half1, children1, 0);
    ANode[] children2 = new ANode[half2];
    new Stitch(children2, 0)
      .copyAll(_children, half1, ins)
      .copyOne(nodes[0])
      .copyOne(nodes[1])
      .copyAll(_children, ins + 1, _len);

    return new ANode[]{
      new Branch(half1, keys1, addresses1, children1, edit),
      new Branch(half2, keys2, addresses2, children2, edit)
    };
  }

  @Override
  public ANode[] remove(IStorage storage, Object key, ANode _left, ANode _right, Comparator cmp, AtomicBoolean edit) {
    Branch left = (Branch) _left;
    Branch right = (Branch) _right;

    int idx = search(key, cmp);
    if (idx < 0) idx = -idx - 1;

    if (idx == _len) // not in set
      return PersistentSortedSet.UNCHANGED;

    assert 0 <= idx && idx < _len;
    
    ANode leftChild  = idx > 0      ? child(storage, idx - 1) : null,
          rightChild = idx < _len-1 ? child(storage, idx + 1) : null;
    int leftChildLen = safeLen(leftChild);
    int rightChildLen = safeLen(rightChild);
    ANode[] nodes = child(storage, idx).remove(storage, key, leftChild, rightChild, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling element not in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }

    boolean leftChanged = leftChild != nodes[0] || leftChildLen != safeLen(nodes[0]);
    boolean rightChanged = rightChild != nodes[2] || rightChildLen != safeLen(nodes[2]);

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
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        Stitch as = new Stitch(_addresses, Math.max(idx-1, 0));
        if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
        if (newLen != _len)
          as.copyAll(_addresses, idx+2, _len);

        Stitch<ANode> cs = new Stitch(_children, Math.max(idx-1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(_children, idx+2, _len);

        _len = newLen;
        return PersistentSortedSet.EARLY_EXIT;
      }

      Branch newCenter = new Branch(newLen, edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      Stitch as = new Stitch(newCenter._addresses, 0);
      as.copyAll(_addresses, 0, idx - 1);
      if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                            as.copyOne(null);
      if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
      as.copyAll(_addresses, idx + 2, _len);

      Stitch<ANode> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new ANode[] { left, newCenter, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(left._len + newLen, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,     idx + 2, _len);

      Stitch as = new Stitch(join._addresses, 0);
      as.copyAll(left._addresses, 0, left._len);
      as.copyAll(_addresses,      0, idx - 1);
      if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                            as.copyOne(null);
      if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
      as.copyAll(_addresses, idx + 2, _len);

      Stitch<ANode> cs = new Stitch(join._children, 0);
      cs.copyAll(left._children, 0, left._len);
      cs.copyAll(_children,      0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(newLen + right._len, edit);

      Stitch<Object> ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,       idx + 2, _len);
      ks.copyAll(right._keys, 0, right._len);

      Stitch as = new Stitch(join._addresses, 0);
      as.copyAll(_addresses, 0, idx - 1);
      if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                            as.copyOne(null);
      if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
      as.copyAll(_addresses,     idx + 2, _len);
      as.copyAll(right._addresses, 0, right._len);

      Stitch<ANode> cs = new Stitch(join._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children,     idx + 2, _len);
      cs.copyAll(right._children, 0, right._len);
      
      return new ANode[] { left, join, null };
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen;

      Branch newLeft   = new Branch(newLeftLen, edit),
             newCenter = new Branch(newCenterLen, edit);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      ArrayUtil.copy(left._addresses, 0, newLeftLen, newLeft._addresses, 0);
      ArrayUtil.copy(left._children, 0, newLeftLen, newLeft._children, 0);

      Stitch as = new Stitch(newCenter._addresses, 0);
      as.copyAll(left._addresses, newLeftLen, left._len);
      as.copyAll(_addresses, 0, idx - 1);
      if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                            as.copyOne(null);
      if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
      as.copyAll(_addresses, idx + 2, _len);

      Stitch<ANode> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(left._children, newLeftLen, left._len);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new ANode[] { newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Branch newCenter = new Branch(newCenterLen, edit),
             newRight  = new Branch(newRightLen, edit);

      Stitch<Object> ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);
      ks.copyAll(right._keys, 0, rightHead);

      ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);

      Stitch as = new Stitch(newCenter._addresses, 0);
      as.copyAll(_addresses, 0, idx - 1);
      if (nodes[0] != null) as.copyOne(leftChanged ? null : _addresses[idx - 1]);
                            as.copyOne(null);
      if (nodes[2] != null) as.copyOne(rightChanged ? null : _addresses[idx + 1]);
      as.copyAll(_addresses, idx + 2, _len);
      as.copyAll(right._addresses, 0, rightHead);

      Stitch<ANode> cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);
      cs.copyAll(right._children, 0, rightHead);

      ArrayUtil.copy(right._addresses, rightHead, right._len, newRight._addresses, 0);
      ArrayUtil.copy(right._children, rightHead, right._len, newRight._children, 0);

      return new ANode[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  public String str(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      for (int j = 0; j < lvl; ++j)
        sb.append("| ");
      sb.append(_keys[i] + ": " + child(storage, i).str(storage, lvl+1));
    }
    return sb.toString();
  }

  @Override
  public void toString(StringBuilder sb, Object address, String indent) {
    sb.append(indent);
    sb.append("Branch addr: " + address + " len: " + _len + " ");
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      ANode child = _children[i];
      if (child != null)
        child.toString(sb, _addresses[i], indent + "  ");
      else
        sb.append(indent + "  " + _addresses[i] + ": <lazy> ");
    }
  }
}