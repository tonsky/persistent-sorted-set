package me.tonsky.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

@SuppressWarnings("unchecked")
public class Branch extends ANode {
  // Optional, only in branches
  // Only valid [0 ... _len-1]
  public Object[] _addresses;

  // Optional, only in branches
  // Only valid [0 ... _len-1]
  public Object[] _children;

  public Branch(int len, Object[] keys, Object[] addresses, Object[] children, AtomicBoolean edit) {
    super(len, keys, edit);
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _addresses = addresses;
    _children  = children;
  }

  public Branch(int len, AtomicBoolean edit) {
    super(len, new Object[ANode.newLen(len, edit)], edit);
    _addresses = null;
    _children  = null;
  }

  public ANode child(IRestore storage, int idx) {
    assert 0 <= idx && idx < _len;
    assert (_children != null && _children[idx] != null) || (_addresses != null && _addresses[idx] != null);

    ANode child = null;
    if (_children != null) {
      Object ref = _children[idx];
      if (ref instanceof SoftReference) {
        child = (ANode) ((SoftReference) ref).get();
      } else {
        child = (ANode) ref;
      }
    }

    if (child == null) {
      child = storage.load(_addresses[idx]);
      ensureChildren()[idx] = new SoftReference<ANode>(child);
    }
    return child;
  }

  public ANode child(int idx, ANode child) {
    if (_children != null || child != null) {
      ensureChildren();
      _children[idx] = child;
    }
    return child;
  }

  @Override
  public int count(IRestore storage) {
    int count = 0;
    for (int i = 0; i < _len; ++i) {
      count += child(storage, i).count(storage);
    }
    return count;
  }

  public Object address(int idx) {
    assert 0 <= idx && idx < _len;

    if (_addresses == null) {
      return null;
    }
    return _addresses[idx];
  }

  public Object address(int idx, Object address) {
    assert 0 <= idx && idx < _len;

    if (_addresses != null || address != null) {
      ensureAddresses();
      _addresses[idx] = address;
      if (_children[idx] instanceof ANode) {
        _children[idx] = new SoftReference(_children[idx]);
      }
    }
    return address;
  }

  protected Object[] ensureAddresses() {
    if (_addresses == null) {
      _addresses = new Object[_keys.length];
    }
    return _addresses;
  }

  protected Object[] ensureChildren() {
    if (_children == null) {
      _children = new Object[_keys.length];
    }
    return _children;
  }

  @Override
  public boolean contains(IRestore storage, Object key, Comparator cmp) {
    int idx = search(key, cmp);
    if (idx >= 0) return true;
    int ins = -idx - 1; 
    if (ins == _len) return false;
    assert 0 <= ins && ins < _len;
    return child(storage, ins).contains(storage, key, cmp);
  }

  @Override
  public Object[] add(IRestore storage, Object key, Comparator cmp, AtomicBoolean edit) {
    int idx = search(key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;
    
    int ins = -idx - 1;
    if (ins == _len) ins = _len - 1;
    assert 0 <= ins && ins < _len;
    Object[] nodes = child(storage, ins).add(storage, key, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling already in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }
    
    // same len, editable
    if (1 == nodes.length && editable()) {
      ANode node = (ANode) nodes[0];
      _keys[ins] = node.maxKey();
      address(ins, null);
      child(ins, node);
      if (ins == _len - 1 && node.maxKey() == maxKey()) // TODO why maxKey check?
        return new Object[]{this}; // update maxKey
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // same len, not editable
    if (1 == nodes.length) {
      ANode node = (ANode) nodes[0];
      Object[] newKeys;
      if (0 == cmp.compare(node.maxKey(), _keys[ins])) {
        newKeys = _keys;
      } else {
        newKeys = Arrays.copyOfRange(_keys, 0, _len);
        newKeys[ins] = node.maxKey();
      }

      Object[] newAddresses = null;
      Object[] newChildren = null;
      if (node == child(storage, ins)) { // TODO how is this possible?
        newAddresses = _addresses;
        newChildren = _children;
      } else {
        if (_addresses != null) {
          newAddresses = Arrays.copyOfRange(_addresses, 0, _len);
          newAddresses[ins] = null;
        }

        newChildren = _children == null ? new Object[_keys.length] : Arrays.copyOfRange(_children, 0, _len);
        newChildren[ins] = node;
      }

      return new Object[]{new Branch(_len, newKeys, newAddresses, newChildren, edit)};
    }

    // len + 1
    if (_len < PersistentSortedSet.MAX_LEN) {
      Branch n = new Branch(_len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(((ANode) nodes[0]).maxKey())
        .copyOne(((ANode) nodes[1]).maxKey())
        .copyAll(_keys, ins + 1, _len);

      if (_addresses != null) {
        n.ensureAddresses();
        new Stitch(n._addresses, 0)
          .copyAll(_addresses, 0, ins)
          .copyOne(null)
          .copyOne(null)
          .copyAll(_addresses, ins + 1, _len);
      }

      n.ensureChildren();
      new Stitch(n._children, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, _len);

      return new Object[]{n};
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
        .copyOne(((ANode) nodes[0]).maxKey())
        .copyOne(((ANode) nodes[1]).maxKey())
        .copyAll(_keys, ins+1, half1-1);
      Object[] keys2 = new Object[half2];
      ArrayUtil.copy(_keys, half1 - 1, _len, keys2, 0);

      Object[] addresses1 = null;
      Object[] addresses2 = null;
      if (_addresses != null) {
        addresses1 = new Object[half1];
        new Stitch(addresses1, 0)
          .copyAll(_addresses, 0, ins)
          .copyOne(null)
          .copyOne(null)
          .copyAll(_addresses, ins + 1, half1 - 1);
        addresses2 = new Object[half2];
        ArrayUtil.copy(_addresses, half1 - 1, _len, addresses2, 0);
      }

      Object[] children1 = new Object[half1];
      Object[] children2 = null;
      new Stitch(children1, 0)
        .copyAll(_children, 0, ins)
        .copyOne(nodes[0])
        .copyOne(nodes[1])
        .copyAll(_children, ins + 1, half1 - 1);
      if (_children != null) {
        children2 = new Object[half2];
        ArrayUtil.copy(_children, half1 - 1, _len, children2, 0);
      }

      return new Object[] {
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
      .copyOne(((ANode) nodes[0]).maxKey())
      .copyOne(((ANode) nodes[1]).maxKey())
      .copyAll(_keys, ins + 1, _len);

    Object addresses1[] = null;
    Object addresses2[] = null;
    if (_addresses != null) {
      addresses1 = new Object[half1];
      ArrayUtil.copy(_addresses, 0, half1, addresses1, 0);
      addresses2 = new Object[half2];
      new Stitch(addresses2, 0)
        .copyAll(_addresses, half1, ins)
        .copyOne(null)
        .copyOne(null)
        .copyAll(_addresses, ins + 1, _len);
    }

    Object[] children1 = null;
    Object[] children2 = new Object[half2];
    if (_children != null) {
      children1 = new Object[half1];
      ArrayUtil.copy(_children, 0, half1, children1, 0);
    }
    new Stitch(children2, 0)
      .copyAll(_children, half1, ins)
      .copyOne(nodes[0])
      .copyOne(nodes[1])
      .copyAll(_children, ins + 1, _len);

    return new Object[]{
      new Branch(half1, keys1, addresses1, children1, edit),
      new Branch(half2, keys2, addresses2, children2, edit)
    };
  }

  @Override
  public Object[] remove(IRestore storage, Object key, ANode _left, ANode _right, Comparator cmp, AtomicBoolean edit) {
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
    Object[] nodes = child(storage, idx).remove(storage, key, leftChild, rightChild, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling element not in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }

    boolean leftChanged = leftChild != nodes[0] || leftChildLen != safeLen((ANode) nodes[0]);
    boolean rightChanged = rightChild != nodes[2] || rightChildLen != safeLen((ANode) nodes[2]);

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
        Stitch ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                              ks.copyOne(((ANode) nodes[1]).maxKey());
        if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        if (_addresses != null) {
          Stitch as = new Stitch(_addresses, Math.max(idx - 1, 0));
          if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                                as.copyOne(null);
          if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
          if (newLen != _len)
            as.copyAll(_addresses, idx+2, _len);
        }

        ensureChildren();
        Stitch cs = new Stitch(_children, Math.max(idx - 1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(_children, idx+2, _len);

        _len = newLen;
        return PersistentSortedSet.EARLY_EXIT;
      }

      Branch newCenter = new Branch(newLen, edit);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                            ks.copyOne(((ANode) nodes[1]).maxKey());
      if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      if (_addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Object[] { left, newCenter, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(left._len + newLen, edit);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx - 1);
      if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                            ks.copyOne(((ANode) nodes[1]).maxKey());
      if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
      ks.copyAll(_keys,     idx + 2, _len);

      if (left._addresses != null || _addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(left._addresses, 0, left._len);
        as.copyAll(_addresses,      0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      join.ensureChildren();
      Stitch cs = new Stitch(join._children, 0);
      cs.copyAll(left._children, 0, left._len);
      cs.copyAll(_children,      0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Object[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(newLen + right._len, edit);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                            ks.copyOne(((ANode) nodes[1]).maxKey());
      if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
      ks.copyAll(_keys,       idx + 2, _len);
      ks.copyAll(right._keys, 0, right._len);

      if (_addresses != null || right._addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
        as.copyAll(right._addresses, 0, right._len);
      }

      join.ensureChildren();
      Stitch cs = new Stitch(join._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children,     idx + 2, _len);
      cs.copyAll(right._children, 0, right._len);
      
      return new Object[] { left, join, null };
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen;

      Branch newLeft   = new Branch(newLeftLen, edit),
             newCenter = new Branch(newCenterLen, edit);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                            ks.copyOne(((ANode) nodes[1]).maxKey());
      if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      if (left._addresses != null) {
        ArrayUtil.copy(left._addresses, 0, newLeftLen, newLeft.ensureAddresses(), 0);
      }
      if (left._children != null) {
        ArrayUtil.copy(left._children, 0, newLeftLen, newLeft.ensureChildren(), 0);
      }

      if (left._addresses != null || _addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(left._addresses, newLeftLen, left._len);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(left._children, newLeftLen, left._len);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      return new Object[] { newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Branch newCenter = new Branch(newCenterLen, edit),
             newRight  = new Branch(newRightLen, edit);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(((ANode) nodes[0]).maxKey());
                            ks.copyOne(((ANode) nodes[1]).maxKey());
      if (nodes[2] != null) ks.copyOne(((ANode) nodes[2]).maxKey());
      ks.copyAll(_keys, idx + 2, _len);
      ks.copyAll(right._keys, 0, rightHead);

      ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);

      if (_addresses != null || right._addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
        as.copyAll(right._addresses, 0, rightHead);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);
      cs.copyAll(right._children, 0, rightHead);

      if (right._addresses != null) {
        ArrayUtil.copy(right._addresses, rightHead, right._len, newRight.ensureAddresses(), 0);
      }
      if (right._children != null) {
        ArrayUtil.copy(right._children, rightHead, right._len, newRight.ensureChildren(), 0);
      }

      return new Object[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  @Override
  public void walk(IRestore storage, Object address, BiConsumer<Object, ANode> consumer) {
    consumer.accept(address, this);
    for (int i = 0; i < _len; ++i) {
      child(storage, i).walk(storage, address(i), consumer);
    }
  }

  @Override
  public Object store(IStore storage) {
    ensureAddresses();
    for (int i = 0; i < _len; ++i) {
      if (_addresses[i] == null) {
        assert _children != null;
        assert _children[i] != null;
        assert _children[i] instanceof ANode;
        address(i, ((ANode) _children[i]).store(storage));
      }
    }
    Object[] keys = _len == _keys.length ? _keys : Arrays.copyOfRange(_keys, 0, _len);
    Object[] addresses = _len == _addresses.length ? _addresses : Arrays.copyOfRange(_addresses, 0, _len);
    return storage.store(keys, addresses);
  }

  public String str(IRestore storage, int lvl) {
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
      ANode child = null;
      if(_children == null) {
        Object ref = _children[i];
        if (ref != null) {
          if (ref instanceof SoftReference) {
            child = (ANode) ((SoftReference) ref).get();
          } else {
            child = (ANode) ref;
          }
        }
      }
      if (child != null)
        child.toString(sb, address(i), indent + "  ");
      else
        sb.append(indent + "  " + address(i) + ": <lazy> ");
    }
  }
}