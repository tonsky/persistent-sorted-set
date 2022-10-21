package me.tonsky.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Branch<Key, Address> extends ANode<Key, Address> {
  // 1+ for Branches
  public final int _level;

  // Nullable, null == no addresses
  // Only valid [0 ... _len-1]
  public Address[] _addresses;

  // Nullable, null == children not populated yet
  // Only valid [0 ... _len-1]
  // Object == ANode | SoftReference<ANode>
  public Object[] _children;

  // For i in [0.._len):
  // 
  // 1. Not stored:       (_addresses == null || _addresses[i] == null) && _children[i] == ANode
  // 2. Stored:            _addresses[i] == Object && _children[i] == SoftReference<ANode>
  // 3. Not restored yet:  _addresses[i] == Object && (_children == null || _children[i] == null)

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, AtomicBoolean edit) {
    super(len, keys, edit);
    assert level >= 1;
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _level     = level;
    _addresses = addresses;
    _children  = children;
  }

  public Branch(int level, int len, AtomicBoolean edit) {
    super(len, (Key[]) new Object[ANode.newLen(len, edit)], edit);
    assert level >= 1;

    _level     = level;
    _addresses = null;
    _children  = null;
  }

  public Branch(int level, List<Key> keys, List<Address> addresses) {
    this(level, keys.size(), (Key[]) keys.toArray(), (Address[]) addresses.toArray(), null, null);
  }

  protected Address[] ensureAddresses() {
    if (_addresses == null) {
      _addresses = (Address[]) new Object[_keys.length];
    }
    return _addresses;
  }

  public List<Address> addresses() {
    if (_addresses == null) {
      return (List<Address>) Arrays.asList(new Object[_len]);
    } else if (_addresses.length == _len) {
      return Arrays.asList(_addresses);
    } else {
      return Arrays.asList(Arrays.copyOfRange(_addresses, 0, _len));
    }
  }
  
  public Address address(int idx) {
    assert 0 <= idx && idx < _len;

    if (_addresses == null) {
      return null;
    }
    return _addresses[idx];
  }

  public Address address(int idx, Address address) {
    assert 0 <= idx && idx < _len;

    if (_addresses != null || address != null) {
      ensureAddresses();
      _addresses[idx] = address;
      if (_children != null) {
        _children[idx] = null;
      }
      // if (_children[idx] instanceof ANode) {
      //   _children[idx] = new SoftReference(_children[idx]);
      // }
    }
    return address;
  }

  public ANode<Key, Address> child(IStorage storage, int idx) {
    assert 0 <= idx && idx < _len;
    assert (_children != null && _children[idx] != null) || (_addresses != null && _addresses[idx] != null);

    ANode child = null;
    if (_children != null) {
      child = (ANode) _children[idx];
    }

    if (child == null) {
      child = storage.restore(_addresses[idx]);
      // ensureChildren()[idx] = new SoftReference<ANode>(child);
    }
    return child;
  }

  public ANode<Key, Address> child(int idx, ANode<Key, Address> child) {
    address(idx, null);
    if (_children != null || child != null) {
      ensureChildren();
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

  public int level() {
    return _level;
  }

  protected Object[] ensureChildren() {
    if (_children == null) {
      _children = new Object[_keys.length];
    }
    return _children;
  }

  @Override
  public boolean contains(IStorage storage, Key key, Comparator<Key> cmp) {
    int idx = search(key, cmp);
    if (idx >= 0) return true;
    int ins = -idx - 1; 
    if (ins == _len) return false;
    assert 0 <= ins && ins < _len;
    return child(storage, ins).contains(storage, key, cmp);
  }

  @Override
  public ANode[] add(IStorage storage, Key key, Comparator<Key> cmp, AtomicBoolean edit) {
    int idx = search(key, cmp);
    if (idx >= 0) { // already in set
      return PersistentSortedSet.UNCHANGED;
    }
    
    int ins = -idx - 1;
    if (ins == _len) ins = _len - 1;
    assert 0 <= ins && ins < _len;
    ANode[] nodes = child(storage, ins).add(storage, key, cmp, edit);

    if (PersistentSortedSet.UNCHANGED == nodes) { // child signalling already in set
      return PersistentSortedSet.UNCHANGED;
    }

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      return PersistentSortedSet.EARLY_EXIT;
    }
    
    // same len, editable
    if (1 == nodes.length && editable()) {
      ANode<Key, Address> node = nodes[0];
      _keys[ins] = node.maxKey();
      child(ins, node);
      if (ins == _len - 1 && node.maxKey() == maxKey()) // TODO why maxKey check?
        return new ANode[]{ this }; // update maxKey
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // same len, not editable
    if (1 == nodes.length) {
      ANode<Key, Address> node = nodes[0];
      Key[] newKeys;
      if (0 == cmp.compare(node.maxKey(), _keys[ins])) {
        newKeys = _keys;
      } else {
        newKeys = Arrays.copyOfRange(_keys, 0, _len);
        newKeys[ins] = node.maxKey();
      }

      Address[] newAddresses = null;
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

      return new ANode[]{ new Branch(_level, _len, newKeys, newAddresses, newChildren, edit) };
    }

    // len + 1
    if (_len < PersistentSortedSet.MAX_LEN) {
      Branch n = new Branch(_level, _len + 1, edit);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
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

      return new ANode[]{n};
    }

    // split
    int half1 = (_len + 1) >>> 1;
    if (ins+1 == half1) ++half1;
    int half2 = _len + 1 - half1;

    // add to first half
    if (ins < half1) {
      Key[] keys1 = (Key[]) new Object[half1];
      new Stitch(keys1, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(nodes[0].maxKey())
        .copyOne(nodes[1].maxKey())
        .copyAll(_keys, ins+1, half1-1);
      Key[] keys2 = (Key[]) new Object[half2];
      ArrayUtil.copy(_keys, half1 - 1, _len, keys2, 0);

      Address[] addresses1 = null;
      Address[] addresses2 = null;
      if (_addresses != null) {
        addresses1 = (Address[]) new Object[half1];
        new Stitch(addresses1, 0)
          .copyAll(_addresses, 0, ins)
          .copyOne(null)
          .copyOne(null)
          .copyAll(_addresses, ins + 1, half1 - 1);
        addresses2 = (Address[]) new Object[half2];
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

      return new ANode[] {
        new Branch(_level, half1, keys1, addresses1, children1, edit),
        new Branch(_level, half2, keys2, addresses2, children2, edit)
      };
    }

    // add to second half
    Key[] keys1 = (Key[]) new Object[half1];
    Key[] keys2 = (Key[]) new Object[half2];
    ArrayUtil.copy(_keys, 0, half1, keys1, 0);

    new Stitch(keys2, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(nodes[0].maxKey())
      .copyOne(nodes[1].maxKey())
      .copyAll(_keys, ins + 1, _len);

    Address addresses1[] = null;
    Address addresses2[] = null;
    if (_addresses != null) {
      addresses1 = (Address[]) new Object[half1];
      ArrayUtil.copy(_addresses, 0, half1, addresses1, 0);
      addresses2 = (Address[]) new Object[half2];
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

    return new ANode[]{
      new Branch(_level, half1, keys1, addresses1, children1, edit),
      new Branch(_level, half2, keys2, addresses2, children2, edit)
    };
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, AtomicBoolean edit) {
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
        Stitch ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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

      Branch newCenter = new Branch(_level, newLen, edit);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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

      return new ANode[] { left, newCenter, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(_level, left._len + newLen, edit);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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

      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right._len <= PersistentSortedSet.MAX_LEN) {
      Branch join = new Branch(_level, newLen + right._len, edit);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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
      
      return new ANode[] { left, join, null };
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen;
      int newLeftLen   = totalLen >>> 1;
      int newCenterLen = totalLen - newLeftLen;

      Branch newLeft   = new Branch(_level, newLeftLen, edit);
      Branch newCenter = new Branch(_level, newCenterLen, edit);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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

      return new ANode[] { newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Branch newCenter = new Branch(_level, newCenterLen, edit),
             newRight  = new Branch(_level, newRightLen, edit);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
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

      return new ANode[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  @Override
  public void walkAddresses(IStorage storage, IFn onAddress) {
    for (int i = 0; i < _len; ++i) {
      Address address = address(i);
      if (address != null) {
        onAddress.invoke(address);
      }
      if (_level > 1) {
        child(storage, i).walkAddresses(storage, onAddress);
      }
    }
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    ensureAddresses();
    for (int i = 0; i < _len; ++i) {
      if (_addresses[i] == null) {
        assert _children != null;
        assert _children[i] != null;
        assert _children[i] instanceof ANode;
        address(i, ((ANode<Key, Address>) _children[i]).store(storage));
      }
    }
    return storage.store(this);
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
  public void toString(StringBuilder sb, Address address, String indent) {
    sb.append(indent);
    sb.append("Branch addr: " + address + " len: " + _len + " ");
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      ANode child = null;
      if (_children != null) {
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