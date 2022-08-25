package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class PersistentSortedSet extends APersistentSortedSet implements IEditableCollection, ITransientSet, Reversible, Sorted, IReduce, IPersistentSortedSet {

  public static Leaf[] EARLY_EXIT = new Leaf[0],
                       UNCHANGED  = new Leaf[0];

  public static int MIN_LEN = 32, MAX_LEN = 64, EXPAND_LEN = 8;

  public static final PersistentSortedSet EMPTY = new PersistentSortedSet();

  public static void setMaxLen(int maxLen) {
    MAX_LEN = maxLen;
    MIN_LEN = maxLen >>> 1;
  }

  public Leaf _root;
  public final Edit _edit;
  public int _version = 0;

  public PersistentSortedSet() { this(null, RT.DEFAULT_COMPARATOR); }
  public PersistentSortedSet(Comparator cmp) { this(null, cmp); }
  public PersistentSortedSet(IPersistentMap meta, Comparator cmp) {
    super(meta, cmp);
    _edit  = new Edit(false);
    _root  = new Leaf(new Object[]{}, 0, _edit);
  }

  public PersistentSortedSet(IPersistentMap meta, Comparator cmp, Leaf root, Edit edit, int version) {
    super(meta, cmp);
    _root  = root;
    _edit  = edit;
    _version = version;
  }

  public void ensureEditable(boolean value) {
    if (value != _edit.editable())
      throw new RuntimeException("Expected" + (value ? " transient" : " persistent") + "set");
  }

  // IPersistentSortedSet
  public Seq slice(Object from, Object to) { return slice(from, to, _cmp); }
  public Seq slice(Object from, Object to, Comparator cmp) {
    assert from == null || to == null || cmp.compare(from, to) <= 0 : "From " + from + " after to " + to;
    Seq seq = null;
    Leaf node = _root;

    if (_root.count() == 0) return null;

    if (from == null) {
      while (true) {
        if (node instanceof Node) {
          seq = new Seq(null, this,seq, node, 0, null, null, true, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this,seq, node, 0, to, cmp, true, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      int idx = node.searchFirst(from, cmp);
      if (idx < 0) idx = -idx-1;
      if (idx == node._len) return null;
      if (node instanceof Node) {
        seq = new Seq(null, this,seq, node, idx, null, null, true, _version);
        node = seq.child();
      } else { // Leaf
        seq = new Seq(null, this,seq, node, idx, to, cmp, true, _version);
        return seq.over() ? null : seq;
      }
    }
  }

  public Seq rslice(Object from, Object to) { return rslice(from, to, _cmp); }
  public Seq rslice(Object from, Object to, Comparator cmp) {
    assert from == null || to == null || cmp.compare(from, to) >= 0 : "From " + from + " before to " + to;
    Seq seq = null;
    Leaf node = _root;

    if (_root.count() == 0) return null;

    if (from == null) {
      while (true) {
        int idx = node._len-1;
        if (node instanceof Node) {
          seq = new Seq(null, this,seq, node, idx, null, null, false, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this,seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      if (node instanceof Node) {
        int idx = node.searchLast(from, cmp) + 1;
        if (idx == node._len) --idx; // last or beyond, clamp to last
        seq = new Seq(null, this,seq, node, idx, null, null, false, _version);
        node = seq.child();
      } else { // Leaf
        int idx = node.searchLast(from, cmp);
        if (idx == -1) { // not in this, so definitely in prev
          seq = new Seq(null, this,seq, node, 0, to, cmp, false, _version);
          return seq.advance() ? seq : null;
        } else { // exact match
          seq = new Seq(null, this,seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("#{");
    for(Object o: this)
      sb.append(o).append(" ");
    if (sb.charAt(sb.length()-1) == " ".charAt(0))
      sb.delete(sb.length()-1, sb.length());
    sb.append("}");
    return sb.toString();
  }

  public String str() { return _root.str(0); }


  // IObj
  public PersistentSortedSet withMeta(IPersistentMap meta) {
    if(_meta == meta) return this;
    return new PersistentSortedSet(meta, _cmp, _root, _edit, _version);
  }

  // Counted
  public int count() { return _root.count(); }

  // Sorted
  public Comparator comparator() { return _cmp; }
  public Object entryKey(Object entry) { return entry; }

  // IReduce
  public Object reduce(IFn f) {
    Seq seq = (Seq) seq();
    return seq == null ? f.invoke() : seq.reduce(f);
  }

  public Object reduce(IFn f, Object start) {
    Seq seq = (Seq) seq();
    return seq == null ? start : seq.reduce(f, start);
  }

  // IPersistentCollection
  public PersistentSortedSet empty() {
    return new PersistentSortedSet(_meta, _cmp);
  }

  public PersistentSortedSet cons(Object key) {
    return cons(key, _cmp);
  }

  public PersistentSortedSet cons(Object key, Comparator cmp) {
    Leaf nodes[] = _root.add(key, cmp, _edit);

    if (UNCHANGED == nodes)
      return this;

    if (_edit.editable()) {
      if (1 == nodes.length)
        _root = nodes[0];
      if (2 == nodes.length) {
        Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
        _root = new Node(keys, nodes, 2, _edit);
      }
      _version++;
      return this;
    }

    if (1 == nodes.length)
      return new PersistentSortedSet(_meta, _cmp, nodes[0], _edit, _version+1);
    
    Object keys[] = new Object[] { nodes[0].maxKey(), nodes[1].maxKey() };
    Leaf newRoot = new Node(keys, nodes, 2, _root.count() + 1, _edit);
    return new PersistentSortedSet(_meta, _cmp, newRoot, _edit, _version+1);
  }

  // IPersistentSet
  public PersistentSortedSet disjoin(Object key) {
    return disjoin(key, _cmp);
  }

  public PersistentSortedSet disjoin(Object key, Comparator cmp) { 
    Leaf nodes[] = _root.remove(key, null, null, cmp, _edit);

    // not in set
    if (UNCHANGED == nodes) return this;
    // in place update
    if (nodes == EARLY_EXIT) { _version++; return this; }
    Leaf newRoot = nodes[1];
    if (_edit.editable()) {
      if (newRoot instanceof Node && newRoot._len == 1)
        newRoot = ((Node) newRoot)._children[0];
      _root = newRoot;
      _version++;
      return this;
    }
    if (newRoot instanceof Node && newRoot._len == 1) {
      newRoot = ((Node) newRoot)._children[0];
      return new PersistentSortedSet(_meta, _cmp, newRoot, _edit, _version+1);
    }
    return new PersistentSortedSet(_meta, _cmp, newRoot, _edit, _version+1);
  }

  public boolean contains(Object key) {
    return _root.contains(key, _cmp);
  }

  // IEditableCollection
  public PersistentSortedSet asTransient() {
    ensureEditable(false);
    return new PersistentSortedSet(_meta, _cmp, _root, new Edit(true), _version);
  }

  // ITransientCollection
  public PersistentSortedSet conj(Object key) {
    return cons(key, _cmp);
  }

  public PersistentSortedSet persistent() {
    ensureEditable(true);
    _edit.setEditable(false);
    return this;
  }

  // Iterable
  public Iterator iterator() {
    return new JavaIter((Seq) seq());
  }
}