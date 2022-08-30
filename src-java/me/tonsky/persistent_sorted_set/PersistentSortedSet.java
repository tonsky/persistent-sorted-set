package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.concurrent.atomic.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class PersistentSortedSet extends APersistentSortedSet implements IEditableCollection, ITransientSet, Reversible, Sorted, IReduce, IPersistentSortedSet {

  public static Node[] EARLY_EXIT = new Node[0],
                       UNCHANGED  = new Node[0];

  public static int MIN_LEN = 32, MAX_LEN = 64, EXPAND_LEN = 8;

  public static final PersistentSortedSet EMPTY = new PersistentSortedSet();

  public static void setMaxLen(int maxLen) {
    MAX_LEN = maxLen;
    MIN_LEN = maxLen >>> 1;
  }

  public Node _root;
  public int _count;
  public int _version;
  public final AtomicBoolean _edit;
  public final IStorage _storage;

  public PersistentSortedSet() {
    this(null, RT.DEFAULT_COMPARATOR);
  }
  
  public PersistentSortedSet(Comparator cmp) {
    this(null, cmp);
  }
  
  public PersistentSortedSet(IPersistentMap meta, Comparator cmp) {
    this(meta, cmp, null, new Node(new Object[]{}, 0, null), 0, null, 0);
  }

  public PersistentSortedSet(IPersistentMap meta, Comparator cmp, IStorage storage, Node root, int count, AtomicBoolean edit, int version) {
    super(meta, cmp);
    _root    = root;
    _count   = count;
    _version = version;
    _edit    = edit;
    _storage = storage;
  }

  private int alterCount(int delta) {
    return _count < 0 ? _count : _count + delta;
  }

  public boolean editable() {
    return _edit != null && _edit.get();
  }

  // IPersistentSortedSet
  public Seq slice(Object from, Object to) {
    return slice(from, to, _cmp);
  }

  public Seq slice(Object from, Object to, Comparator cmp) {
    assert from == null || to == null || cmp.compare(from, to) <= 0 : "From " + from + " after to " + to;
    Seq seq = null;
    Node node = _root;

    if (_root.len(_storage) == 0)
      return null;

    if (from == null) {
      while (true) {
        if (node.branch(_storage)) {
          seq = new Seq(null, this, seq, node, 0, null, null, true, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this, seq, node, 0, to, cmp, true, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      int idx = node.searchFirst(_storage, from, cmp);
      if (idx < 0) idx = -idx-1;
      if (idx == node._len) return null;
      if (node.branch(_storage)) {
        seq = new Seq(null, this, seq, node, idx, null, null, true, _version);
        node = seq.child();
      } else {
        seq = new Seq(null, this, seq, node, idx, to, cmp, true, _version);
        return seq.over() ? null : seq;
      }
    }
  }

  public Seq rslice(Object from, Object to) { return rslice(from, to, _cmp); }
  public Seq rslice(Object from, Object to, Comparator cmp) {
    assert from == null || to == null || cmp.compare(from, to) >= 0 : "From " + from + " before to " + to;
    Seq seq = null;
    Node node = _root;

    if (_root.len(_storage) == 0)
      return null;

    if (from == null) {
      while (true) {
        node.ensureLoaded(_storage);
        int idx = node._len - 1;
        if (node.branch(_storage)) {
          seq = new Seq(null, this, seq, node, idx, null, null, false, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this, seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      if (node.branch(_storage)) {
        int idx = node.searchLast(_storage, from, cmp) + 1;
        if (idx == node._len) --idx; // last or beyond, clamp to last
        seq = new Seq(null, this, seq, node, idx, null, null, false, _version);
        node = seq.child();
      } else {
        int idx = node.searchLast(_storage, from, cmp);
        if (idx == -1) { // not in this, so definitely in prev
          seq = new Seq(null, this, seq, node, 0, to, cmp, false, _version);
          return seq.advance() ? seq : null;
        } else { // exact match
          seq = new Seq(null, this, seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("#{");
    for(Object o: this)
      sb.append(o).append(" ");
    if (sb.charAt(sb.length() - 1) == " ".charAt(0))
      sb.delete(sb.length() - 1, sb.length());
    sb.append("}");
    return sb.toString();
  }

  public String str() {
    return _root.str(_storage, 0);
  }

  // IObj
  public PersistentSortedSet withMeta(IPersistentMap meta) {
    if (_meta == meta)
      return this;
    return new PersistentSortedSet(meta, _cmp, _storage, _root, _count, _edit, _version);
  }

  // Counted
  public int count() {
    if (_count < 0)
      _count = _root.count(_storage);
    // assert _count == _root.count(_storage) : _count + " != " + _root.count(_storage);
    return _count;
  }

  // Sorted
  public Comparator comparator() {
    return _cmp;
  }

  public Object entryKey(Object entry) {
    return entry;
  }

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
    Node[] nodes = _root.add(_storage, key, cmp, _edit);

    if (UNCHANGED == nodes)
      return this;

    if (editable()) {
      if (1 == nodes.length)
        _root = nodes[0];
      if (2 == nodes.length) {
        Object[] keys = new Object[] { nodes[0].maxKey(_storage), nodes[1].maxKey(_storage) };
        _root = new Node(keys, nodes, 2, _edit);
      }
      _count = alterCount(1);
      _version += 1;
      return this;
    }

    if (1 == nodes.length)
      return new PersistentSortedSet(_meta, _cmp, _storage, nodes[0], alterCount(1), _edit, _version + 1);
    
    Object[] keys = new Object[] { nodes[0].maxKey(_storage), nodes[1].maxKey(_storage) };
    Node newRoot = new Node(keys, nodes, 2, _edit);
    return new PersistentSortedSet(_meta, _cmp, _storage, newRoot, alterCount(1), _edit, _version + 1);
  }

  // IPersistentSet
  public PersistentSortedSet disjoin(Object key) {
    return disjoin(key, _cmp);
  }

  public PersistentSortedSet disjoin(Object key, Comparator cmp) { 
    Node[] nodes = _root.remove(_storage, key, null, null, cmp, _edit);

    // not in set
    if (UNCHANGED == nodes)
      return this;

    // in place update
    if (nodes == EARLY_EXIT) {
      _count = alterCount(-1);
      _version += 1;
      return this;
    }

    Node newRoot = nodes[1];
    if (editable()) {
      if (newRoot.branch(_storage) && newRoot._len == 1)
        newRoot = ((Node) newRoot)._children[0];
      _root = newRoot;
      _count = alterCount(-1);
      _version += 1;
      return this;
    }
    if (newRoot.branch(_storage) && newRoot._len == 1) {
      newRoot = ((Node) newRoot)._children[0];
      return new PersistentSortedSet(_meta, _cmp, _storage, newRoot, alterCount(-1), _edit, _version + 1);
    }
    return new PersistentSortedSet(_meta, _cmp, _storage, newRoot, alterCount(-1), _edit, _version + 1);
  }

  public boolean contains(Object key) {
    return _root.contains(_storage, key, _cmp);
  }

  // IEditableCollection
  public PersistentSortedSet asTransient() {
    if (editable())
      throw new IllegalStateException("Expected persistent set");
    return new PersistentSortedSet(_meta, _cmp, _storage, _root, _count, new AtomicBoolean(true), _version);
  }

  // ITransientCollection
  public PersistentSortedSet conj(Object key) {
    return cons(key, _cmp);
  }

  public PersistentSortedSet persistent() {
    if (!editable())
      throw new IllegalStateException("Expected transient set");
    _edit.set(false);
    return this;
  }

  // Iterable
  public Iterator iterator() {
    return new JavaIter((Seq) seq());
  }
}