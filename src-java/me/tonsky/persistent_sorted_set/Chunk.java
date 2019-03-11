package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
class Chunk implements IChunk {
  final PersistentSortedSet _set;
  final Object[] _keys;
  final int _idx, _end;
  final boolean _asc;
  final int _version;

  Chunk(Seq seq) {
    _set  = seq._set;
    _asc  = seq._asc;
    _idx  = seq._idx;
    _keys = seq._node._keys;
    _version = seq._version;
    if (_asc) {
      int end = seq._node._len - 1;
      if (seq._keyTo != null)
        while (end > _idx && seq._cmp.compare(_keys[end], seq._keyTo) > 0)
          --end;
      _end = end;
    } else {
      int end = 0;
      if (seq._keyTo != null)
        while (end < _idx && seq._cmp.compare(_keys[end], seq._keyTo) < 0)
          ++end;
      _end = end;
    }
  }

  Chunk(PersistentSortedSet set, Object[] keys, int idx, int end, boolean asc, int version) {
    _set  = set;
    _keys = keys;
    _idx  = idx;
    _end  = end;
    _asc  = asc;
    _version = version;
  }

  void checkVersion() {
    if (_version != _set._version)
      throw new RuntimeException("Tovarisch, you are iterating and mutating a transient set at the same time!");
  }

  public IChunk dropFirst() {
    checkVersion();
    if (_idx == _end)
      throw new IllegalStateException("dropFirst of empty chunk");
    return new Chunk(_set, _keys, _asc ? _idx+1 : _idx-1, _end, _asc, _version);
  }

  public Object reduce(IFn f, Object start) {
    checkVersion();
    Object ret = f.invoke(start, _keys[_idx]);
    if (ret instanceof Reduced)
      return ((Reduced) ret).deref();
    if (_asc)
      for (int x = _idx + 1; x <= _end; ++x) {
        ret = f.invoke(ret, _keys[x]);
        if (ret instanceof Reduced)
          return ((Reduced) ret).deref();
      }
    else // !_asc
      for (int x = _idx - 1; x >= _end; --x) {
        ret = f.invoke(ret, _keys[x]);
        if (ret instanceof Reduced)
          return ((Reduced) ret).deref();
      }
    return ret;
  }

  public Object nth(int i) {
    checkVersion();
    assert (i >= 0 && i < count());
    return _asc ? _keys[_idx + i] : _keys[_idx - i];
  }

  public Object nth(int i, Object notFound) {
    checkVersion();
    if (i >= 0 && i < count())
      return nth(i);
    return notFound;
  }

  public int count() {
    checkVersion();
    if (_asc) return _end - _idx + 1;
    else return _idx - _end + 1;
  }
}