package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
class Chunk implements IChunk {
  final Object[] _keys;
  final int _idx, _end;
  final boolean _asc;

  Chunk(Seq seq) {
    _asc  = seq._asc;
    _idx  = seq._idx;
    _keys = seq._node._keys;
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

  Chunk(Object[] keys, int idx, int end, boolean asc) {
    _keys = keys;
    _idx  = idx;
    _end  = end;
    _asc  = asc;
  }

  public IChunk dropFirst() {
    if (_idx == _end)
      throw new IllegalStateException("dropFirst of empty chunk");
    return new Chunk(_keys, _asc ? _idx+1 : _idx-1, _end, _asc);
  }

  public Object reduce(IFn f, Object start) {
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
    assert (i >= 0 && i < count());
    return _asc ? _keys[_idx + i] : _keys[_idx - i];
  }

  public Object nth(int i, Object notFound) {
    if (i >= 0 && i < count())
      return nth(i);
    return notFound;
  }

  public int count() {
    if (_asc) return _end - _idx + 1;
    else return _idx - _end + 1;
  }
}