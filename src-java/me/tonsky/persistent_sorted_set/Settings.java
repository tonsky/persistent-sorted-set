package me.tonsky.persistent_sorted_set;

import java.lang.ref.*;
import java.util.concurrent.atomic.*;

public class Settings {
  public final int _maxLen;
  public final RefType _refType;
  public final AtomicBoolean _edit;

  public Settings(int maxLen, RefType refType, AtomicBoolean edit) {
    _maxLen = maxLen;
    _refType = refType;
    _edit = edit;
  }

  public Settings() {
    _maxLen = 64;
    _refType = RefType.SOFT;
    _edit = null;
  }

  public Settings(int maxLen) {
    _maxLen = maxLen;
    _refType = RefType.SOFT;
    _edit = null;
  }

  public Settings(int maxLen, RefType refType) {
    if (maxLen <= 0) {
      maxLen = 64;
    }
    if (null == refType) {
      refType = RefType.SOFT;
    }
    _maxLen = maxLen;
    _refType = refType;
    _edit = null;
  }

  public int minLen() {
    return _maxLen >>> 1;
  }

  public int maxLen() {
    return _maxLen;
  }

  public int expandLen() {
    return 8;
  }

  public RefType refType() {
    return _refType;
  }

  public boolean editable() {
    return _edit != null && _edit.get();
  }

  public Settings editable(boolean value) {
    assert !editable();
    assert value == true;
    return new Settings(_maxLen, _refType, new AtomicBoolean(value));
  }

  public void persistent() {
    assert _edit != null;
    _edit.set(false);
  }

  public Object makeReference(Object value) {
    switch (_refType) {
    case STRONG:
      return value;
    case SOFT:
      return new SoftReference(value);
    case WEAK:
      return new WeakReference(value);
    }
    throw new RuntimeException("Unexpected _refType: " + _refType);
  }

  public Object readReference(Object ref) {
    return ref instanceof Reference ? ((Reference) ref).get() : ref;
  }
}