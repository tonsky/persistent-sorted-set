package me.tonsky.persistent_sorted_set;

import java.lang.ref.*;
import java.util.concurrent.atomic.*;

public class Settings {
  public final int _branchingFactor;
  public final RefType _refType;
  public final AtomicBoolean _edit;

  public Settings(int branchingFactor, RefType refType, AtomicBoolean edit) {
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = edit;
  }

  public Settings() {
    this(0, null);
  }

  public Settings(int branchingFactor) {
    this(branchingFactor, null);
  }

  public Settings(int branchingFactor, RefType refType) {
    if (branchingFactor <= 0) {
      branchingFactor = 512;
    }
    if (null == refType) {
      refType = RefType.SOFT;
    }
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = null;
  }

  public int minBranchingFactor() {
    return _branchingFactor >>> 1;
  }

  public int branchingFactor() {
    return _branchingFactor;
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
    return new Settings(_branchingFactor, _refType, new AtomicBoolean(value));
  }

  public void persistent() {
    assert _edit != null;
    _edit.set(false);
  }

  public <T> Object makeReference(T value) {
    switch (_refType) {
    case STRONG:
      return value;
    case SOFT:
      return new SoftReference<T>(value);
    case WEAK:
      return new WeakReference<T>(value);
    default:
      throw new RuntimeException("Unexpected _refType: " + _refType);
    }
  }

  public Object readReference(Object ref) {
    return ref instanceof Reference ? ((Reference) ref).get() : ref;
  }
}