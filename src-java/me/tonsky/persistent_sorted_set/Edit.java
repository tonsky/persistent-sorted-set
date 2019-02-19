package me.tonsky.persistent_sorted_set;

public class Edit {
  public volatile boolean _value = false;
  public Edit(boolean value) { _value = value; }
  public boolean editable() { return _value; }
  public void setEditable(boolean value) { _value = value; }
}
