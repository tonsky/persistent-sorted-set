package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public interface IPersistentSortedSet<Key, Address> extends Seqable, Reversible, Sorted {
  ISeq slice(Key from, Key to, Comparator<Key> cmp);
  ISeq rslice(Key from, Key to, Comparator<Key> cmp);

  default ISeq slice(Key from, Key to) { return slice(from, to, comparator()); }
  default ISeq rslice(Key from, Key to) { return rslice(from, to, comparator()); }

  //  Seqable
  default ISeq seq() { return slice(null, null, comparator()); }

  // Reversible
  default ISeq rseq() { return rslice(null, null, comparator()); }

  // Sorted
  default ISeq seq(boolean asc) { return asc ? slice(null, null, comparator()) : rslice(null, null, comparator()); }
  default ISeq seqFrom(Object key, boolean asc) { return asc ? slice((Key) key, null, comparator()) : rslice((Key) key, null, comparator()); }
}