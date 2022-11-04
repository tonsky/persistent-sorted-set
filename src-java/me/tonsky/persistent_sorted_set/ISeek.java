package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

public interface ISeek {
  ISeq seek(Object to, Comparator cmp);
  default ISeq seek(Object to) { return seek(to, RT.DEFAULT_COMPARATOR); }
}
