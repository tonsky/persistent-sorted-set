# 0.2.1

Added:

- `seek` to jump ahead during iteration #9 via @FiV0

# 0.2.0

Added:

- Durability in Clojure version #7 with @whilo
- `IStorage`, `store`, `restore`, `restore-by`, `walk-addresses`, `set-branching-factor!`

# 0.1.4

Special handling of nils in slice/rslice in CLJS, matching CLJ behaviour #6

# 0.1.3

Fixed NPE in `me.tonsky.persistent-sorted-set.arrays/array?` #4 #5 thx @timothypratley

# 0.1.2

Throw if iterating over a transient set that has been mutated.

# 0.1.1

Recompiled for Java 8.

# 0.1.0

Initial.