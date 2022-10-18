package me.tonsky.persistent_sorted_set;

public interface IRestore {
    ANode load(Object address);
}
