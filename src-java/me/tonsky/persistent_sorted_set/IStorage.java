package me.tonsky.persistent_sorted_set;

public interface IStorage {
    ANode load(Object address);
}
