package me.tonsky.persistent_sorted_set;

public interface IStorage {
    Node load(Object address);
}
