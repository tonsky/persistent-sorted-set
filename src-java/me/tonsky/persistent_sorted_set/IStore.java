package me.tonsky.persistent_sorted_set;

public interface IStore {
    Object store(Object[] keys, Object[] addresses);
}
