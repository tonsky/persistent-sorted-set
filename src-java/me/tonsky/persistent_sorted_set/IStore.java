package me.tonsky.persistent_sorted_set;

public interface IStore<Key, Address> {
    Address store(Key[] keys, Address[] addresses);
}
