package me.tonsky.persistent_sorted_set;

public interface IStorage<Key, Address> {
    ANode<Key, Address> restore(Address address);
    Address store(Key[] keys, Address[] addresses);
}
