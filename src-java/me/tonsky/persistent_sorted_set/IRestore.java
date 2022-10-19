package me.tonsky.persistent_sorted_set;

public interface IRestore<Key, Address> {
    ANode<Key, Address> load(Address address);
}
