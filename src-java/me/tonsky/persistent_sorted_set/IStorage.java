package me.tonsky.persistent_sorted_set;

public interface IStorage<Key, Address> {
    /**
     * Given address, reconstruct and (optionally) cache the node.
     * Set itself would not store any strong references to nodes and
     * will request them by address during its operation many times.
     * 
     * Use ANode.restore() or Leaf(keys)/Branch(level, keys, addresses) ctors 
     */
    ANode<Key, Address> restore(Address address);

    /**
     * Will be called after all children of node has been stored and have addresses.
     * 
     * For node instanceof Leaf, store node.keys()
     * For node instanceof Branch, store node.level(), node.keys() and node.addresses()
     * Generate and return new address for node
     * Return null if doesn’t need to be stored
     */
    Address store(ANode<Key, Address> node);
}
