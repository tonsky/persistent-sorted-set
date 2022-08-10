package me.tonsky.persistent_sorted_set;
import java.util.UUID;

public interface StorageBackend {
    public void hitCache(Node node);
    public Leaf[] load(Node node);
    public UUID store(Node node, Leaf[] children);
}
