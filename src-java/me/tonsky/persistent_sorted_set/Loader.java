package me.tonsky.persistent_sorted_set;
import java.util.UUID;

public interface Loader {
    public Leaf[] load(UUID address);
    public UUID store(Leaf[] children);
}
