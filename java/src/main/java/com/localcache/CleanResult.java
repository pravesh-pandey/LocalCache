package com.localcache;

/**
 * Result metadata from a clean/eviction run.
 */
public record CleanResult(long removedItems, long removedBytes, long remainingBytes) { }
