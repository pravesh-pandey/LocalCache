package com.localcache;

/**
 * Aggregate statistics about the cache contents.
 */
public record Stats(long items, long bytes) { }
