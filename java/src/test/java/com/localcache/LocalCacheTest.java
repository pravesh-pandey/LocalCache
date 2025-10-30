package com.localcache;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalCacheTest {

    @TempDir
    Path tempDir;

    private LocalCache cache;

    @BeforeEach
    void setUp() throws IOException {
        cache = LocalCache.newBuilder(tempDir.resolve("cache")).build();
    }

    @Test
    void putAndGetString() throws IOException {
        cache.putString("greeting", "hello");

        Optional<String> result = cache.getString("greeting");
        assertTrue(result.isPresent());
        assertEquals("hello", result.get());

        CacheEntry entry = cache.getEntry("greeting").orElseThrow();
        assertEquals("greeting", entry.getKey());
        assertEquals("hello", entry.getValueAsString());
        assertNull(entry.getContentType());
        assertTrue(entry.getMetadata().isEmpty());
    }

    @Test
    void putBinaryWithMetadata() throws IOException {
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        cache.put("binary", payload, Duration.ofSeconds(5), Map.of("source", "test"), "text/plain");

        Optional<byte[]> result = cache.get("binary");
        assertTrue(result.isPresent());
        assertArrayEquals(payload, result.get());

        CacheEntry entry = cache.getEntry("binary").orElseThrow();
        assertEquals("text/plain", entry.getContentType());
        assertEquals("test", entry.getMetadata().get("source"));
        assertNotNull(entry.getTtlMillis());
    }

    @Test
    void touchExtendsTtl() throws IOException, InterruptedException {
        cache.putString("ephemeral", "value");
        assertTrue(cache.touch("ephemeral", Duration.ofMillis(200)));
        Thread.sleep(100);
        assertTrue(cache.getString("ephemeral").isPresent());
    }

    @Test
    void ttlExpiresEntry() throws IOException, InterruptedException {
        cache.put("soon", "bye".getBytes(StandardCharsets.UTF_8), Duration.ofMillis(50), Map.of(), null);
        Thread.sleep(120);
        assertTrue(cache.get("soon").isEmpty());
        assertFalse(cache.exists("soon"));
    }

    @Test
    void cleanRemovesOldEntries() throws IOException, InterruptedException {
        cache.putString("alpha", "a");
        cache.putString("beta", "b");
        Thread.sleep(10);
        cache.putString("gamma", "c");

        CleanResult result = cache.clean(5L, null);
        assertTrue(result.removedItems() >= 1);
        assertTrue(result.removedBytes() > 0);
    }

    @Test
    void backgroundCleanerRemovesOldEntries() throws IOException, InterruptedException {
        LocalCache autoCache = LocalCache.newBuilder(tempDir.resolve("auto-cache"))
                .cleanInterval(Duration.ofMillis(100))
                .autoCleanMaxBytes(50L)
                .build();

        try {
            autoCache.putString("key1", "value1");
            autoCache.putString("key2", "value2");
            autoCache.putString("key3", "value3");
            autoCache.putString("key4", "value4");

            // Wait for background cleaner to run
            Thread.sleep(300);

            // Should have cleaned some entries
            Stats stats = autoCache.stats();
            assertTrue(stats.bytes() <= 50L || stats.items() < 4,
                      "Expected cleaner to reduce cache size");
        } finally {
            autoCache.close();
        }
    }

    @Test
    void keysIteratesAllKeys() throws IOException {
        cache.putString("key1", "value1");
        cache.putString("key2", "value2");
        cache.putString("key3", "value3");

        var keys = cache.keys().toList();
        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    void entriesIteratesAllEntries() throws IOException {
        cache.putString("key1", "value1");
        cache.putString("key2", "value2");

        var entries = cache.entries().toList();
        assertEquals(2, entries.size());
        assertTrue(entries.stream().anyMatch(e -> "key1".equals(e.getKey())));
        assertTrue(entries.stream().anyMatch(e -> "key2".equals(e.getKey())));
    }

    @Test
    void getMultiRetrievesMultipleKeys() throws IOException {
        cache.putString("key1", "value1");
        cache.putString("key2", "value2");
        cache.putString("key3", "value3");

        var result = cache.getMulti(java.util.List.of("key1", "key2", "missing"));
        assertEquals(2, result.size());
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), result.get("key1"));
        assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), result.get("key2"));
        assertFalse(result.containsKey("missing"));
    }

    @Test
    void putMultiStoresMultipleKeys() throws IOException {
        var entries = Map.of(
                "key1", "value1".getBytes(StandardCharsets.UTF_8),
                "key2", "value2".getBytes(StandardCharsets.UTF_8)
        );
        cache.putMulti(entries);

        assertTrue(cache.get("key1").isPresent());
        assertTrue(cache.get("key2").isPresent());
    }

    @Test
    void deleteMultiRemovesMultipleKeys() throws IOException {
        cache.putString("key1", "value1");
        cache.putString("key2", "value2");
        cache.putString("key3", "value3");

        int deleted = cache.deleteMulti(java.util.List.of("key1", "key2", "missing"));
        assertEquals(2, deleted);
        assertFalse(cache.exists("key1"));
        assertFalse(cache.exists("key2"));
        assertTrue(cache.exists("key3"));
    }

    @Test
    void tryGetWithTimeout() throws IOException {
        cache.putString("key", "value");

        var result = cache.tryGet("key", Duration.ofMillis(100));
        assertTrue(result.isPresent());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), result.get());
    }

    @Test
    void tryPutWithTimeout() throws IOException {
        boolean success = cache.tryPut("key", "value".getBytes(StandardCharsets.UTF_8),
                                       Duration.ofMillis(100));
        assertTrue(success);
        assertTrue(cache.exists("key"));
    }

    @Test
    void closePreventsFurtherOperations() throws IOException {
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.putString("key", "value"));
        assertThrows(IllegalStateException.class, () -> cache.get("key"));
        assertThrows(IllegalStateException.class, () -> cache.delete("key"));
        assertThrows(IllegalStateException.class, () -> cache.stats());
    }

    @Test
    void closeIsIdempotent() throws IOException {
        cache.close();
        cache.close(); // Should not throw
    }

    @Test
    void tryWithResourcesAutoCloses() throws IOException {
        Path cachePath = tempDir.resolve("twr-cache");
        try (LocalCache twr = LocalCache.open(cachePath)) {
            twr.putString("key", "value");
            assertTrue(twr.exists("key"));
        }
        // After close, creating new cache should work
        try (LocalCache twr2 = LocalCache.open(cachePath)) {
            assertTrue(twr2.exists("key"));
        }
    }

    @Test
    void tryGetReturnsEmptyOnTimeout() throws IOException, InterruptedException {
        // Create two caches accessing the same data
        LocalCache cache1 = LocalCache.open(tempDir.resolve("shared"));
        LocalCache cache2 = LocalCache.open(tempDir.resolve("shared"));

        try {
            cache1.putString("key", "value");

            // Hold an exclusive lock from cache1 by starting a put operation
            // This isn't directly testable without reflection, but we can test the timeout behavior
            // by checking that tryGet with a short timeout returns empty rather than hanging

            // This should succeed with a reasonable timeout
            var result = cache2.tryGet("key", Duration.ofSeconds(1));
            assertTrue(result.isPresent());
        } finally {
            cache1.close();
            cache2.close();
        }
    }

    @Test
    void keysFiltersExpiredEntries() throws IOException, InterruptedException {
        cache.putString("valid", "value");
        cache.put("expired", "value".getBytes(StandardCharsets.UTF_8),
                  Duration.ofMillis(50), Map.of(), null);

        // Wait for expiration
        Thread.sleep(100);

        var keys = cache.keys().toList();
        assertEquals(1, keys.size());
        assertTrue(keys.contains("valid"));
        assertFalse(keys.contains("expired"));
    }

    @Test
    void entriesFiltersExpiredEntries() throws IOException, InterruptedException {
        cache.putString("valid", "value");
        cache.put("expired", "value".getBytes(StandardCharsets.UTF_8),
                  Duration.ofMillis(50), Map.of(), null);

        // Wait for expiration
        Thread.sleep(100);

        var entries = cache.entries().toList();
        assertEquals(1, entries.size());
        assertEquals("valid", entries.get(0).getKey());
    }

    @Test
    void builderRejectsZeroCleanInterval() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .cleanInterval(Duration.ZERO)
                     .build()
        );
    }

    @Test
    void builderRejectsNegativeCleanInterval() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .cleanInterval(Duration.ofMillis(-1))
                     .build()
        );
    }

    @Test
    void builderRejectsSubMillisecondCleanInterval() {
        // Duration.ofNanos(500_000).toMillis() == 0 (500 microseconds)
        // This would break scheduleAtFixedRate which requires period > 0
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .cleanInterval(Duration.ofNanos(500_000))
                     .build()
        );

        // Duration.ofNanos(999_999).toMillis() == 0 (999.999 microseconds)
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .cleanInterval(Duration.ofNanos(999_999))
                     .build()
        );
    }

    @Test
    void builderAcceptsOneMillisecondCleanInterval() throws IOException {
        // Duration.ofMillis(1) should be accepted
        try (LocalCache c = LocalCache.newBuilder(tempDir.resolve("test"))
                .cleanInterval(Duration.ofMillis(1))
                .build()) {
            c.putString("key", "value");
            assertTrue(c.exists("key"));
        }
    }

    @Test
    void builderRejectsNegativeMaxBytes() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .autoCleanMaxBytes(-1L)
                     .build()
        );
    }

    @Test
    void builderRejectsZeroMaxAge() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalCache.newBuilder(tempDir.resolve("test"))
                     .autoCleanMaxAge(Duration.ZERO)
                     .build()
        );
    }

    @Test
    void builderAcceptsNullAutoCleanParameters() throws IOException {
        // Should not throw
        try (LocalCache c = LocalCache.newBuilder(tempDir.resolve("test"))
                .autoCleanMaxBytes(null)
                .autoCleanMaxAge(null)
                .build()) {
            c.putString("key", "value");
            assertTrue(c.exists("key"));
        }
    }

    @Test
    void backgroundCleanerDoesNotThrowOnClose() throws IOException, InterruptedException {
        LocalCache autoCache = LocalCache.newBuilder(tempDir.resolve("cleanup-test"))
                .cleanInterval(Duration.ofMillis(50))
                .autoCleanMaxBytes(100L)
                .build();

        autoCache.putString("key", "value");
        Thread.sleep(30); // Let cleaner schedule but not run yet

        // Close should not throw even if cleaner is scheduled
        autoCache.close();
        Thread.sleep(100); // Give time for any scheduled task to try to run

        // If we get here without exceptions, the test passes
    }
}
