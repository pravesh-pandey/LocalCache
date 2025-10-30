# LocalCache

LocalCache is a filesystem-backed cache that stores entries as sharded, hash-addressed files so lookups stay O(1) as the cache grows. The design targets production scenarios where clients need Redis-like ergonomics but want the durability and capacity of disk without keeping an in-memory index.

This repository now includes both a production-ready Java implementation (preferred for JVM/C++ caller ecosystems) and an earlier Python prototype.

## Highlights

- **O(1) Performance**: SHA-256 hashed filenames with configurable directory sharding (default 2/2)
- **Crash Safety**: Atomic writes (temp file + rename) with optional fsync for durability
- **Concurrency**: POSIX file locking (via `FileChannel.lock`) with non-blocking operations
- **TTL Support**: Metadata stored alongside payload; lazy expiration on read
- **Low RAM Usage**: Append-only access journal so eviction decisions avoid in-memory state
- **Background Cleaning**: Automatic eviction based on size/age limits
- **Batch Operations**: Multi-key get/put/delete for efficient bulk operations
- **Iteration**: Stream-based iteration over keys and entries
- **AutoCloseable**: Proper resource management with try-with-resources

## Java usage

### Basic Operations

```java
import com.localcache.LocalCache;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

// Create cache with automatic background cleaning
try (var cache = LocalCache.newBuilder(Path.of("/var/tmp/localcache"))
        .hashAlgorithm("SHA-256")
        .shardSizes(2, 2)
        .cleanInterval(Duration.ofMinutes(5))        // Auto-clean every 5 min
        .autoCleanMaxBytes(1_000_000_000L)           // Keep under 1GB
        .autoCleanMaxAge(Duration.ofHours(24))       // Drop entries > 24h old
        .build()) {

    // Simple string operations
    cache.putString("greeting", "hello");
    cache.getString("greeting").ifPresent(System.out::println);

    // Binary data with TTL and metadata
    cache.put("blob", dataBytes,
              Duration.ofMinutes(5),
              Map.of("source", "api", "version", "1.0"),
              "application/json");

    // Touch to extend TTL
    cache.touch("blob", Duration.ofMinutes(10));

    // Check existence
    if (cache.exists("greeting")) {
        cache.delete("greeting");
    }

    // Get full entry with metadata
    cache.getEntry("blob").ifPresent(entry -> {
        System.out.println("Created: " + entry.getCreatedAtMillis());
        System.out.println("Metadata: " + entry.getMetadata());
    });

    // Manual cleaning (also runs automatically if configured)
    CleanResult result = cache.clean(1_000_000L, Duration.ofHours(1));
    System.out.println("Removed: " + result.removedItems() + " items");

    // Stats
    Stats stats = cache.stats();
    System.out.println("Items: " + stats.items() + ", Bytes: " + stats.bytes());
}
```

### Advanced Features

```java
// Batch operations (more efficient than individual calls)
var entries = Map.of(
    "key1", "value1".getBytes(),
    "key2", "value2".getBytes(),
    "key3", "value3".getBytes()
);
cache.putMulti(entries);

var values = cache.getMulti(List.of("key1", "key2", "key3"));
int deleted = cache.deleteMulti(List.of("key1", "key2"));

// Iterate over all keys
cache.keys().forEach(System.out::println);

// Iterate over all entries
cache.entries()
     .filter(e -> e.getMetadata().containsKey("important"))
     .forEach(e -> System.out.println(e.getKey()));

// Non-blocking operations with timeout
Optional<byte[]> value = cache.tryGet("key", Duration.ofMillis(100));
boolean success = cache.tryPut("key", data, Duration.ofMillis(100));
```

### Building & testing

```bash
cd java
./gradlew test           # Run all tests
./gradlew build          # Build JAR with sources and Javadoc
./gradlew publishToMavenLocal  # Install to local Maven repo
```

Requirements: Java 17 or higher

## Python prototype (optional)

The original prototype lives under `localcache/` and exposes the same semantics in Python. It remains useful for quick experimentation or scripting environments.

```python
from localcache import LocalCache

cache = LocalCache("/tmp/localcache")
cache.set("hello", "world", ttl=60)
print(cache.get("hello"))
```

## What's New in v0.1.0

- ✅ Gradle wrapper included
- ✅ Background cleaner with automatic eviction
- ✅ Non-blocking operations with timeout (`tryGet`, `tryPut`)
- ✅ Stream-based iteration (`keys()`, `entries()`)
- ✅ Batch operations (`getMulti`, `putMulti`, `deleteMulti`)
- ✅ AutoCloseable for proper resource management
- ✅ Comprehensive test suite (16 tests)

## Roadmap

- CI/CD workflow (GitHub Actions)
- Async/reactive APIs for high-throughput use cases
- Optional compression (gzip/zstd) for large values
- Chunking support for streaming large objects
- Benchmarks comparing SSD vs NVMe workloads
- Additional language bindings (Python, Go, Rust)
