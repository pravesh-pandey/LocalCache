package com.localcache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Filesystem-backed cache with O(1) hashed lookups and atomic writes.
 */
public final class LocalCache implements AutoCloseable {

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final Base64.Encoder B64_URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64_URL_DEC = Base64.getUrlDecoder();
    private static final int HEADER_VERSION = 1;

    private final Path rootDir;
    private final ThreadLocal<MessageDigest> digest;
    private final int[] shardSizes;
    private final boolean fsyncOnWrite;
    private final boolean enableLocks;
    private final boolean accessJournal;
    private final Path journalPath;
    private final SecureRandom secureRandom;
    private final ScheduledExecutorService cleanerScheduler;
    private final Duration cleanInterval;
    private final Long autoCleanMaxBytes;
    private final Duration autoCleanMaxAge;
    private volatile boolean closed = false;

    private LocalCache(Builder builder) throws IOException {
        this.rootDir = builder.rootDir.toAbsolutePath().normalize();
        this.digest = ThreadLocal.withInitial(() -> createDigest(builder.hashAlgorithm));
        this.shardSizes = builder.shardSizes.clone();
        this.fsyncOnWrite = builder.fsyncOnWrite;
        this.enableLocks = builder.enableLocks;
        this.accessJournal = builder.accessJournal;
        this.journalPath = this.rootDir.resolve("access.journal");
        this.secureRandom = new SecureRandom();
        this.cleanInterval = builder.cleanInterval;
        this.autoCleanMaxBytes = builder.autoCleanMaxBytes;
        this.autoCleanMaxAge = builder.autoCleanMaxAge;
        Files.createDirectories(this.rootDir);

        // Start background cleaner if configured
        if (builder.cleanInterval != null) {
            this.cleanerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "LocalCache-Cleaner");
                t.setDaemon(true);
                return t;
            });
            long intervalMillis = builder.cleanInterval.toMillis();
            this.cleanerScheduler.scheduleAtFixedRate(
                this::runBackgroundClean,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
            );
        } else {
            this.cleanerScheduler = null;
        }
    }

    /**
     * Creates a builder for configuring a {@link LocalCache} rooted at {@code rootDir}.
     *
     * @param rootDir directory where cache data will be stored
     * @return builder for further customization
     * @throws NullPointerException if {@code rootDir} is {@code null}
     */
    public static Builder newBuilder(Path rootDir) {
        Objects.requireNonNull(rootDir, "rootDir");
        return new Builder(rootDir);
    }

    /**
     * Opens a cache with default settings at {@code rootDir}.
     *
     * @param rootDir directory where cache data lives
     * @return initialized cache instance
     * @throws IOException if the cache cannot be created
     */
    public static LocalCache open(Path rootDir) throws IOException {
        return newBuilder(rootDir).build();
    }

    /**
     * Stores a binary value for {@code key}, replacing any existing entry.
     *
     * @param key cache key
     * @param value payload bytes
     * @throws IOException if persisting the value fails
     */
    public void put(String key, byte[] value) throws IOException {
        checkNotClosed();
        put(key, value, null, Collections.emptyMap(), null);
    }

    /**
     * Stores a UTF-8 encoded string value for {@code key}.
     *
     * @param key cache key
     * @param value string payload
     * @throws IOException if persisting the value fails
     */
    public void putString(String key, String value) throws IOException {
        checkNotClosed();
        put(key, value.getBytes(UTF8), null, Collections.emptyMap(), null, true, UTF8);
    }

    /**
     * Stores a binary payload along with optional TTL, metadata, and content type.
     *
     * @param key cache key
     * @param value payload bytes
     * @param ttl optional time-to-live
     * @param metadata optional metadata copied into the entry
     * @param contentType optional MIME type
     * @throws IOException if persisting the value fails
     */
    public void put(
            String key,
            byte[] value,
            Duration ttl,
            Map<String, String> metadata,
            String contentType
    ) throws IOException {
        checkNotClosed();
        put(key, value, ttl, metadata, contentType, false, UTF8);
    }

    private void put(
            String key,
            byte[] value,
            Duration ttl,
            Map<String, String> metadata,
            String contentType,
            boolean valueIsString,
            Charset stringEncoding
    ) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Map<String, String> safeMetadata = metadata == null ? Collections.emptyMap() : new HashMap<>(metadata);

        String hashed = hashKey(key);
        Path targetPath = pathForHash(hashed);
        Files.createDirectories(targetPath.getParent());

        long now = System.currentTimeMillis();
        Long ttlMs = ttl == null ? null : ttl.toMillis();
        Long expiresAt = ttlMs == null ? null : now + ttlMs;

        Header header = new Header();
        header.version = HEADER_VERSION;
        header.key = key;
        header.createdAtMillis = now;
        header.ttlMillis = ttlMs;
        header.expiresAtMillis = expiresAt;
        header.valueType = valueIsString ? "string" : "binary";
        header.encoding = valueIsString ? stringEncoding.name() : null;
        header.contentType = contentType;
        header.metadata = safeMetadata;

        byte[] payload = encodePayload(header, value);
        Path tempPath = tempPath(targetPath);

        try (LockHandle lock = lockExclusive(hashed)) {
            writeAtomically(tempPath, targetPath, payload);
        }

        if (accessJournal) {
            appendJournal(hashed);
        }
    }

    /**
     * Reads the value for {@code key} as raw bytes.
     *
     * @param key cache key
     * @return optional payload if present and not expired
     * @throws IOException if reading from disk fails
     */
    public Optional<byte[]> get(String key) throws IOException {
        checkNotClosed();
        EntryData data = readEntry(key);
        if (data == null) {
            return Optional.empty();
        }
        if (accessJournal) {
            appendJournal(data.hashedKey);
        }
        return Optional.of(data.value);
    }

    /**
     * Reads the value for {@code key} as a string if it was stored as text.
     *
     * @param key cache key
     * @return optional decoded string
     * @throws IOException if reading from disk fails
     */
    public Optional<String> getString(String key) throws IOException {
        checkNotClosed();
        EntryData data = readEntry(key);
        if (data == null) {
            return Optional.empty();
        }
        if (!"string".equals(data.header.valueType)) {
            return Optional.empty();
        }
        Charset encoding = data.header.encoding == null ? UTF8 : Charset.forName(data.header.encoding);
        if (accessJournal) {
            appendJournal(data.hashedKey);
        }
        return Optional.of(new String(data.value, encoding));
    }

    /**
     * Returns the full cache entry including metadata for {@code key}.
     *
     * @param key cache key
     * @return optional entry snapshot
     * @throws IOException if reading from disk fails
     */
    public Optional<CacheEntry> getEntry(String key) throws IOException {
        checkNotClosed();
        EntryData data = readEntry(key);
        if (data == null) {
            return Optional.empty();
        }
        if (accessJournal) {
            appendJournal(data.hashedKey);
        }
        return Optional.of(toCacheEntry(data));
    }

    /**
     * Removes {@code key} if present.
     *
     * @param key cache key
     * @return {@code true} if the entry existed
     * @throws IOException if deleting the entry fails
     */
    public boolean delete(String key) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "key");
        String hashed = hashKey(key);
        Path target = pathForHash(hashed);
        try (LockHandle lock = lockExclusive(hashed)) {
            if (Files.deleteIfExists(target)) {
                Path lockPath = lockPath(hashed);
                Files.deleteIfExists(lockPath);
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether {@code key} has a backing file (ignoring expiration).
     *
     * @param key cache key
     * @return {@code true} if the entry exists on disk
     * @throws IOException if file system operations fail
     */
    public boolean exists(String key) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "key");
        String hashed = hashKey(key);
        Path target = pathForHash(hashed);
        return Files.exists(target);
    }

    /**
     * Updates an entry's TTL and touch timestamp without modifying the payload.
     *
     * @param key cache key
     * @param newTtl optional new TTL
     * @return {@code true} if the entry exists
     * @throws IOException if rewriting the entry fails
     */
    public boolean touch(String key, Duration newTtl) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "key");
        String hashed = hashKey(key);
        Path target = pathForHash(hashed);
        EntryData data;
        try (LockHandle lock = lockExclusive(hashed)) {
            data = readEntryInternal(hashed, target);
            if (data == null) {
                return false;
            }
            long now = System.currentTimeMillis();
            Long ttlMs = newTtl == null ? data.header.ttlMillis : newTtl.toMillis();
            Long expiresAt = ttlMs == null ? null : now + ttlMs;
            data.header.createdAtMillis = now;
            data.header.ttlMillis = ttlMs;
            data.header.expiresAtMillis = expiresAt;

            byte[] payload = encodePayload(data.header, data.value);
            Path tempPath = tempPath(target);
            writeAtomically(tempPath, target, payload);
        }
        if (accessJournal && data != null) {
            appendJournal(data.hashedKey);
        }
        return true;
    }

    /**
     * Aggregates cache size and item count statistics.
     *
     * @return current cache statistics
     * @throws IOException if directory traversal fails
     */
    public Stats stats() throws IOException {
        checkNotClosed();
        long totalBytes = 0;
        long count = 0;
        if (!Files.exists(rootDir)) {
            return new Stats(0, 0);
        }
        try (var stream = Files.walk(rootDir)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (Files.isRegularFile(path) && isCacheFile(path)) {
                    totalBytes += Files.size(path);
                    count += 1;
                }
            }
        }
        return new Stats(count, totalBytes);
    }

    /**
     * Runs an eviction pass based on {@code maxBytes} and {@code maxAge}.
     *
     * @param maxBytes optional hard limit for retained bytes
     * @param maxAge optional maximum age for retained items
     * @return result describing the clean operation
     * @throws IOException if file operations fail
     */
    public CleanResult clean(Long maxBytes, Duration maxAge) throws IOException {
        checkNotClosed();
        long nowMicros = System.currentTimeMillis() * 1_000L;
        Long maxAgeSeconds = maxAge == null ? null : maxAge.toSeconds();

        Map<String, Long> accessMap = new HashMap<>();
        if (accessJournal && Files.exists(journalPath)) {
            List<String> lines = Files.readAllLines(journalPath, StandardCharsets.US_ASCII);
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    try {
                        accessMap.put(parts[0], Long.parseLong(parts[1]));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        List<EntryStats> entries = new ArrayList<>();
        long totalBytes = 0;
        if (Files.exists(rootDir)) {
            try (var stream = Files.walk(rootDir)) {
                var iterator = stream.iterator();
                while (iterator.hasNext()) {
                    Path path = iterator.next();
                    if (Files.isRegularFile(path) && isCacheFile(path)) {
                        long size = Files.size(path);
                        totalBytes += size;
                        String hashed = path.getFileName().toString();
                        long access = accessMap.getOrDefault(hashed, Files.getLastModifiedTime(path).toMillis() * 1_000L);
                        entries.add(new EntryStats(hashed, path, size, access));
                    }
                }
            }
        }

        entries.sort(Comparator.comparingLong(e -> e.lastAccessMicros));
        long removedBytes = 0;
        long removedItems = 0;
        long cutoffMicros = maxAgeSeconds == null ? Long.MIN_VALUE : (nowMicros - maxAgeSeconds * 1_000_000L);

        List<String> rewrittenJournalLines = new ArrayList<>();

        for (EntryStats entry : entries) {
            boolean delete = false;
            if (maxAgeSeconds != null && entry.lastAccessMicros < cutoffMicros) {
                delete = true;
            }
            if (!delete && maxBytes != null && (totalBytes - removedBytes) > maxBytes) {
                delete = true;
            }
            if (delete) {
                if (deleteEntryByPath(entry.hashedKey, entry.path)) {
                    removedBytes += entry.size;
                    removedItems += 1;
                }
            } else {
                rewrittenJournalLines.add(entry.hashedKey + "," + entry.lastAccessMicros);
            }
        }

        if (accessJournal) {
            Path tempJournal = journalPath.resolveSibling("access.journal.tmp");
            Files.createDirectories(tempJournal.getParent());
            Files.write(tempJournal, rewrittenJournalLines, StandardCharsets.US_ASCII);
            moveAtomically(tempJournal, journalPath);
        }

        return new CleanResult(removedItems, removedBytes, Math.max(totalBytes - removedBytes, 0));
    }

    /**
     * Deletes orphaned temporary files left by interrupted writes.
     *
     * @return count of files deleted
     * @throws IOException if the directory walk fails
     */
    public int cleanupTemporaryFiles() throws IOException {
        checkNotClosed();
        if (!Files.exists(rootDir)) {
            return 0;
        }
        var counter = new int[]{0};
        Files.walkFileTree(rootDir, new SimpleVisitor(path -> {
            if (Files.isRegularFile(path) && path.getFileName().toString().startsWith(".tmp-")) {
                try {
                    Files.deleteIfExists(path);
                    counter[0]++;
                } catch (IOException ignored) {
                }
            }
        }));
        return counter[0];
    }

    /**
     * Iterates lazily over all keys in the cache while filtering expired entries.
     *
     * @return stream of live keys
     * @throws IOException if directory traversal fails
     */
    public Stream<String> keys() throws IOException {
        checkNotClosed();
        List<Path> cachePaths = new ArrayList<>();
        if (Files.exists(rootDir)) {
            try (var stream = Files.walk(rootDir)) {
                stream.filter(Files::isRegularFile)
                      .filter(this::isCacheFile)
                      .forEach(cachePaths::add);
            }
        }
        long now = System.currentTimeMillis();
        return cachePaths.stream()
                .map(path -> {
                    try {
                        String hashed = path.getFileName().toString();
                        EntryData data = readEntryInternal(hashed, path);
                        if (data == null) {
                            return null;
                        }
                        // Filter out expired entries
                        if (data.header.expiresAtMillis != null && data.header.expiresAtMillis < now) {
                            return null;
                        }
                        return data.header.key;
                    } catch (IOException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull);
    }

    /**
     * Iterates lazily over cache entries while filtering expired data.
     *
     * @return stream of live cache entries
     * @throws IOException if directory traversal fails
     */
    public Stream<CacheEntry> entries() throws IOException {
        checkNotClosed();
        List<Path> cachePaths = new ArrayList<>();
        if (Files.exists(rootDir)) {
            try (var stream = Files.walk(rootDir)) {
                stream.filter(Files::isRegularFile)
                      .filter(this::isCacheFile)
                      .forEach(cachePaths::add);
            }
        }
        long now = System.currentTimeMillis();
        return cachePaths.stream()
                .map(path -> {
                    try {
                        String hashed = path.getFileName().toString();
                        EntryData data = readEntryInternal(hashed, path);
                        if (data == null) {
                            return null;
                        }
                        // Filter out expired entries
                        if (data.header.expiresAtMillis != null && data.header.expiresAtMillis < now) {
                            return null;
                        }
                        return toCacheEntry(data);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull);
    }

    /**
     * Retrieves many keys in a single pass.
     *
     * @param keys keys to read
     * @return map of key to value for keys that exist
     * @throws IOException if reading any entry fails
     */
    public Map<String, byte[]> getMulti(List<String> keys) throws IOException {
        checkNotClosed();
        Map<String, byte[]> result = new HashMap<>();
        for (String key : keys) {
            Optional<byte[]> value = get(key);
            value.ifPresent(bytes -> result.put(key, bytes));
        }
        return result;
    }

    /**
     * Stores multiple entries sequentially.
     *
     * @param entries key/value pairs to persist
     * @throws IOException if any put fails
     */
    public void putMulti(Map<String, byte[]> entries) throws IOException {
        checkNotClosed();
        for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Deletes multiple keys sequentially.
     *
     * @param keys keys to delete
     * @return number of keys deleted
     * @throws IOException if any delete fails
     */
    public int deleteMulti(List<String> keys) throws IOException {
        checkNotClosed();
        int count = 0;
        for (String key : keys) {
            if (delete(key)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Attempts to read an entry while waiting at most {@code lockTimeout} for locks.
     *
     * @param key cache key
     * @param lockTimeout maximum time to wait for the shared lock
     * @return optional value, empty on timeout or absence
     * @throws IOException if reading fails
     */
    public Optional<byte[]> tryGet(String key, Duration lockTimeout) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "key");
        String hashed = hashKey(key);
        Path target = pathForHash(hashed);
        try (LockHandle handle = tryLockShared(hashed, lockTimeout)) {
            if (handle.isNoop() && enableLocks) {
                return Optional.empty(); // timeout
            }
            EntryData data = readEntryInternal(hashed, target);
            if (data == null) {
                return Optional.empty();
            }
            if (!key.equals(data.header.key)) {
                return Optional.empty();
            }
            if (data.header.expiresAtMillis != null && data.header.expiresAtMillis < System.currentTimeMillis()) {
                delete(key);
                return Optional.empty();
            }
            if (accessJournal) {
                appendJournal(data.hashedKey);
            }
            return Optional.of(data.value);
        }
    }

    /**
     * Attempts to write an entry while waiting at most {@code lockTimeout} for locks.
     *
     * @param key cache key
     * @param value payload bytes
     * @param lockTimeout maximum time to wait for the exclusive lock
     * @return {@code true} if the write succeeded before the timeout
     * @throws IOException if writing fails
     */
    public boolean tryPut(String key, byte[] value, Duration lockTimeout) throws IOException {
        checkNotClosed();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        String hashed = hashKey(key);
        Path targetPath = pathForHash(hashed);
        Files.createDirectories(targetPath.getParent());

        long now = System.currentTimeMillis();

        Header header = new Header();
        header.version = HEADER_VERSION;
        header.key = key;
        header.createdAtMillis = now;
        header.ttlMillis = null;
        header.expiresAtMillis = null;
        header.valueType = "binary";
        header.encoding = null;
        header.contentType = null;
        header.metadata = Collections.emptyMap();

        byte[] payload = encodePayload(header, value);
        Path tempPath = tempPath(targetPath);

        try (LockHandle handle = tryLockExclusive(hashed, lockTimeout)) {
            if (handle.isNoop() && enableLocks) {
                return false; // timeout
            }
            writeAtomically(tempPath, targetPath, payload);
        }

        if (accessJournal) {
            appendJournal(hashed);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (cleanerScheduler != null) {
            cleanerScheduler.shutdown();
            try {
                if (!cleanerScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanerScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanerScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runBackgroundClean() {
        if (closed) {
            return; // Don't run cleaner if cache is closed
        }
        try {
            clean(autoCleanMaxBytes, autoCleanMaxAge);
        } catch (IllegalStateException e) {
            // Cache was closed during clean operation - this is expected during shutdown
        } catch (IOException e) {
            // Log error in production; for now swallow
        } catch (Exception e) {
            // Catch any other unexpected exceptions to prevent cleaner thread from dying
            // Log error in production
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("LocalCache is closed");
        }
    }

    private EntryData readEntry(String key) throws IOException {
        Objects.requireNonNull(key, "key");
        String hashed = hashKey(key);
        Path target = pathForHash(hashed);

        EntryData data;
        try (LockHandle lock = lockShared(hashed)) {
            data = readEntryInternal(hashed, target);
            if (data == null) {
                return null;
            }
            if (!key.equals(data.header.key)) {
                return null;
            }
            if (data.header.expiresAtMillis != null && data.header.expiresAtMillis < System.currentTimeMillis()) {
                // Entry expired - mark for deletion but need to release shared lock first
                data = null;
            }
        }

        // If entry was expired, delete it now (outside of lock scope)
        if (data == null && Files.exists(target)) {
            try (LockHandle lock = lockExclusive(hashed)) {
                // Re-read to ensure it's still expired
                EntryData recheck = readEntryInternal(hashed, target);
                if (recheck != null && recheck.header.expiresAtMillis != null &&
                    recheck.header.expiresAtMillis < System.currentTimeMillis()) {
                    Files.deleteIfExists(target);
                    Files.deleteIfExists(lockPath(hashed));
                }
            }
        }

        return data;
    }

    private EntryData readEntryInternal(String hashed, Path target) throws IOException {
        if (!Files.exists(target)) {
            return null;
        }
        try (InputStream in = new BufferedInputStream(Files.newInputStream(target))) {
            DataInputStream dataIn = new DataInputStream(in);
            int headerLength = dataIn.readInt();
            if (headerLength <= 0 || headerLength > 1_000_000) {
                throw new IOException("Corrupt header length: " + headerLength);
            }
            byte[] headerBytes = dataIn.readNBytes(headerLength);
            byte[] value = dataIn.readAllBytes();
            Header header = decodeHeader(headerBytes);
            if (header.version != HEADER_VERSION) {
                throw new IOException("Unsupported header version: " + header.version);
            }
            EntryData data = new EntryData();
            data.hashedKey = hashed;
            data.path = target;
            data.header = header;
            data.value = value;
            return data;
        }
    }

    private CacheEntry toCacheEntry(EntryData data) {
        Charset encoding = data.header.encoding == null ? UTF8 : Charset.forName(data.header.encoding);
        return new CacheEntry(
                data.header.key,
                data.value,
                "string".equals(data.header.valueType),
                encoding,
                data.header.ttlMillis,
                data.header.expiresAtMillis,
                data.header.createdAtMillis,
                data.header.metadata,
                data.header.contentType
        );
    }

    private boolean deleteEntryByPath(String hashed, Path path) throws IOException {
        try (LockHandle lock = lockExclusive(hashed)) {
            if (Files.deleteIfExists(path)) {
                Files.deleteIfExists(lockPath(hashed));
                return true;
            }
        }
        return false;
    }

    private boolean isCacheFile(Path path) {
        String name = path.getFileName().toString();
        if (name.equals(journalPath.getFileName().toString())) {
            return false;
        }
        return !name.endsWith(".lock") && !name.startsWith(".tmp-");
    }

    private void appendJournal(String hashed) throws IOException {
        long micros = System.currentTimeMillis() * 1_000L + ThreadLocalRandom.current().nextInt(1_000);
        Files.createDirectories(journalPath.getParent());
        String line = hashed + "," + micros + System.lineSeparator();
        Files.writeString(
                journalPath,
                line,
                StandardCharsets.US_ASCII,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    private void writeAtomically(Path tempPath, Path targetPath, byte[] payload) throws IOException {
        Files.createDirectories(tempPath.getParent());
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
            out.write(payload);
            out.flush();
            if (fsyncOnWrite) {
                out.flush();
            }
        }
        moveAtomically(tempPath, targetPath);
        if (fsyncOnWrite) {
            fsyncDirectory(targetPath.getParent());
        }
    }

    private void moveAtomically(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            // fallback without ATOMIC_MOVE
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void fsyncDirectory(Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (UnsupportedOperationException | IOException ex) {
            // Some filesystems do not support directory sync (e.g., on Windows). Swallow gracefully.
        }
    }

    private Path tempPath(Path targetPath) {
        String nonce = Long.toHexString(secureRandom.nextLong());
        return targetPath.getParent().resolve(".tmp-" + nonce);
    }

    private byte[] encodePayload(Header header, byte[] value) throws IOException {
        byte[] headerBytes = encodeHeader(header);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(headerBytes.length + value.length + 4);
        try (DataOutputStream dataOut = new DataOutputStream(buffer)) {
            dataOut.writeInt(headerBytes.length);
            dataOut.write(headerBytes);
            dataOut.write(value);
            dataOut.flush();
        }
        return buffer.toByteArray();
    }

    private byte[] encodeHeader(Header header) {
        StringBuilder sb = new StringBuilder();
        sb.append("version=").append(header.version).append('\n');
        sb.append("key=").append(encodeString(header.key)).append('\n');
        sb.append("created_at_ms=").append(header.createdAtMillis).append('\n');
        sb.append("ttl_ms=").append(header.ttlMillis == null ? -1 : header.ttlMillis).append('\n');
        sb.append("expires_at_ms=").append(header.expiresAtMillis == null ? -1 : header.expiresAtMillis).append('\n');
        sb.append("value_type=").append(header.valueType == null ? "" : header.valueType).append('\n');
        sb.append("encoding=").append(header.encoding == null ? "" : header.encoding).append('\n');
        sb.append("content_type=").append(header.contentType == null ? "" : escapeLine(header.contentType)).append('\n');

        if (header.metadata != null && !header.metadata.isEmpty()) {
            for (Map.Entry<String, String> entry : header.metadata.entrySet()) {
                String metaKey = encodeString(entry.getKey());
                String metaValue = encodeString(entry.getValue());
                sb.append("meta=").append(metaKey).append(':').append(metaValue).append('\n');
            }
        }

        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private Header decodeHeader(byte[] headerBytes) throws IOException {
        Header header = new Header();
        String headerStr = new String(headerBytes, StandardCharsets.US_ASCII);
        String[] lines = headerStr.split("\n");
        Map<String, String> metadata = new HashMap<>();

        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            int idx = line.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            String key = line.substring(0, idx);
            String value = line.substring(idx + 1);
            switch (key) {
                case "version":
                    header.version = Integer.parseInt(value.trim());
                    break;
                case "key":
                    header.key = decodeString(value);
                    break;
                case "created_at_ms":
                    header.createdAtMillis = Long.parseLong(value.trim());
                    break;
                case "ttl_ms":
                    long ttl = Long.parseLong(value.trim());
                    header.ttlMillis = ttl < 0 ? null : ttl;
                    break;
                case "expires_at_ms":
                    long expires = Long.parseLong(value.trim());
                    header.expiresAtMillis = expires < 0 ? null : expires;
                    break;
                case "value_type":
                    header.valueType = value.isEmpty() ? null : value;
                    break;
                case "encoding":
                    header.encoding = value.isEmpty() ? null : value;
                    break;
                case "content_type":
                    header.contentType = value.isEmpty() ? null : unescapeLine(value);
                    break;
                case "meta":
                    int sep = value.indexOf(':');
                    if (sep > 0) {
                        String metaKey = decodeString(value.substring(0, sep));
                        String metaValue = decodeString(value.substring(sep + 1));
                        metadata.put(metaKey, metaValue);
                    }
                    break;
                default:
                    break;
            }
        }
        header.metadata = metadata;
        if (header.version == 0) {
            header.version = HEADER_VERSION;
        }
        return header;
    }

    private static String escapeLine(String input) {
        return input.replace("\\", "\\\\").replace("\n", "\\n");
    }

    private static String unescapeLine(String input) {
        StringBuilder sb = new StringBuilder();
        boolean escaping = false;
        for (char c : input.toCharArray()) {
            if (escaping) {
                if (c == 'n') {
                    sb.append('\n');
                } else {
                    sb.append(c);
                }
                escaping = false;
            } else if (c == '\\') {
                escaping = true;
            } else {
                sb.append(c);
            }
        }
        if (escaping) {
            sb.append('\\');
        }
        return sb.toString();
    }

    private static String encodeString(String input) {
        return B64_URL.encodeToString(input.getBytes(UTF8));
    }

    private static String decodeString(String input) {
        byte[] decoded = B64_URL_DEC.decode(input);
        return new String(decoded, UTF8);
    }

    private String hashKey(String key) {
        MessageDigest md = digest.get();
        md.reset();
        byte[] digestBytes = md.digest(key.getBytes(UTF8));
        return toHex(digestBytes);
    }

    private String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }

    private Path pathForHash(String hashed) {
        Path path = rootDir;
        int index = 0;
        for (int shard : shardSizes) {
            if (index + shard > hashed.length()) {
                break;
            }
            path = path.resolve(hashed.substring(index, index + shard));
            index += shard;
        }
        return path.resolve(hashed);
    }

    private Path lockPath(String hashed) {
        return pathForHash(hashed).resolveSibling(hashed + ".lock");
    }

    private LockHandle lockExclusive(String hashed) throws IOException {
        if (!enableLocks) {
            return LockHandle.noop();
        }
        Path lockFile = lockPath(hashed);
        Files.createDirectories(lockFile.getParent());
        FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        FileLock lock = channel.lock(0L, Long.MAX_VALUE, false);
        return new LockHandle(channel, lock);
    }

    private LockHandle lockShared(String hashed) throws IOException {
        if (!enableLocks) {
            return LockHandle.noop();
        }
        Path lockFile = lockPath(hashed);
        Files.createDirectories(lockFile.getParent());
        FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        FileLock lock = channel.lock(0L, Long.MAX_VALUE, true);
        return new LockHandle(channel, lock);
    }

    private LockHandle tryLockExclusive(String hashed, Duration timeout) throws IOException {
        if (!enableLocks) {
            return LockHandle.noop();
        }
        Path lockFile = lockPath(hashed);
        Files.createDirectories(lockFile.getParent());
        FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);

        long endTime = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < endTime) {
            try {
                FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, false);
                if (lock != null) {
                    return new LockHandle(channel, lock);
                }
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.close();
                throw new IOException("Lock acquisition interrupted", e);
            }
        }
        channel.close();
        return LockHandle.noop(); // timeout
    }

    private LockHandle tryLockShared(String hashed, Duration timeout) throws IOException {
        if (!enableLocks) {
            return LockHandle.noop();
        }
        Path lockFile = lockPath(hashed);
        Files.createDirectories(lockFile.getParent());
        FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        long endTime = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < endTime) {
            try {
                FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, true);
                if (lock != null) {
                    return new LockHandle(channel, lock);
                }
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.close();
                throw new IOException("Lock acquisition interrupted", e);
            }
        }
        channel.close();
        return LockHandle.noop(); // timeout
    }

    private static MessageDigest createDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalArgumentException("Unsupported hash algorithm: " + algorithm, ex);
        }
    }

    private static final class Header {
        int version;
        String key;
        Long ttlMillis;
        Long expiresAtMillis;
        long createdAtMillis;
        String valueType;
        String encoding;
        String contentType;
        Map<String, String> metadata = Collections.emptyMap();
    }

    private static final class EntryData {
        String hashedKey;
        Path path;
        Header header;
        byte[] value;
    }

    private static final class EntryStats {
        final String hashedKey;
        final Path path;
        final long size;
        final long lastAccessMicros;

        EntryStats(String hashedKey, Path path, long size, long lastAccessMicros) {
            this.hashedKey = hashedKey;
            this.path = path;
            this.size = size;
            this.lastAccessMicros = lastAccessMicros;
        }
    }

    private static final class SimpleVisitor extends java.nio.file.SimpleFileVisitor<Path> {
        private final Consumer<Path> consumer;

        SimpleVisitor(Consumer<Path> consumer) {
            this.consumer = consumer;
        }

        @Override
        public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
            consumer.accept(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }

    private static final class LockHandle implements AutoCloseable {
        private static final LockHandle NOOP = new LockHandle(null, null);

        private final FileChannel channel;
        private final FileLock lock;

        private LockHandle(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }

        static LockHandle noop() {
            return NOOP;
        }

        boolean isNoop() {
            return this == NOOP;
        }

        @Override
        public void close() throws IOException {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    /**
     * Builder for configuring {@link LocalCache} instances.
     */
    public static final class Builder {
        private final Path rootDir;
        private String hashAlgorithm = "SHA-256";
        private int[] shardSizes = new int[]{2, 2};
        private boolean fsyncOnWrite = true;
        private boolean enableLocks = true;
        private boolean accessJournal = true;
        private Duration cleanInterval = null;
        private Long autoCleanMaxBytes = null;
        private Duration autoCleanMaxAge = null;

        private Builder(Path rootDir) {
            this.rootDir = rootDir;
        }

        /**
         * Sets the hash algorithm used to derive shard paths.
         *
         * @param algorithm message-digest algorithm (e.g. {@code SHA-256})
         * @return this builder
         * @throws NullPointerException if {@code algorithm} is {@code null}
         */
        public Builder hashAlgorithm(String algorithm) {
            this.hashAlgorithm = Objects.requireNonNull(algorithm, "algorithm");
            return this;
        }

        /**
         * Configures shard directory sizes per path segment.
         *
         * @param shards number of characters used per shard level
         * @return this builder
         * @throws IllegalArgumentException if {@code shards} is empty
         */
        public Builder shardSizes(int... shards) {
            if (shards == null || shards.length == 0) {
                throw new IllegalArgumentException("Shard sizes required");
            }
            this.shardSizes = shards.clone();
            return this;
        }

        /**
         * Enables durable writes by fsyncing file data and parent directories.
         *
         * @param fsync whether to fsync after writes
         * @return this builder
         */
        public Builder fsyncOnWrite(boolean fsync) {
            this.fsyncOnWrite = fsync;
            return this;
        }

        /**
         * Enables file-lock based mutual exclusion.
         *
         * @param enable {@code true} to acquire OS locks for operations
         * @return this builder
         */
        public Builder enableLocks(boolean enable) {
            this.enableLocks = enable;
            return this;
        }

        /**
         * Enables append-only journal bookkeeping used by the cleaner.
         *
         * @param enable {@code true} to record access times
         * @return this builder
         */
        public Builder accessJournal(boolean enable) {
            this.accessJournal = enable;
            return this;
        }

        /**
         * Enable automatic background cleaning at the specified interval.
         * @param interval How often to run the cleaner (e.g., Duration.ofMinutes(5))
         * @return this builder
         * @throws IllegalArgumentException if interval is zero, negative, or rounds to 0 milliseconds
         */
        public Builder cleanInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("cleanInterval must be positive, got: " + interval);
            }
            // Validate that interval is at least 1 millisecond when converted
            // scheduleAtFixedRate requires period > 0 in the target TimeUnit
            long millis = interval.toMillis();
            if (millis == 0) {
                throw new IllegalArgumentException(
                    "cleanInterval must be at least 1 millisecond, got: " + interval +
                    " (rounds to 0ms)");
            }
            this.cleanInterval = interval;
            return this;
        }

        /**
         * Set maximum cache size in bytes for automatic cleaning.
         * @param maxBytes Maximum total size; older entries evicted when exceeded
         * @return this builder
         * @throws IllegalArgumentException if maxBytes is negative
         */
        public Builder autoCleanMaxBytes(Long maxBytes) {
            if (maxBytes != null && maxBytes < 0) {
                throw new IllegalArgumentException("autoCleanMaxBytes must be non-negative, got: " + maxBytes);
            }
            this.autoCleanMaxBytes = maxBytes;
            return this;
        }

        /**
         * Set maximum age for entries during automatic cleaning.
         * @param maxAge Maximum age; entries older than this are evicted
         * @return this builder
         * @throws IllegalArgumentException if maxAge is zero or negative
         */
        public Builder autoCleanMaxAge(Duration maxAge) {
            if (maxAge != null && (maxAge.isZero() || maxAge.isNegative())) {
                throw new IllegalArgumentException("autoCleanMaxAge must be positive, got: " + maxAge);
            }
            this.autoCleanMaxAge = maxAge;
            return this;
        }

        /**
         * Builds a {@link LocalCache} instance with the configured options.
         *
         * @return new cache instance
         * @throws IOException if initialization fails
         */
        public LocalCache build() throws IOException {
            return new LocalCache(this);
        }
    }
}
