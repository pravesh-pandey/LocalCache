package com.localcache;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a cache entry with metadata.
 */
public final class CacheEntry {

    private final String key;
    private final byte[] value;
    private final boolean valueIsString;
    private final Charset encoding;
    private final Long ttlMillis;
    private final Long expiresAtMillis;
    private final long createdAtMillis;
    private final Map<String, String> metadata;
    private final String contentType;

    CacheEntry(
            String key,
            byte[] value,
            boolean valueIsString,
            Charset encoding,
            Long ttlMillis,
            Long expiresAtMillis,
            long createdAtMillis,
            Map<String, String> metadata,
            String contentType
    ) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value.clone();
        this.valueIsString = valueIsString;
        this.encoding = encoding == null ? StandardCharsets.UTF_8 : encoding;
        this.ttlMillis = ttlMillis;
        this.expiresAtMillis = expiresAtMillis;
        this.createdAtMillis = createdAtMillis;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
        this.contentType = contentType;
    }

    /**
     * Returns the logical cache key for this entry.
     *
     * @return cache key string
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns a defensive copy of the stored value bytes.
     *
     * @return value payload
     */
    public byte[] getValue() {
        return value.clone();
    }

    /**
     * Indicates whether the payload originated from a string value.
     *
     * @return {@code true} when the value was stored via {@link LocalCache#putString(String, String)}
     */
    public boolean isValueString() {
        return valueIsString;
    }

    /**
     * Returns the payload decoded as a string using the stored character set.
     *
     * @return decoded string value
     * @throws IllegalStateException if the value is binary data
     */
    public String getValueAsString() {
        if (!valueIsString) {
            throw new IllegalStateException("Value stored as binary; cannot decode as string.");
        }
        return new String(value, encoding);
    }

    /**
     * Returns the charset used to encode string payloads.
     *
     * @return payload encoding (defaults to UTF-8)
     */
    public Charset getEncoding() {
        return encoding;
    }

    /**
     * Returns the configured time-to-live for the entry in milliseconds.
     *
     * @return TTL in milliseconds or {@code null} when not set
     */
    public Long getTtlMillis() {
        return ttlMillis;
    }

    /**
     * Returns the absolute expiration timestamp in milliseconds.
     *
     * @return epoch millis when the entry expires or {@code null} if it does not expire automatically
     */
    public Long getExpiresAtMillis() {
        return expiresAtMillis;
    }

    /**
     * Returns when the entry was written to disk.
     *
     * @return creation time in epoch milliseconds
     */
    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    /**
     * Returns immutable metadata that was stored alongside the entry.
     *
     * @return read-only metadata map (never {@code null})
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Returns the declared content type for the payload.
     *
     * @return MIME type string or {@code null} when unset
     */
    public String getContentType() {
        return contentType;
    }
}
