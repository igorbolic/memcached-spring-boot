/*
 * Copyright 2016-2023 Sixhours
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.sixhours.memcached.cache;

import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache implementation on top of Memcached.
 *
 * @author Igor Bolic
 */
public class MemcachedCache extends AbstractValueAdaptingCache {

    private static final String KEY_DELIMITER = ":";

    private final IMemcachedClient memcachedClient;
    private final MemcacheCacheMetadata memcacheCacheMetadata;

    private final Lock lock = new ReentrantLock();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    /**
     * Create an {@code MemcachedCache} with the given settings.
     *
     * @param name            Cache name
     * @param memcachedClient {@link IMemcachedClient}
     * @param expiration      Cache expiration in seconds
     * @param prefix          Cache key prefix
     * @param namespace       Cache invalidation namespace key
     */
    public MemcachedCache(String name, IMemcachedClient memcachedClient, int expiration, String prefix, String namespace) {
        super(true);
        this.memcachedClient = memcachedClient;
        this.memcacheCacheMetadata = new MemcacheCacheMetadata(name, expiration, prefix, namespace);
    }

    @Override
    protected Object lookup(Object key) {
        return trackHitsMisses(memcachedClient.get(memcachedKey(key)));
    }

    @Override
    public String getName() {
        return this.memcacheCacheMetadata.name();
    }

    @Override
    public Object getNativeCache() {
        return this.memcachedClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = lookup(key);
        if (value != null) {
            return (T) fromStoreValue(value);
        }

        lock.lock();
        try {
            value = lookup(key);
            if (value != null) {
                return (T) fromStoreValue(value);
            } else {
                return (T) fromStoreValue(loadValue(key, valueLoader));
            }
        } finally {
            lock.unlock();
        }
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, Object value) {
        this.memcachedClient.set(memcachedKey(key), this.memcacheCacheMetadata.expiration(), toStoreValue(value));
        this.memcachedClient.touch(this.memcacheCacheMetadata.namespaceKey(), this.memcacheCacheMetadata.expiration());
        puts.incrementAndGet();
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object existingValue = lookup(key);
        if (existingValue == null) {
            put(key, value);
            return toValueWrapper(value);
        }

        return toValueWrapper(existingValue);
    }

    @Override
    public void evict(Object key) {
        this.memcachedClient.delete(memcachedKey(key));
        this.evictions.incrementAndGet();
    }

    @Override
    public void clear() {
        this.memcachedClient.incr(this.memcacheCacheMetadata.namespaceKey(), 1);
    }

    public long hits() {
        return hits.get();
    }

    public long misses() {
        return misses.get();
    }

    public long puts() {
        return puts.get();
    }

    public long evictions() {
        return evictions.get();
    }

    /**
     * Tracks number of hits and misses per {@code MemcachedCache} instance.
     *
     * @param value Value returned from the underlying cache store.
     * @return The value
     */
    private Object trackHitsMisses(Object value) {
        if (value != null) {
            hits.incrementAndGet();
        } else {
            misses.incrementAndGet();
        }
        return value;
    }

    /**
     * Gets Memcached key value.
     * <p>
     * Prepends cache prefix and namespace value to the given {@code key}. All whitespace characters will be stripped from
     * the {@code key} value, for Memcached key to be valid.
     *
     * @param key The key
     * @return Memcached key
     */
    private String memcachedKey(Object key) {
        return memcacheCacheMetadata.keyPrefix() +
                namespaceValue() +
                KEY_DELIMITER +
                String.valueOf(key).replaceAll("\\s", "");
    }

    /**
     * Gets namespace value from the cache. The value is used for invalidation of the cache data
     * by incrementing current namespace value by 1.
     *
     * @return Namespace integer value returned as {@code String}
     */
    private String namespaceValue() {
        String value = (String) this.memcachedClient.get(this.memcacheCacheMetadata.namespaceKey());
        if (value == null) {
            value = String.valueOf(System.currentTimeMillis());
            this.memcachedClient.set(this.memcacheCacheMetadata.namespaceKey(),
                    this.memcacheCacheMetadata.expiration(), value);
        }

        return value;
    }

    static class MemcacheCacheMetadata {
        private final String name;
        private final int expiration;
        private final String keyPrefix;
        private final String namespaceKey;

        public MemcacheCacheMetadata(String name, int expiration, String cachePrefix, String namespace) {
            this.name = name;
            this.expiration = expiration;

            StringBuilder sb = new StringBuilder(cachePrefix)
                    .append(KEY_DELIMITER)
                    .append(name)
                    .append(KEY_DELIMITER);

            this.keyPrefix = sb.toString();
            this.namespaceKey = sb.append(namespace).toString();
        }

        public String name() {
            return name;
        }

        /**
         * Expiration times are specified in unsigned integer seconds. They can be set from 0, meaning "never expire",
         * to 30 days (60*60*24*30). Any time higher than 30 days is interpreted as a unix timestamp date. If you
         * want to expire an object on january 1st of next year, this is how you do that.
         * <p>
         * The unix timestamp is a way to track time as a running total of seconds. This count starts at the Unix
         * Epoch on January 1st, 1970 at UTC. Therefore, the unix time stamp is merely the number of seconds between
         * a particular date and the Unix Epoch.
         *
         * @return expiration time as seconds (up to 30 days) or as UNIX timestamp epoch seconds (greater than 30 days).
         * @see <a href="https://github.com/memcached/memcached/wiki/Programming#expiration">Memcached Expiration</a>
         * @see <a href="https://www.unixtimestamp.com/">Unix timestamp</a>
         */
        public int expiration() {
            // If the expiration is greater than 30 days: expiration time = UNIX timestamp + expiration
            if (this.expiration > Duration.ofDays(30).getSeconds()) {
                return (int) Instant.now().plusSeconds(expiration).getEpochSecond();
            }
            return expiration;
        }

        public String keyPrefix() {
            return keyPrefix;
        }

        public String namespaceKey() {
            return namespaceKey;
        }
    }
}
