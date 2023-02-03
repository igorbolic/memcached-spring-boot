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

import net.spy.memcached.MemcachedClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * {@code SpyMemcached} memcached client implementation.
 *
 * @author Sasa Bolic
 */
public class SpyMemcachedClient implements IMemcachedClient {
    private static final Log log = LogFactory.getLog(SpyMemcachedClient.class);

    private final MemcachedClient memcachedClient;

    public SpyMemcachedClient(MemcachedClient memcachedClient) {
        log.info("SpyMemcached client initialized.");
        this.memcachedClient = memcachedClient;
    }

    @Override
    public MemcachedClient nativeClient() {
        return this.memcachedClient;
    }

    @Override
    public Object get(String key) {
        return this.memcachedClient.get(key);
    }

    @Override
    public void set(String key, int exp, Object value) {
        this.memcachedClient.set(key, exp, value);
    }

    @Override
    public void touch(String key, int exp) {
        this.memcachedClient.touch(key, exp);
    }

    @Override
    public void delete(String key) {
        this.memcachedClient.delete(key);
    }

    @Override
    public void flush() {
        this.memcachedClient.flush();
    }

    @Override
    public long incr(String key, int by) {
        return this.memcachedClient.incr(key, by);
    }

    @Override
    public void shutdown() {
        this.memcachedClient.shutdown();
    }
}
