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

import io.sixhours.memcached.cache.MemcachedCacheProperties.HashStrategy;
import io.sixhours.memcached.cache.MemcachedCacheProperties.Provider;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static io.sixhours.memcached.cache.MemcachedCacheProperties.Protocol;
import static java.util.Collections.singletonList;

/**
 * Default cache configuration values.
 *
 * @author Igor Bolic
 */
public final class Default {

    public static final List<InetSocketAddress> SERVERS = singletonList(new InetSocketAddress("localhost", 11211));

    public static final Provider PROVIDER = Provider.STATIC;

    public static final int EXPIRATION = 0;

    public static final String PREFIX = "memcached:spring-boot";

    public static final String NAMESPACE = "namespace";

    public static final Protocol PROTOCOL = Protocol.TEXT;

    public static final long OPERATION_TIMEOUT = 2500L;

    public static final Duration SERVERS_REFRESH_INTERVAL = Duration.ofMinutes(1);

    public static final HashStrategy HASH_STRATEGY = HashStrategy.STANDARD;

    private Default() {
        throw new AssertionError("Suppress default constructor");
    }
}
