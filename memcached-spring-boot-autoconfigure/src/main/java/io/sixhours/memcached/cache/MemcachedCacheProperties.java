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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Configuration properties for Memcached cache.
 *
 * @author Igor Bolic
 */
@ConfigurationProperties(prefix = "memcached.cache")
public class MemcachedCacheProperties {

    /**
     * Comma-separated list of hostname:port for memcached servers. The default hostname:port is 'localhost:11211'.
     */
    private List<InetSocketAddress> servers = Default.SERVERS;

    /**
     * Comma-separated list of cache names to disable
     */
    private Set<String> disabledCacheNames = new HashSet<>();

    /**
     * Comma-separated list of cache names for which metrics will be collected
     */
    private List<String> metricsCacheNames = new ArrayList<>();

    /**
     * Memcached server provider. Use 'appengine' if running on Google Cloud Platform;
     * Use 'aws' for Amazon ElastiCache with node auto discovery. Defaults to 'static'.
     */
    private Provider provider = Default.PROVIDER;

    /**
     * Cache expiration in seconds. The default is 0s, meaning the cache will never expire.
     */
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration expiration = Duration.ofSeconds(Default.EXPIRATION);

    /**
     * Cache expiration per cache. The map contains cache name as the key and expiration as the value.
     * The expiration value in the map will override default {@code expiration} for the cache named as key.
     */
    private Map<String, Duration> expirationPerCache = new HashMap<>();

    /**
     * Cached object key prefix. The default is 'memcached:spring-boot'.
     */
    private String prefix = Default.PREFIX;

    /**
     * Memcached client protocol. Supports two main protocols: the classic text (ascii), and the newer binary protocol.
     * The default is 'text' protocol.
     */
    private Protocol protocol = Default.PROTOCOL;

    /**
     * Memcached client operation timeout in milliseconds. The default is 2500 milliseconds.
     */
    private Duration operationTimeout = Duration.ofMillis(Default.OPERATION_TIMEOUT);

    /**
     * Amazon ElastiCache configuration polling interval in milliseconds that refreshes the list of cache node hostnames and IP
     * addresses.  The default is 60000 milliseconds
     */
    private Duration serversRefreshInterval = Default.SERVERS_REFRESH_INTERVAL;

    /**
     * Memcached client hash strategy for distribution of data between servers. Supports 'standard' (array based :
     * "hash(key) mod server_count"), 'libmemcached' (consistent hash), 'ketama' (consistent hash),
     * 'php' (make easier to share data with PHP based clients), 'election', 'roundrobin', 'random'.
     * The default is 'standard'.
     */
    private HashStrategy hashStrategy = Default.HASH_STRATEGY;

    public List<InetSocketAddress> getServers() {
        return servers;
    }

    /**
     * Populate server list from comma-separated list of hostname:port strings.
     *
     * @param value Comma-separated list
     */
    public void setServers(String value) {
        if (value == null || StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("Server list is empty");
        }
        this.servers = Stream.of(value.split("\\s*,\\s*"))
                .map(SocketAddress::new)
                .map(SocketAddress::value)
                .collect(Collectors.toList());
    }

    public Set<String> getDisabledCacheNames() {
        return disabledCacheNames;
    }

    public void setDisabledCacheNames(Set<String> disabledCacheNames) {
        this.disabledCacheNames = disabledCacheNames;
    }

    public List<String> getMetricsCacheNames() {
        return metricsCacheNames;
    }

    public void setMetricsCacheNames(List<String> metricsCacheNames) {
        this.metricsCacheNames = metricsCacheNames;
    }

    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    public Duration getExpiration() {
        return expiration;
    }

    public void setExpiration(Duration expiration) {
        validateExpiration(expiration);
        this.expiration = expiration;
    }

    public void setExpirationPerCache(Map<String, String> expirationPerCache) {
        if (expirationPerCache != null) {
            expirationPerCache.forEach((cacheName, cacheExpiration) -> {
                Duration exp = DurationStyle.detect(cacheExpiration).parse(cacheExpiration, ChronoUnit.SECONDS);
                validateExpiration(exp);
                this.expirationPerCache.put(cacheName, exp);
            });
        }
    }

    public Map<String, Duration> getExpirationPerCache() {
        return expirationPerCache;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public Duration getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(Duration operationTimeout) {
        if (operationTimeout == null || Duration.ZERO.compareTo(operationTimeout) >= 0) {
            throw new IllegalArgumentException("Operation timeout must be greater then zero");
        }
        this.operationTimeout = operationTimeout;
    }

    public Duration getServersRefreshInterval() {
        return serversRefreshInterval;
    }

    public void setServersRefreshInterval(Duration serversRefreshInterval) {
        if (serversRefreshInterval == null || Duration.ZERO.compareTo(serversRefreshInterval) >= 0) {
            throw new IllegalArgumentException("Servers refresh interval must be greater then zero");
        }
        this.serversRefreshInterval = serversRefreshInterval;
    }

    private void validateExpiration(Duration expiration) {
        if (expiration == null || expiration.isNegative()) {
            throw new IllegalArgumentException("Invalid expiration. Duration must be greater than or equal to 0 (zero) seconds.");
        }
    }

    public HashStrategy getHashStrategy() {
        return hashStrategy;
    }

    public void setHashStrategy(HashStrategy hashStrategy) {
        this.hashStrategy = hashStrategy;
    }

    public enum Protocol {
        TEXT, BINARY
    }

    public enum Provider {
        STATIC, APPENGINE, AWS
    }

    public enum HashStrategy {
        STANDARD, LIBMEMCACHED, KETAMA, PHP, ELECTION, ROUNDROBIN, RANDOM
    }
}
