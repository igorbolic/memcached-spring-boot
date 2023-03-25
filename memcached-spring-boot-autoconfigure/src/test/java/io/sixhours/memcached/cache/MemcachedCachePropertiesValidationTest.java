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

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MemcachedCachePropertiesValidationTest {

    private MemcachedCacheProperties properties;

    @Before
    public void setUp() {
        properties = new MemcachedCacheProperties();
    }

    @Test
    public void whenSetServersThenValidationOk() {
        properties.setServers("example1.com:1122");

        assertThat(properties.getServers()).hasSize(1);
        assertThat(properties.getServers().get(0)).isNotNull();
        assertThat(properties.getServers().get(0).getHostName()).isNotNull();
        assertThat(properties.getServers().get(0).getHostName()).isEqualTo("example1.com");
        assertThat(properties.getServers().get(0).getPort()).isEqualTo(1122);
    }

    @Test
    public void whenSetNullServersThenValidationFails() {
        assertThatThrownBy(() -> properties.setServers(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Server list is empty");
    }

    @Test
    public void whenSetEmptyServersThenValidationFails() {
        assertThatThrownBy(() -> properties.setServers(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Server list is empty");
    }

    @Test
    public void whenSetBlankServersThenValidationFails() {
        assertThatThrownBy(() -> properties.setServers(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid server value. It cannot be empty");
    }

    @Test
    public void whenSetDisabledCacheNamesThenValidationOk() {
        Set<String> names = new HashSet<>();
        names.add("cache-1");
        properties.setDisabledCacheNames(names);

        assertThat(properties.getDisabledCacheNames()).hasSize(1);
        assertThat(properties.getDisabledCacheNames()).containsExactly("cache-1");
    }

    @Test
    public void whenSetEmptyDisabledCacheNamesThenValidationOk() {
        properties.setDisabledCacheNames(new HashSet<>());

        assertThat(properties.getDisabledCacheNames()).isEmpty();
    }

    @Test
    public void whenSetMetricsCacheNamesThenValidationOk() {
        List<String> names = new ArrayList<>();
        names.add("cache-1");
        properties.setMetricsCacheNames(names);

        assertThat(properties.getMetricsCacheNames()).hasSize(1);
        assertThat(properties.getMetricsCacheNames()).containsExactly("cache-1");
    }

    @Test
    public void whenSetEmptyMetricsCacheNamesThenValidationOk() {
        properties.setMetricsCacheNames(new ArrayList<>());

        assertThat(properties.getMetricsCacheNames()).isEmpty();
    }

    @Test
    public void whenSetNullMetricsCacheNamesThenValidationOk() {
        properties.setMetricsCacheNames(null);

        assertThat(properties.getMetricsCacheNames()).isNull();
    }

    @Test
    public void whenSetProviderThenValidationOk() {
        properties.setProvider(MemcachedCacheProperties.Provider.STATIC);

        assertThat(properties.getProvider()).isEqualTo(MemcachedCacheProperties.Provider.STATIC);
    }

    @Test
    public void whenSetNullProviderThenValidationOk() {
        properties.setProvider(null);

        assertThat(properties.getProvider()).isNull();
    }

    @Test
    public void whenSetZeroExpirationThenValidationOk() {
        properties.setExpiration(Duration.ZERO);

        assertThat(properties.getExpiration()).isEqualTo(Duration.ZERO);
    }

    @Test
    public void whenSetPositiveExpirationThenValidationOk() {
        properties.setExpiration(Duration.ofDays(40));

        assertThat(properties.getExpiration()).isEqualTo(Duration.ofDays(40));
    }

    @Test
    public void whenSetNullExpirationThenValidationFails() {
        assertThatThrownBy(() -> properties.setExpiration(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid expiration. Duration must be greater than or equal to 0 (zero) seconds.");
    }

    @Test
    public void whenSetNegativeExpirationThenValidationFails() {
        assertThatThrownBy(() -> properties.setExpiration(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid expiration. Duration must be greater than or equal to 0 (zero) seconds.");
    }

    @Test
    public void whenSetExpirationPerCacheThenValidationOk() {
        Map<String, String> perCache = new HashMap<>();
        perCache.put("cache-1", "1d");
        perCache.put("cache-2", "15h");
        perCache.put("cache-3", "30d");
        perCache.put("cache-4", "40d");

        properties.setExpirationPerCache(perCache);

        assertThat(properties.getExpirationPerCache()).hasSize(4);
        assertThat(properties.getExpirationPerCache().get("cache-1")).isEqualTo(Duration.ofDays(1));
        assertThat(properties.getExpirationPerCache().get("cache-2")).isEqualTo(Duration.ofHours(15));
        assertThat(properties.getExpirationPerCache().get("cache-3")).isEqualTo(Duration.ofDays(30));
        assertThat(properties.getExpirationPerCache().get("cache-4")).isEqualTo(Duration.ofDays(40));
    }

    @Test
    public void whenSetNullExpirationPerCacheThenValidationOk() {
        properties.setExpirationPerCache(null);

        assertThat(properties.getExpirationPerCache()).isEmpty();
    }

    @Test
    public void whenSetNegativeExpirationPerCacheThenValidationFails() {
        Map<String, String> perCache = new HashMap<>();
        perCache.put("cache-1", "-1d");
        perCache.put("cache-4", "40d");

        assertThatThrownBy(() -> properties.setExpirationPerCache(perCache))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid expiration. Duration must be greater than or equal to 0 (zero) seconds.");
    }

    @Test
    public void whenSetPrefixThenValidationOk() {
        properties.setPrefix("my-cache");

        assertThat(properties.getPrefix()).isEqualTo("my-cache");
    }

    @Test
    public void whenSetNullPrefixThenValidationOk() {
        properties.setPrefix(null);

        assertThat(properties.getPrefix()).isNull();
    }

    @Test
    public void whenSetProtocolThenValidationOk() {
        properties.setProtocol(MemcachedCacheProperties.Protocol.TEXT);

        assertThat(properties.getProtocol()).isEqualTo(MemcachedCacheProperties.Protocol.TEXT);
    }

    @Test
    public void whenSetNullProtocolThenValidationOk() {
        properties.setProtocol(null);

        assertThat(properties.getProtocol()).isNull();
    }

    @Test
    public void whenSetOperationTimeoutThenValidationOk() {
        properties.setOperationTimeout(Duration.ofHours(4));

        assertThat(properties.getOperationTimeout()).isEqualTo(Duration.ofHours(4));
    }

    @Test
    public void whenSetNullOperationTimeoutThenValidationFails() {
        assertThatThrownBy(() -> properties.setOperationTimeout(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Operation timeout must be greater then zero");
    }

    @Test
    public void whenSetZeroOperationTimeoutThenValidationFails() {
        assertThatThrownBy(() -> properties.setOperationTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Operation timeout must be greater then zero");
    }

    @Test
    public void whenSetNegativeOperationTimeoutThenValidationFails() {
        assertThatThrownBy(() -> properties.setOperationTimeout(Duration.ofDays(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Operation timeout must be greater then zero");
    }

    @Test
    public void whenSetServersRefreshIntervalThenValidationOk() {
        properties.setServersRefreshInterval(Duration.ofHours(4));

        assertThat(properties.getServersRefreshInterval()).isEqualTo(Duration.ofHours(4));
    }

    @Test
    public void whenSetNullServersRefreshIntervalThenValidationFails() {
        assertThatThrownBy(() -> properties.setServersRefreshInterval(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Servers refresh interval must be greater then zero");
    }

    @Test
    public void whenSetZeroServersRefreshIntervalThenValidationFails() {
        assertThatThrownBy(() -> properties.setServersRefreshInterval(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Servers refresh interval must be greater then zero");
    }

    @Test
    public void whenSetNegativeServersRefreshIntervalThenValidationFails() {
        assertThatThrownBy(() -> properties.setServersRefreshInterval(Duration.ofDays(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Servers refresh interval must be greater then zero");
    }

    @Test
    public void whenSetHashStrategyThenValidationOk() {
        properties.setHashStrategy(MemcachedCacheProperties.HashStrategy.KETAMA);

        assertThat(properties.getHashStrategy()).isEqualTo(MemcachedCacheProperties.HashStrategy.KETAMA);
    }

    @Test
    public void whenSetNullHashStrategyThenValidationOk() {
        properties.setHashStrategy(null);

        assertThat(properties.getHashStrategy()).isNull();
    }
}