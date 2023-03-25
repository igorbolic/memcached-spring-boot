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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = MemcachedCacheContextConfigIT.DefaultConfig.class, initializers = ConfigFileApplicationContextInitializer.class)
public class MemcachedCacheContextConfigIT {

    @Autowired
    private MemcachedCacheManager memcachedCacheManager;

    @Autowired
    private MemcachedCacheProperties properties;

    @Test
    public void contextLoads() {
        // Cache manager with default XMemcached client loaded
        assertThat(memcachedCacheManager).isNotNull();
        assertThat(memcachedCacheManager.client()).isNotNull();
        assertThat(memcachedCacheManager.client()).isInstanceOf(XMemcachedClient.class);

        // Configuration properties loaded
        assertThat(properties).isNotNull();
        assertThat(properties.getServers()).hasSize(2);
        assertThat(properties.getServers().get(0)).isNotNull();
        assertThat(properties.getServers().get(0).getHostName()).isEqualTo("example1.com");
        assertThat(properties.getServers().get(0).getPort()).isEqualTo(12345);
        assertThat(properties.getServers().get(1)).isNotNull();
        assertThat(properties.getServers().get(1).getHostName()).isEqualTo("example2.com");
        assertThat(properties.getServers().get(1).getPort()).isEqualTo(12346);
        assertThat(properties.getServersRefreshInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getOperationTimeout()).isEqualTo(Duration.ofMillis(7200));
        assertThat(properties.getPrefix()).isEqualTo("memcached:test-app");
        assertThat(properties.getProtocol()).isEqualTo(MemcachedCacheProperties.Protocol.TEXT);
        assertThat(properties.getProvider()).isEqualTo(MemcachedCacheProperties.Provider.STATIC);
        assertThat(properties.getExpiration()).isEqualTo(Duration.ofDays(40));
        assertThat(properties.getExpirationPerCache()).hasSize(4);
        assertThat(properties.getExpirationPerCache().get("cache_name1")).isEqualTo(Duration.ofHours(1));
        assertThat(properties.getExpirationPerCache().get("cache_name2")).isEqualTo(Duration.ofHours(10));
        assertThat(properties.getExpirationPerCache().get("cache_name3")).isEqualTo(Duration.ofHours(2));
        assertThat(properties.getExpirationPerCache().get("cache_name4")).isEqualTo(Duration.ofDays(60));
        assertThat(properties.getHashStrategy()).isEqualTo(MemcachedCacheProperties.HashStrategy.KETAMA);
        assertThat(properties.getMetricsCacheNames())
                .hasSize(3)
                .containsExactly(
                        "cache_name1",
                        "cache_name2",
                        "cache_name3"
                );
        assertThat(properties.getDisabledCacheNames())
                .hasSize(3)
                .containsExactlyInAnyOrder(
                        "disabled_cache_name",
                        "something",
                        "nothing"
                );
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    static class DefaultConfig {

    }
}