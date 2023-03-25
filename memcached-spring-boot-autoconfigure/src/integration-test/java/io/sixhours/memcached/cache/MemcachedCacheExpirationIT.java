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

import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mockStatic;

/**
 * Memcached cache expiration integration tests.
 *
 * @author Igor Bolic
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = MemcachedCacheExpirationIT.CacheConfig.class)
public class MemcachedCacheExpirationIT {

    @ClassRule
    public static GenericContainer memcached = new GenericContainer("memcached:alpine")
            .withExposedPorts(11211);

    @Autowired
    MemcachedCacheManager cacheManager;

    @Autowired
    IMemcachedClient memcachedClient;

    @Before
    public void setUp() {
        // Clear cache before each test
        memcachedClient.flush();
    }

    @Test
    public void whenNewCacheCacheExpiresThenCachedValueNotFound() {
        // Given new cache and default expiration of 4 seconds
        Cache cache = cacheManager.getCache("cache");
        assertThat(cache).isNotNull();
        assertThat(cache).isInstanceOf(MemcachedCache.class);

        // When value is cached
        cache.put("key", "value");

        // Then cached value expires in 5 seconds
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Cache.ValueWrapper actual = cache.get("key");

            assertThat(actual).isNull();
        });
    }

    @Test
    public void whenAuthorsCacheExpiresThenCachedValueNotFound() {
        // Given authors cache with expiration in 3 seconds
        Cache authors = cacheManager.getCache("authors");
        assertThat(authors).isNotNull();
        assertThat(authors).isInstanceOf(MemcachedCache.class);

        // When value is cached
        authors.put("test-key", "value");

        // Then cached value expires in 4 seconds
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            Cache.ValueWrapper actual = authors.get("test-key");

            assertThat(actual).isNull();
        });
    }

    @Test
    public void whenExpirationInThirtyDaysThenValueCached() {
        // Given Instant.now() time is 30 days and 2 seconds in the past
        Instant pastTime = Instant.now()
                .minusSeconds(Duration.ofDays(30).minusSeconds(2).getSeconds());
        Clock clock = Clock.fixed(pastTime, ZoneId.of("UTC"));

        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            mockedStatic.when(Clock::systemUTC).thenReturn(clock);
            Instant now = Instant.now();
            assertThat(now).isEqualTo(pastTime);

            // Given 30-day cache with expiration in 30 days
            Cache cache = cacheManager.getCache("30-days");
            assertThat(cache).isNotNull();
            assertThat(cache).isInstanceOf(MemcachedCache.class);

            // When value is cached
            cache.put("test-key", "value");

            // Then value should be found in cache i.e. unix timestamp
            // should not have been used for setting the expiration time,
            // since if it was, the expiration time would be in past
            Cache.ValueWrapper actual = cache.get("test-key");

            assertThat(actual).isNotNull();
            assertThat(actual.get()).isEqualTo("value");
        }
    }

    @Test
    public void whenExpirationInThirtyDaysAndSecondThenValueCached() {
        // Given Instant.now() time is 30 days and 2 second in the past
        Instant pastTime = Instant.now()
                .minusSeconds(Duration.ofDays(30).minusSeconds(2).getSeconds());
        Clock clock = Clock.fixed(pastTime, ZoneId.of("UTC"));

        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            mockedStatic.when(Clock::systemUTC).thenReturn(clock);
            Instant now = Instant.now();
            assertThat(now).isEqualTo(pastTime);

            // Given 30-day cache with expiration in 30 days and 1 second
            Cache cache = cacheManager.getCache("30-days-1-second");
            assertThat(cache).isNotNull();
            assertThat(cache).isInstanceOf(MemcachedCache.class);

            // When value is cached
            cache.put("test-key", "value");

            // The unix timestamp should have been added with the expiration in seconds
            // i.e. in case it was not, the expiration would equal to
            // Instant.ofEpochSecond(60*60*24*30 + 1) = 1970-01-31T00:00:01Z, which is in the
            // past, and therefore value would not have been cached at all.

            // Then value should be available in cache for the first 2 seconds
            await().during(Duration.ofSeconds(2)).untilAsserted(() -> {
                Cache.ValueWrapper actual = cache.get("test-key");

                assertThat(actual).isNotNull();
                assertThat(actual.get()).isEqualTo("value");
            });
            // And should not be found in cache within next 2 seconds
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                Cache.ValueWrapper actual = cache.get("test-key");

                assertThat(actual).isNull();
            });
        }
    }

    @Test
    public void whenExpirationInThirtyDaysAndSomeSecondsThenUnixTimestampExpirationUsed() {
        // Given Instant.now() time is 30 days and 2 seconds in the past
        Instant pastTime = Instant.now()
                .minusSeconds(Duration.ofDays(30).minusSeconds(2).getSeconds());
        Clock clock = Clock.fixed(pastTime, ZoneId.of("UTC"));

        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            mockedStatic.when(Clock::systemUTC).thenReturn(clock);
            Instant now = Instant.now();
            assertThat(now).isEqualTo(pastTime);

            // Given cache with expiration in 30 days and 1 second
            Cache oneSecond = cacheManager.getCache("30-days-1-second");
            assertThat(oneSecond).isNotNull();
            assertThat(oneSecond).isInstanceOf(MemcachedCache.class);

            // When value is cached
            oneSecond.put("test-key-expired", "value-expired");

            // Then cache should expire in at most 4 seconds
            await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                Cache.ValueWrapper actual = oneSecond.get("test-key-expired");

                assertThat(actual).isNull();
            });

            // Given expiration in 30 days and 5 seconds
            Cache fiveSeconds = cacheManager.getCache("30-days-5-seconds");
            assertThat(fiveSeconds).isNotNull();
            assertThat(fiveSeconds).isInstanceOf(MemcachedCache.class);

            // When value is cached
            fiveSeconds.put("test-key-not-expired", "value-not-expired");

            // Then cache should not expire in first 3 seconds
            await().during(Duration.ofSeconds(3)).untilAsserted(() -> {
                Cache.ValueWrapper actual = fiveSeconds.get("test-key-not-expired");

                assertThat(actual).isNotNull();
                assertThat(actual.get()).isEqualTo("value-not-expired");
            });
            // And should expire after additional 4 seconds
            await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                Cache.ValueWrapper actual = fiveSeconds.get("test-key-not-expired");

                assertThat(actual).isNull();
            });
        }
    }

    @Test
    public void whenExpirationInMoreThanThirtyDaysThenValueCached() {
        // Given 40-days cache with expiration in 41 days
        Cache cache = cacheManager.getCache("41-days");
        assertThat(cache).isNotNull();
        assertThat(cache).isInstanceOf(MemcachedCache.class);

        // When value is cached
        cache.put("test-key", "value");

        // Then value is still found in cache after 2 seconds
        await().during(Duration.ofSeconds(2)).untilAsserted(() -> {
            Cache.ValueWrapper actual = cache.get("test-key");

            assertThat(actual).isNotNull();
            assertThat(actual.get()).isEqualTo("value");
        });
    }

    @Test
    public void whenExpirationOfMoreThanThirtyDaysExpiresThenCachedValueNotFound() {
        // Given Instant.now() time is 40 days and 2 seconds in the past
        Instant pastTime = Instant.now()
                .minusSeconds(Duration.ofDays(40).minusSeconds(2).getSeconds());
        Clock clock = Clock.fixed(pastTime, ZoneId.of("UTC"));

        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            mockedStatic.when(Clock::systemUTC).thenReturn(clock);
            Instant now = Instant.now();
            assertThat(now).isEqualTo(pastTime);

            // Given 40-days cache with expiration in 40 days i.e. in 2 seconds from Instant.now()
            Cache forthyCache = cacheManager.getCache("40-days");
            assertThat(forthyCache).isNotNull();
            assertThat(forthyCache).isInstanceOf(MemcachedCache.class);

            // When value is cached
            forthyCache.put("test-key-expired", "value-expired");

            // Then cache should expire in at most 3 seconds
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                Cache.ValueWrapper actual = forthyCache.get("test-key-expired");

                assertThat(actual).isNull();
            });

            // Given expiration in 40 days and 4 seconds
            Cache forthyAndFourCache = cacheManager.getCache("40-days-4-seconds");
            assertThat(forthyAndFourCache).isNotNull();
            assertThat(forthyAndFourCache).isInstanceOf(MemcachedCache.class);

            // When value is cached
            forthyAndFourCache.put("test-key-not-expired", "value-not-expired");

            // Then cache should not expire in 3 seconds
            await().during(Duration.ofSeconds(3)).untilAsserted(() -> {
                Cache.ValueWrapper actual = forthyAndFourCache.get("test-key-not-expired");

                assertThat(actual).isNotNull();
                assertThat(actual.get()).isEqualTo("value-not-expired");
            });
            // And should expire after additional 3 seconds
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                Cache.ValueWrapper actual = forthyAndFourCache.get("test-key-not-expired");

                assertThat(actual).isNotNull();
                assertThat(actual.get()).isEqualTo("value-not-expired");
            });
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableCaching
    @ComponentScan(basePackageClasses = {AuthorService.class, BookService.class})
    static class CacheConfig {

        @Bean
        public MemcachedCacheManager cacheManager() throws IOException {
            final MemcachedCacheManager memcachedCacheManager = new MemcachedCacheManager(memcachedClient());
            memcachedCacheManager.setExpiration(4);

            Map<String, Integer> expirationsPerCache = new HashMap<>();
            expirationsPerCache.put("authors", 3);
            expirationsPerCache.put("30-days", (int) Duration.ofDays(30).getSeconds());
            expirationsPerCache.put("30-days-1-second", (int) Duration.ofDays(30).plusSeconds(1).getSeconds());
            expirationsPerCache.put("30-days-5-seconds", (int) Duration.ofDays(30).plusSeconds(5).getSeconds());
            expirationsPerCache.put("40-days", (int) Duration.ofDays(40).getSeconds());
            expirationsPerCache.put("40-days-4-seconds", (int) Duration.ofDays(40).plusSeconds(4).getSeconds());
            expirationsPerCache.put("41-days", (int) Duration.ofDays(41).getSeconds());
            memcachedCacheManager.setExpirationPerCache(expirationsPerCache);

            return memcachedCacheManager;
        }

        @Bean
        public IMemcachedClient memcachedClient() throws IOException {
            final String host = memcached.getHost();
            final int port = memcached.getMappedPort(11211);

            return new XMemcachedClient(new XMemcachedClientBuilder(Collections.singletonList(new InetSocketAddress(host, port))).build());
        }
    }
}
