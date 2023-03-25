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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.Cache;
import org.springframework.cache.support.NullValue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.endsWith;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Memcached cache tests.
 *
 * @author Igor Bolic
 */
public class MemcachedCacheTest {

    private static final String CACHED_OBJECT_KEY = "cached_value_key";

    private static final String CACHE_NAME = "cache";
    private static final String CACHE_PREFIX = Default.PREFIX;
    private static final int CACHE_EXPIRATION = Default.EXPIRATION;

    private static final String CACHED_KEY_REGEX = String.format("%s:.+:%s", CACHE_PREFIX, CACHED_OBJECT_KEY);

    private static final String NAMESPACE_KEY = Default.NAMESPACE;
    private static final String NAMESPACE_KEY_VALUE = String.valueOf(System.currentTimeMillis());

    private IMemcachedClient memcachedClient;
    private MemcachedCache memcachedCache;

    private final Object cachedValue = new Object();
    private final Object nullCachedValue = NullValue.INSTANCE;
    private final Object newCachedValue = new Object();

    private final Object valueLoaderValue = new Object();
    private final Object valueLoaderNullValue = NullValue.INSTANCE;

    private String memcachedKey;
    private String namespaceKey;

    @Before
    public void setUp() {
        memcachedClient = mock(IMemcachedClient.class);
        memcachedCache = new MemcachedCache(CACHE_NAME, memcachedClient, CACHE_EXPIRATION, CACHE_PREFIX, NAMESPACE_KEY);

        memcachedKey = String.format("%s:%s:%s:%s", CACHE_PREFIX, CACHE_NAME, NAMESPACE_KEY_VALUE, CACHED_OBJECT_KEY);
        namespaceKey = String.format("%s:%s:%s", CACHE_PREFIX, CACHE_NAME, NAMESPACE_KEY);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(memcachedClient);
    }

    @Test
    public void whenLookupThenCallMemcachedClientGetKey() {
        when(memcachedClient.get(any())).thenReturn(NAMESPACE_KEY_VALUE).thenReturn(cachedValue);

        Object actual = memcachedCache.lookup(CACHED_OBJECT_KEY);

        assertThat(actual).isEqualTo(cachedValue);

        verify(memcachedClient).get(memcachedKey);
        verify(memcachedClient).get(namespaceKey);
    }

    @Test
    public void whenLookupThenIncrementHits() {
        when(memcachedClient.get(any())).thenReturn(NAMESPACE_KEY_VALUE).thenReturn(cachedValue);

        assertThat(memcachedCache.hits()).isZero();
        assertThat(memcachedCache.misses()).isZero();

        memcachedCache.lookup(CACHED_OBJECT_KEY);

        assertThat(memcachedCache.hits()).isEqualTo(1);
        assertThat(memcachedCache.misses()).isZero();

        verify(memcachedClient).get(memcachedKey);
        verify(memcachedClient).get(namespaceKey);
    }

    @Test
    public void whenLookupAndCacheValueMissingThenIncrementMisses() {
        when(memcachedClient.get(any())).thenReturn(NAMESPACE_KEY_VALUE).thenReturn(null);

        assertThat(memcachedCache.hits()).isZero();
        assertThat(memcachedCache.misses()).isZero();

        memcachedCache.lookup(CACHED_OBJECT_KEY);

        assertThat(memcachedCache.hits()).isZero();
        assertThat(memcachedCache.misses()).isEqualTo(1);

        verify(memcachedClient).get(memcachedKey);
        verify(memcachedClient).get(namespaceKey);
    }

    @Test
    public void whenGetNameThenReturnCacheName() {
        String actual = memcachedCache.getName();

        assertThat(actual).isEqualTo(CACHE_NAME);
    }

    @Test
    public void whenGetNativeThenReturnMemcachedClient() {
        IMemcachedClient actual = (IMemcachedClient) memcachedCache.getNativeCache();

        assertThat(actual).isSameAs(memcachedClient);
    }

    @Test
    public void whenGetWithValueLoaderThenReturnCachedValue() {
        when(memcachedClient.get(anyString()))
                .thenReturn(NAMESPACE_KEY_VALUE)
                .thenReturn(cachedValue);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderValue);

        assertThat(actual).isEqualTo(cachedValue);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).get(memcachedKey);
    }

    @Test
    public void whenGetWithValueLoaderAndNullCachedValueThenReturnNull() {
        when(memcachedClient.get(anyString()))
                .thenReturn(NAMESPACE_KEY_VALUE)
                .thenReturn(nullCachedValue);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderValue);

        assertThat(actual).isNull();

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).get(memcachedKey);
    }

    @Test
    public void whenGetWithValueLoaderAndCachedValueFromSecondLookupThenReturnCachedValue() {
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);
        when(memcachedClient.get(memcachedKey)).thenReturn(null).thenReturn(cachedValue);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderValue);

        assertThat(actual).isEqualTo(cachedValue);

        verify(memcachedClient, times(2)).get(namespaceKey);
        verify(memcachedClient, times(2)).get(memcachedKey);
    }

    @Test
    public void whenGetWithValueLoaderAndNullCachedValueFromSecondLookupThenReturnNull() {
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);
        when(memcachedClient.get(memcachedKey)).thenReturn(null).thenReturn(nullCachedValue);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderValue);

        assertThat(actual).isNull();

        verify(memcachedClient, times(2)).get(namespaceKey);
        verify(memcachedClient, times(2)).get(memcachedKey);
    }

    @Test
    public void whenGetWithValueLoaderAndCachedValueMissingThenReturnValueLoaderNull() {
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);
        when(memcachedClient.get(memcachedKey)).thenReturn(null);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderNullValue);

        assertThat(actual).isNull();

        verify(memcachedClient, times(3)).get(namespaceKey);
        verify(memcachedClient, times(2)).get(memcachedKey);
        verify(memcachedClient).set(memcachedKey, CACHE_EXPIRATION, valueLoaderNullValue);
        verify(memcachedClient).touch(namespaceKey, CACHE_EXPIRATION);
    }

    @Test
    public void whenGetWithValueLoaderAndCachedValueMissingThenReturnValueLoaderValue() {
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);
        when(memcachedClient.get(memcachedKey)).thenReturn(null);

        Object actual = memcachedCache.get(CACHED_OBJECT_KEY, () -> valueLoaderValue);

        assertThat(actual).isEqualTo(valueLoaderValue);

        verify(memcachedClient, times(3)).get(namespaceKey);
        verify(memcachedClient, times(2)).get(memcachedKey);
        verify(memcachedClient).set(memcachedKey, CACHE_EXPIRATION, valueLoaderValue);
        verify(memcachedClient).touch(namespaceKey, CACHE_EXPIRATION);
    }

    @Test
    public void whenGetWithValueLoaderThrowsExceptionThenValueRetrievalException() {
        when(memcachedClient.get(namespaceKey))
                .thenReturn(NAMESPACE_KEY_VALUE)
                .thenReturn(null);

        assertThatThrownBy(() ->
                memcachedCache.get(CACHED_OBJECT_KEY, () -> {
                    throw new Exception("exception to be wrapped");
                }))
                .isInstanceOf(Cache.ValueRetrievalException.class)
                .hasFieldOrPropertyWithValue("key", CACHED_OBJECT_KEY);

        verify(memcachedClient, times(2)).get(namespaceKey);
        verify(memcachedClient, times(2)).get(matches(CACHED_KEY_REGEX));
        verify(memcachedClient).set(eq(namespaceKey), eq(CACHE_EXPIRATION), anyString());
    }

    @Test
    public void whenPutNullThenStoreNullValueInstance() {
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, null);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(memcachedKey, CACHE_EXPIRATION, NullValue.INSTANCE);
        verify(memcachedClient).touch(namespaceKey, CACHE_EXPIRATION);
    }

    @Test
    public void whenPutValueExpirationThirtyDaysMinusSecondThenStoreValue() {
        int expiration = (int) Duration.ofDays(30).minusSeconds(1).getSeconds();

        memcachedCache = new MemcachedCache(
                CACHE_NAME,
                memcachedClient,
                expiration,
                CACHE_PREFIX,
                NAMESPACE_KEY
        );
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, "value");

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(memcachedKey, expiration, "value");
        verify(memcachedClient).touch(namespaceKey, expiration);
    }

    @Test
    public void whenPutValueExpirationUpToThirtyDaysThenStoreValue() {
        int expiration = (int) Duration.ofDays(30).getSeconds();

        memcachedCache = new MemcachedCache(
                CACHE_NAME,
                memcachedClient,
                expiration,
                CACHE_PREFIX,
                NAMESPACE_KEY
        );
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, "value");

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(memcachedKey, expiration, "value");
        verify(memcachedClient).touch(namespaceKey, expiration);
    }

    @Test
    public void whenPutValueExpirationOfThirtyDaysAndOneSecondThenStoreValueUsingUnixTimestamp() {
        int expiration = (int) Duration.ofDays(30).plusSeconds(1).getSeconds();
        int expectedExpiration = (int) Instant.now().plusSeconds(expiration).getEpochSecond();

        memcachedCache = new MemcachedCache(
                CACHE_NAME,
                memcachedClient,
                expiration,
                CACHE_PREFIX,
                NAMESPACE_KEY
        );
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, "value");

        ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(eq(memcachedKey), captor.capture(), eq("value"));
        verify(memcachedClient).touch(eq(namespaceKey), captor.capture());

        List<Integer> actualExpirations = captor.getAllValues();
        actualExpirations.forEach(exp -> {
            assertThat(exp).isGreaterThanOrEqualTo(expectedExpiration);
            assertThat(exp).isLessThanOrEqualTo(expectedExpiration + 1);
        });
    }

    @Test
    public void whenPutValueExpirationOfFiftyDaysAndOneSecondThenStoreValueUsingUnixTimestamp() {
        int expiration = (int) Duration.ofDays(50).getSeconds();
        int expectedExpiration = (int) Instant.now().plusSeconds(expiration).getEpochSecond();

        memcachedCache = new MemcachedCache(
                CACHE_NAME,
                memcachedClient,
                expiration,
                CACHE_PREFIX,
                NAMESPACE_KEY
        );
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, "50-value");

        ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(eq(memcachedKey), captor.capture(), eq("50-value"));
        verify(memcachedClient).touch(eq(namespaceKey), captor.capture());

        List<Integer> actualExpirations = captor.getAllValues();
        actualExpirations.forEach(exp -> {
            assertThat(exp).isGreaterThanOrEqualTo(expectedExpiration);
            assertThat(exp).isLessThanOrEqualTo(expectedExpiration + 1);
        });
    }

    @Test
    public void whenPutValueExpirationOfTenYearsThenStoreValueUsingUnixTimestamp() {
        int expiration = (int) Duration.ofDays(10 * 365).getSeconds();
        int expectedExpiration = (int) Instant.now().plusSeconds(expiration).getEpochSecond();

        memcachedCache = new MemcachedCache(
                CACHE_NAME,
                memcachedClient,
                expiration,
                CACHE_PREFIX,
                NAMESPACE_KEY
        );
        when(memcachedClient.get(namespaceKey)).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.put(CACHED_OBJECT_KEY, "10-years-value");

        ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(eq(memcachedKey), captor.capture(), eq("10-years-value"));
        verify(memcachedClient).touch(eq(namespaceKey), captor.capture());

        List<Integer> actualExpirations = captor.getAllValues();
        actualExpirations.forEach(exp -> {
            assertThat(exp).isGreaterThanOrEqualTo(expectedExpiration);
            assertThat(exp).isLessThanOrEqualTo(expectedExpiration + 1);
        });
    }

    @Test
    public void whenPutAndNamespaceMissingThenSetNamespace() {
        when(memcachedClient.get(namespaceKey)).thenReturn(null);

        memcachedCache.put(CACHED_OBJECT_KEY, cachedValue);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).set(eq(namespaceKey), eq(CACHE_EXPIRATION), anyString());
        verify(memcachedClient).set(endsWith(CACHED_OBJECT_KEY), eq(CACHE_EXPIRATION), eq(cachedValue));
        verify(memcachedClient).touch(namespaceKey, CACHE_EXPIRATION);
    }

    @Test
    public void whenPutIfAbsentThenReturnExistingValue() {
        when(memcachedClient.get(anyString())).thenReturn(NAMESPACE_KEY_VALUE).thenReturn(cachedValue);

        Cache.ValueWrapper actual = memcachedCache.putIfAbsent(CACHED_OBJECT_KEY, newCachedValue);

        assertThat(actual.get()).isEqualTo(cachedValue);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).get(memcachedKey);
    }

    @Test
    public void whenPutIfAbsentAndNoCachedValueThenReturnNewValue() {
        when(memcachedClient.get(anyString()))
                .thenReturn(NAMESPACE_KEY_VALUE)
                .thenReturn(null)
                .thenReturn(NAMESPACE_KEY_VALUE);

        Cache.ValueWrapper actual = memcachedCache.putIfAbsent(CACHED_OBJECT_KEY, newCachedValue);

        assertThat(actual.get()).isEqualTo(newCachedValue);

        verify(memcachedClient, times(2)).get(namespaceKey);
        verify(memcachedClient).get(memcachedKey);
        verify(memcachedClient).set(eq(memcachedKey), anyInt(), eq(newCachedValue));
        verify(memcachedClient).touch(namespaceKey, CACHE_EXPIRATION);
    }

    @Test
    public void whenEvictThenMemcachedClientDelete() {
        when(memcachedClient.get(anyString())).thenReturn(NAMESPACE_KEY_VALUE);

        memcachedCache.evict(CACHED_OBJECT_KEY);

        verify(memcachedClient).get(namespaceKey);
        verify(memcachedClient).delete(memcachedKey);
    }

    @Test
    public void whenClearThenMemcachedClientIncrNamespace() {
        memcachedCache.clear();

        verify(memcachedClient).incr(namespaceKey, 1);
    }
}
