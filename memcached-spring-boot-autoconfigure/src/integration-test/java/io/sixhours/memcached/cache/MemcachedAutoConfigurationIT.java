/**
 * Copyright 2016-2020 Sixhours
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

import io.sixhours.memcached.cache.MemcachedCacheProperties.Protocol;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.sixhours.memcached.cache.MemcachedAssertions.assertMemcachedCacheManager;
import static io.sixhours.memcached.cache.MemcachedAssertions.assertMemcachedClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Memcached auto-configuration integration tests.
 *
 * @author Igor Bolic
 * @author Sasa Bolic
 */
public class MemcachedAutoConfigurationIT {

    @ClassRule
    public static GenericContainer MEMCACHED_1 = new GenericContainer(DockerImageName.parse("memcached:alpine"))
            .withExposedPorts(11211);
    @ClassRule
    public static GenericContainer MEMCACHED_2 = new GenericContainer(DockerImageName.parse("memcached:alpine"))
            .withExposedPorts(11211);

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CacheAutoConfiguration.class, MemcachedCacheAutoConfiguration.class));

    private String memcachedHost1;
    private int memcachedPort1;

    private String memcachedHost2;
    private int memcachedPort2;

    @Before
    public void setUp() {
        memcachedHost1 = MEMCACHED_1.getContainerIpAddress();
        memcachedPort1 = MEMCACHED_1.getFirstMappedPort();

        memcachedHost2 = MEMCACHED_2.getContainerIpAddress();
        memcachedPort2 = MEMCACHED_2.getFirstMappedPort();
    }

    @Test
    public void whenNoCustomCacheManagerThenMemcachedWithDefaultConfigurationLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .run(this::assertMemcached);
    }

    @Test
    public void whenRefreshAutoConfigurationThenDefaultConfigurationLoaded() {
        this.contextRunner.withUserConfiguration(CacheWithRefreshAutoConfiguration.class)
                .run(this::assertMemcached);
    }

    @Test
    public void whenStaticProviderAndMultipleServerListThenMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d, %s:%d", memcachedHost1, memcachedPort1, memcachedHost2, memcachedPort2),
                        "memcached.cache.provider=static"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Default.PROTOCOL, Default.OPERATION_TIMEOUT, new InetSocketAddress(memcachedHost1, memcachedPort1), new InetSocketAddress(memcachedHost2, memcachedPort2));
                    assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, Collections.emptyMap(), Default.PREFIX, Default.NAMESPACE);
                });
    }

    @Test
    public void whenTextProtocolAndMultipleServerListThenMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d, %s:%d", memcachedHost1, memcachedPort1, memcachedHost2, memcachedPort2),
                        "memcached.cache.protocol=text"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Protocol.TEXT, Default.OPERATION_TIMEOUT, new InetSocketAddress(memcachedHost1, memcachedPort1), new InetSocketAddress(memcachedHost2, memcachedPort2));
                    assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, Collections.emptyMap(), Default.PREFIX, Default.NAMESPACE);
                });
    }

    @Test
    public void whenBinaryProtocolAndMultipleServerListThenMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d, %s:%d", memcachedHost1, memcachedPort1, memcachedHost2, memcachedPort2),
                        "memcached.cache.protocol=binary"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Protocol.BINARY, Default.OPERATION_TIMEOUT, new InetSocketAddress(memcachedHost1, memcachedPort1), new InetSocketAddress(memcachedHost2, memcachedPort2));
                    assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, Collections.emptyMap(), Default.PREFIX, Default.NAMESPACE);
                });
    }

    @Test
    public void whenCustomConfigurationThenMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d", memcachedHost1, memcachedPort1),
                        "memcached.cache.provider=static",
                        "memcached.cache.expiration=3600",
                        "memcached.cache.expiration-per-cache.myKey1=400",
                        "memcached.cache.prefix=custom:prefix",
                        "memcached.cache.operation-timeout=3000",
                        "memcached.cache.namespace=custom_namespace"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Default.PROTOCOL, 3000, new InetSocketAddress(memcachedHost1, memcachedPort1));
                    assertMemcachedCacheManager(memcachedCacheManager, 3600, Collections.singletonMap("myKey1", 400), "custom:prefix", Default.NAMESPACE);
                });
    }

    @Test
    public void whenPartialConfigurationValuesThenMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d", memcachedHost1, memcachedPort1),
                        "memcached.cache.prefix=custom:prefix"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Default.PROTOCOL, Default.OPERATION_TIMEOUT, new InetSocketAddress(memcachedHost1, memcachedPort1));
                    assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, Collections.emptyMap(), "custom:prefix", Default.NAMESPACE);
                });
    }

    @Test
    public void whenExpirationsValuesGiven() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d", memcachedHost1, memcachedPort1),
                        "memcached.cache.expiration=800",
                        "memcached.cache.expiration-per-cache.testKey1=400",
                        "memcached.cache.expiration-per-cache.testKey2=500",
                        "memcached.cache.expiration-per-cache.testKey3=600",
                        "memcached.cache.expiration-per-cache.testKey4=700"
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    assertMemcachedClient(memcachedClient, Default.PROTOCOL, Default.OPERATION_TIMEOUT, new InetSocketAddress(memcachedHost1, memcachedPort1));

                    final Map<String, Integer> expirations = Stream.of(new Object[][]{
                            {"testKey1", 400},
                            {"testKey2", 500},
                            {"testKey3", 600},
                            {"testKey4", 700},
                    }).collect(Collectors.toMap(e -> (String) e[0], e -> (Integer) e[1]));

                    assertMemcachedCacheManager(memcachedCacheManager, 800, expirations, Default.PREFIX, Default.NAMESPACE);
                });
    }

    private <T extends CacheManager> T cacheManager(AssertableApplicationContext loaded, Class<T> type) {
        CacheManager cacheManager = loaded.getBean(CacheManager.class);
        assertThat(cacheManager).isInstanceOf(type);
        return type.cast(cacheManager);
    }

    private void assertMemcached(AssertableApplicationContext context) {
        MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
        assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

        XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
        assertThat(memcachedClient)
                .isNotNull()
                .isInstanceOf(XMemcachedClient.class)
                .hasFieldOrProperty("memcachedClient");

        assertMemcachedClient(memcachedClient, Default.PROTOCOL, Default.OPERATION_TIMEOUT);
        assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, Collections.emptyMap(), Default.PREFIX, Default.NAMESPACE);
    }

    @Configuration
    @EnableCaching
    static class CacheConfiguration {
    }

    @Configuration
    @Import(RefreshAutoConfiguration.class)
    static class CacheWithRefreshAutoConfiguration extends CacheConfiguration {
    }

}
