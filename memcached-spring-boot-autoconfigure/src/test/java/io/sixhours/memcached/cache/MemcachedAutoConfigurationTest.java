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

import net.rubyeye.xmemcached.impl.*;
import org.junit.Test;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.util.ReflectionTestUtils;

import static io.sixhours.memcached.cache.MemcachedAssertions.assertMemcachedCacheManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Memcached auto-configuration tests.
 *
 * @author Igor Bolic
 * @author Sasa Bolic
 */
public class MemcachedAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CacheAutoConfiguration.class, MemcachedCacheAutoConfiguration.class));

    @Test
    public void whenCachingNotEnabledThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(EmptyConfiguration.class)
                .run(context -> assertThat(context).doesNotHaveBean(MemcachedCacheManager.class));
    }

    @Test
    public void whenCacheTypeIsNoneThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("spring.cache.type=none")
                .run(context -> assertThat(context).doesNotHaveBean(MemcachedCacheManager.class));
    }

    @Test
    public void whenCacheTypeIsNoneThenNoOpCacheLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("spring.cache.type=none")
                .run(context -> cacheManager(context, NoOpCacheManager.class));
    }

    @Test
    public void whenCacheTypeIsSimpleThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("spring.cache.type=simple")
                .run(context -> assertThat(context).doesNotHaveBean(MemcachedCacheManager.class));
    }

    @Test
    public void whenCacheTypeIsSimpleThenSimpleCacheLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("spring.cache.type=simple")
                .run(context -> cacheManager(context, ConcurrentMapCacheManager.class));
    }

    @Test
    public void whenCacheTypeIsInvalidThenContextNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("spring.cache.type=invalid-type")
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(BeanCreationException.class)
                        .hasMessageContaining("Failed to bind properties under 'spring.cache.type'")
                );
    }

    @Test
    public void whenUsingCustomCacheManagerThenMemcachedCacheManagerNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheWithCustomCacheManagerConfiguration.class)
                .run(context -> assertThat(context).doesNotHaveBean(MemcachedCacheManager.class));
    }

    @Test
    public void whenUsingCustomCacheManagerThenMemcachedCustomCacheManagerLoaded() {
        this.contextRunner.withUserConfiguration(CacheWithCustomCacheManagerConfiguration.class)
                .run(context -> cacheManager(context, ConcurrentMapCacheManager.class));
    }

    @Test
    public void whenNoCustomCacheManagerThenMemcachedCacheManagerLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .run(context -> cacheManager(context, MemcachedCacheManager.class));
    }

    @Test
    public void whenRefreshAutoConfigurationThenRefreshConfigurationLoaded() {
        this.contextRunner.withUserConfiguration(CacheWithRefreshAutoConfiguration.class)
                .run(context -> {
                    assertThat(context.getBeanDefinitionNames()).contains("cacheManager");
                    assertThat(context.getBeansWithAnnotation(RefreshScope.class)).hasSize(1);
                    assertThat(context.getBeansWithAnnotation(RefreshScope.class).get("scopedTarget.cacheManager")).isInstanceOf(DisposableMemcachedCacheManager.class);
                });
    }

    @Test
    public void whenMemcachedCacheManagerBeanAlreadyInContextThenMemcachedWithNonCustomConfigurationLoaded() {
        this.contextRunner.withUserConfiguration(CacheWithMemcachedCacheManagerConfiguration.class)
                .withPropertyValues(
                        "memcached.cache.expiration=3600",
                        "memcached.cache.prefix=custom:prefix",
                        "memcached.cache.namespace=custom_namespace"
                )
                .run(context -> {
                    assertThat(context)
                            .hasSingleBean(MemcachedCacheManager.class)
                            .as("Auto-configured disposable instance should not be loaded in context")
                            .isNotInstanceOf(DisposableMemcachedCacheManager.class);

                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertMemcachedCacheManager(memcachedCacheManager, Default.EXPIRATION, null, Default.PREFIX, Default.NAMESPACE);
                });
    }

    @Test
    public void whenAwsProviderAndMultipleServerListThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        String.format("memcached.cache.servers=%s:%d,%s:%d", "memcachedHost1", 11211, "memcachedHost2", 11211),
                        "memcached.cache.provider=aws"
                )
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(BeanCreationException.class)
                        .hasCauseInstanceOf(BeanInstantiationException.class)
                        .hasMessageContaining("Invalid provider for the XMemcached configuration")
                );
    }

    @Test
    public void whenAppEngineProviderThenAppEngineMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.provider=appengine")
                .run(context -> {
                    CacheManager cacheManager = cacheManager(context, CacheManager.class);
                    assertThat(cacheManager).isInstanceOf(DisposableMemcachedCacheManager.class)
                            .hasFieldOrProperty("memcachedClient");
                    assertThat(((DisposableMemcachedCacheManager) cacheManager).memcachedClient)
                            .isInstanceOf(AppEngineMemcachedClient.class);
                });
    }

    @Test
    public void whenAppEngineProviderAndNoAppEngineOnClasspathThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.provider=appengine")
                .withClassLoader(new FilteredClassLoader("com.google.appengine.api.memcache"))
                .run(context ->  assertThat(context).doesNotHaveBean(MemcachedCacheManager.class));
    }

    @Test
    public void whenNoAppEngineOnClasspathThenXMemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withClassLoader(new FilteredClassLoader("com.google.appengine.api.memcache"))
                .run(context -> {
                    CacheManager cacheManager = cacheManager(context, CacheManager.class);
                    assertThat(cacheManager).isInstanceOf(DisposableMemcachedCacheManager.class)
                            .hasFieldOrProperty("memcachedClient");
                    assertThat(((DisposableMemcachedCacheManager) cacheManager).memcachedClient)
                            .isInstanceOf(XMemcachedClient.class);
                });
    }

    @Test
    public void whenNoAppEngineAndXmemcachedOnClasspathThenSpymemcachedLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withClassLoader(new FilteredClassLoader("net.rubyeye.xmemcached", "com.google.appengine.api.memcache"))
                .run(context -> {
                    CacheManager cacheManager = cacheManager(context, CacheManager.class);
                    assertThat(cacheManager).isInstanceOf(DisposableMemcachedCacheManager.class)
                            .hasFieldOrProperty("memcachedClient");
                    assertThat(((DisposableMemcachedCacheManager) cacheManager).memcachedClient)
                            .isInstanceOf(SpymemcachedClient.class);
                });
    }

    @Test
    public void whenStaticProviderAndEmptyServerListThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        "memcached.cache.servers=",
                        "memcached.cache.provider=static"
                )
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(UnsatisfiedDependencyException.class)
                        .hasRootCause(new IllegalArgumentException("Server list is empty"))
                );
    }

    @Test
    public void whenOperationTimeoutZeroThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.operation-timeout=0")
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(UnsatisfiedDependencyException.class)
                        .hasRootCause(new IllegalArgumentException("Operation timeout must be greater then zero"))
                );
    }

    @Test
    public void whenOperationTimeoutNegativeThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.operation-timeout=-1")
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(UnsatisfiedDependencyException.class)
                        .hasRootCause(new IllegalArgumentException("Operation timeout must be greater then zero"))
                );
    }

    @Test
    public void whenServersRefreshIntervalZeroThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.servers-refresh-interval=0")
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(UnsatisfiedDependencyException.class)
                        .hasRootCause(new IllegalArgumentException("Servers refresh interval must be greater then zero"))
                );
    }

    @Test
    public void whenServersRefreshIntervalNegativeThenMemcachedNotLoaded() {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues("memcached.cache.servers-refresh-interval=-1")
                .run(context -> assertThat(context).getFailure()
                        .isInstanceOf(UnsatisfiedDependencyException.class)
                        .hasRootCause(new IllegalArgumentException("Servers refresh interval must be greater then zero"))
                );
    }

    @Test
    public void whenStaticProviderAndStandardHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("standard", ArrayMemcachedSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndLibmemcachedHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("libmemcached", LibmemcachedMemcachedSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndKetamaHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("ketama", KetamaMemcachedSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndPhpHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("php", PHPMemcacheSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndElectionHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("election", ElectionMemcachedSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndRoundRobinHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("roundrobin", RoundRobinMemcachedSessionLocator.class);
    }

    @Test
    public void whenStaticProviderAndRandomHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator("random", RandomMemcachedSessionLocaltor.class);
    }

    @Test
    public void whenStaticProviderAndNoHashStrategyThenMemcachedLoaded() {
        whenHashStrategyThenCorrectSessionLocator(null, ArrayMemcachedSessionLocator.class);
    }

    private <T extends CacheManager> T cacheManager(AssertableApplicationContext loaded, Class<T> type) {
        CacheManager cacheManager = loaded.getBean(CacheManager.class);
        assertThat(cacheManager).isInstanceOf(type);
        return type.cast(cacheManager);
    }

    private void whenHashStrategyThenCorrectSessionLocator(String hashStrategy, Class<?> memcachedSessionLocatorClass) {
        this.contextRunner.withUserConfiguration(CacheConfiguration.class)
                .withPropertyValues(
                        "memcached.cache.provider=static",
                        hashStrategy != null ? "memcached.cache.hash_strategy=" + hashStrategy : ""
                )
                .run(context -> {
                    MemcachedCacheManager memcachedCacheManager = cacheManager(context, MemcachedCacheManager.class);
                    assertThat(memcachedCacheManager).hasFieldOrProperty("memcachedClient");

                    XMemcachedClient memcachedClient = (XMemcachedClient) ReflectionTestUtils.getField(memcachedCacheManager, "memcachedClient");
                    assertThat(memcachedClient)
                            .isNotNull()
                            .isInstanceOf(io.sixhours.memcached.cache.XMemcachedClient.class)
                            .hasFieldOrProperty("memcachedClient");

                    net.rubyeye.xmemcached.XMemcachedClient xMemcachedClient = (net.rubyeye.xmemcached.XMemcachedClient) ReflectionTestUtils.getField(memcachedClient, "memcachedClient");

                    assertThat(xMemcachedClient).isNotNull();
                    assertThat(xMemcachedClient.getSessionLocator()).isInstanceOf(memcachedSessionLocatorClass);
                });
    }

    @Configuration
    static class EmptyConfiguration {
    }

    @Configuration
    @EnableCaching
    static class CacheConfiguration {
    }

    @Configuration
    static class CacheWithCustomCacheManagerConfiguration extends CacheConfiguration {

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager();
        }
    }

    @Configuration
    static class CacheWithMemcachedCacheManagerConfiguration extends CacheConfiguration {

        @Bean
        public MemcachedCacheManager cacheManager() {
            IMemcachedClient memcachedClient = mock(IMemcachedClient.class);

            return new MemcachedCacheManager(memcachedClient);
        }
    }

    @Configuration
    @Import(RefreshAutoConfiguration.class)
    static class CacheWithRefreshAutoConfiguration extends CacheConfiguration {
    }

    // TODO: 10/17/20 How to test for Spymemcached auto configuration i.e. the cache manager ???
    @Configuration
    static class CacheWithSpymemcachedConfiguration extends CacheConfiguration {
    }

}
