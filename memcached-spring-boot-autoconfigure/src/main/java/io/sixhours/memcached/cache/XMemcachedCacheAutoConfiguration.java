/*
 * Copyright 2016-2021 Sixhours
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

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for the Memcached cache
 * backed by XMemcached client.
 * Creates {@link CacheManager} when caching is enabled via {@link EnableCaching}.
 *
 * @author Igor Bolic
 * @author Sasa Bolic
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({net.rubyeye.xmemcached.MemcachedClient.class, CacheManager.class})
@Conditional(NotAppEngineProviderCondition.class)
@EnableConfigurationProperties(MemcachedCacheProperties.class)
public class XMemcachedCacheAutoConfiguration {

    private XMemcachedCacheAutoConfiguration() {
        // Hide public constructor
    }

    @Configuration
    @ConditionalOnRefreshScope
    static class RefreshableMemcachedCacheConfiguration {

        @Bean
        @RefreshScope
        @ConditionalOnMissingBean(value = MemcachedCacheManager.class, search = SearchStrategy.CURRENT)
        public MemcachedCacheManager cacheManager(MemcachedCacheProperties properties) throws IOException {
            return new XMemcachedCacheManagerFactory(properties).create();
        }
    }

    @Configuration
    @ConditionalOnMissingRefreshScope
    static class MemcachedCacheConfiguration {

        @Bean
        @ConditionalOnMissingBean(value = MemcachedCacheManager.class, search = SearchStrategy.CURRENT)
        public MemcachedCacheManager cacheManager(MemcachedCacheProperties properties) throws IOException {
            return new XMemcachedCacheManagerFactory(properties).create();
        }
    }
}
