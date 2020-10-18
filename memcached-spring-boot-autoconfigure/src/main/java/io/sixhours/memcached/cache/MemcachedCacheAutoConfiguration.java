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

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheAspectSupport;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for the Memcached cache.
 * Creates {@link CacheManager} when caching is enabled via {@link EnableCaching}.
 *
 * @author Igor Bolic
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingSpringCacheType
@ConditionalOnBean(CacheAspectSupport.class)
@ConditionalOnMissingBean({CacheManager.class, CacheResolver.class})
@EnableConfigurationProperties(MemcachedCacheProperties.class)
@AutoConfigureBefore(CacheAutoConfiguration.class)
@AutoConfigureAfter(name = "org.springframework.cloud.autoconfigure.RefreshAutoConfiguration")
@Import({AppEngineMemcachedCacheAutoConfiguration.class, XMemcachedCacheAutoConfiguration.class, SpymemcachedCacheAutoConfiguration.class})
public class MemcachedCacheAutoConfiguration {
}
