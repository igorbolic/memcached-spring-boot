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

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Factory for the Spymemcached {@link MemcachedCacheManager} instances.
 *
 * @author Igor Bolic
 */
public class SpymemcachedCacheManagerFactory extends MemcachedCacheManagerFactory {

    public SpymemcachedCacheManagerFactory(MemcachedCacheProperties properties) {
        super(properties);
    }

    @Override
    IMemcachedClient memcachedClient() throws IOException {
        final List<InetSocketAddress> servers = properties.getServers();
        final MemcachedCacheProperties.Provider provider = properties.getProvider();
        final MemcachedCacheProperties.Protocol protocol = properties.getProtocol();
        final MemcachedCacheProperties.HashStrategy hashStrategy = properties.getHashStrategy();

        final ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder()
                .setLocatorType(hashStrategyToLocator(hashStrategy))
                .setClientMode(clientMode(provider))
                .setOpTimeout(properties.getOperationTimeout().toMillis())
                .setProtocol(connectionProtocol(protocol));

        return new SpymemcachedClient(new MemcachedClient(connectionFactoryBuilder.build(), servers));
    }

    private ClientMode clientMode(MemcachedCacheProperties.Provider provider) {
        switch (provider) {
            case STATIC:
                return ClientMode.Static;
            case AWS:
                return ClientMode.Dynamic;
            default:
                throw new IllegalArgumentException("Invalid provider for the Spymemcached configuration");
        }
    }

    private ConnectionFactoryBuilder.Protocol connectionProtocol(MemcachedCacheProperties.Protocol protocol) {
        switch (protocol) {
            case TEXT:
                return ConnectionFactoryBuilder.Protocol.TEXT;
            case BINARY:
                return ConnectionFactoryBuilder.Protocol.BINARY;
            default:
                throw new IllegalArgumentException("Invalid protocol for the Spymemcached configuration");
        }
    }

    private ConnectionFactoryBuilder.Locator hashStrategyToLocator(MemcachedCacheProperties.HashStrategy hashStrategy) {
        switch (hashStrategy) {
            case STANDARD:
                return ConnectionFactoryBuilder.Locator.ARRAY_MOD;
            case KETAMA:
                return ConnectionFactoryBuilder.Locator.CONSISTENT;
            default:
                throw new IllegalArgumentException("Invalid hash strategy for the Spymemcached configuration");
        }
    }
}
