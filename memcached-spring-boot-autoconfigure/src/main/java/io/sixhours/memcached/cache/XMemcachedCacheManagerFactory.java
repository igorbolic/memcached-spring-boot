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

import net.rubyeye.xmemcached.CommandFactory;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.MemcachedSessionLocator;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.aws.AWSElasticCacheClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.command.TextCommandFactory;
import net.rubyeye.xmemcached.impl.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Factory for the XMemcached {@link MemcachedCacheManager} instances.
 *
 * @author Igor Bolic
 */
public class XMemcachedCacheManagerFactory extends MemcachedCacheManagerFactory {

    public XMemcachedCacheManagerFactory(MemcachedCacheProperties properties) {
        super(properties);
    }

    @Override
    IMemcachedClient memcachedClient() throws IOException {
        final List<InetSocketAddress> servers = properties.getServers();
        final MemcachedCacheProperties.Provider provider = properties.getProvider();
        final MemcachedCacheProperties.Protocol protocol = properties.getProtocol();
        final MemcachedCacheProperties.HashStrategy hashStrategy = properties.getHashStrategy();

        final MemcachedClientBuilder builder = builder(provider, servers);

        if (builder instanceof AWSElasticCacheClientBuilder) {
            ((AWSElasticCacheClientBuilder) builder)
                    .setPollConfigIntervalMs(properties.getServersRefreshInterval().toMillis());
        }

        builder.setSessionLocator(hashStrategyToLocator(hashStrategy));
        builder.setOpTimeout(properties.getOperationTimeout().toMillis());
        builder.setCommandFactory(commandFactory(protocol));

        return new XMemcachedClient(builder.build());
    }

    private MemcachedClientBuilder builder(MemcachedCacheProperties.Provider provider, List<InetSocketAddress> servers) {
        switch (provider) {
            case STATIC:
                return new XMemcachedClientBuilder(servers);
            case AWS:
                new AWSElasticCacheClientBuilder(servers);
            default:
                throw new IllegalArgumentException("Invalid provider for the XMemcached configuration");
        }
    }

    private CommandFactory commandFactory(MemcachedCacheProperties.Protocol protocol) {
        switch (protocol) {
            case TEXT:
                return new TextCommandFactory();
            case BINARY:
                return new BinaryCommandFactory();
            default:
                throw new IllegalArgumentException("Invalid protocol for the XMemcached configuration");
        }
    }

    private MemcachedSessionLocator hashStrategyToLocator(MemcachedCacheProperties.HashStrategy hashStrategy) {
        switch (hashStrategy) {
            case STANDARD:
                return new ArrayMemcachedSessionLocator();
            case LIBMEMCACHED:
                return new LibmemcachedMemcachedSessionLocator();
            case KETAMA:
                return new KetamaMemcachedSessionLocator();
            case PHP:
                return new PHPMemcacheSessionLocator();
            case ELECTION:
                return new ElectionMemcachedSessionLocator();
            case ROUNDROBIN:
                return new RoundRobinMemcachedSessionLocator();
            case RANDOM:
                return new RandomMemcachedSessionLocaltor();
            default:
                throw new IllegalArgumentException("Invalid hash strategy for the XMemcached configuration");
        }
    }
}
