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

import net.spy.memcached.MemcachedClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

public class SpymemcachedClientTest {

    private final MemcachedClient client = mock(MemcachedClient.class);

    private SpymemcachedClient memcachedClient;

    @Before
    public void setUp() {
        this.memcachedClient = new SpymemcachedClient(client);

        given(client.get(anyString())).willReturn("result");
        given(client.incr(anyString(), anyLong())).willReturn(123L);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(client);
    }

    @Test
    public void whenGetNativeCache_thenReturnCorrectValue() {
        Object result = memcachedClient.nativeCache();

        assertThat(result).isInstanceOf(MemcachedClient.class);
    }

    @Test
    public void whenGet_thenCorrectMethodInvoked() {
        Object result = memcachedClient.get("my-key");

        assertThat(result).isNotNull();
        verify(client).get("my-key");
    }

    @Test
    public void whenSet_thenCorrectMethodInvoked() {
        memcachedClient.set("my-key", 12000, "my-value");

        verify(client).set("my-key", 12000, "my-value");
    }

    @Test
    public void whenTouch_thenCorrectMethodInvoked() throws ExecutionException, InterruptedException {
        memcachedClient.touch("my-key", 700);

        verify(client).touch("my-key", 700);
    }

    @Test
    public void whenDelete_thenCorrectMethodInvoked() {
        memcachedClient.delete("my-key");

        verify(client).delete("my-key");
    }

    @Test
    public void whenFlush_thenCorrectMethodInvoked() {
        memcachedClient.flush();

        verify(client).flush();
    }

    @Test
    public void whenIncr_thenCorrectMethodInvoked() {
        memcachedClient.incr("my-key", 2);

        verify(client).incr("my-key", 2);
    }

    @Test
    public void whenShutdown_thenCorrectMethodInvoked() {
        memcachedClient.shutdown();

        verify(client).shutdown();
    }
}