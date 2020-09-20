package it.filider.cache;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import net.spy.memcached.MemcachedClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@SuppressWarnings({"rawtypes"})
public class GuavaMemcachedTest {

    private final MemcachedClient memcachedClient = mock(MemcachedClient.class);
    private final int expiryTime = 300; // 5 minutes
    private final CacheLoader cacheLoader = mock(CacheLoader.class);

    private GuavaMemcached underTest;

    @BeforeEach
    void setUp() {
        underTest = new GuavaMemcached(memcachedClient, expiryTime, cacheLoader);
    }

    @Test
    void willReturnValueIfPresent() {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(value);

        // When
        Object result = underTest.getIfPresent(key);

        // Then
        assertThat(result).isEqualTo(value);
    }

    @Test
    void willReturnNull_whenValueNotPresentInCache() {
        // Given
        UUID key = UUID.randomUUID();
        when(memcachedClient.get(key.toString())).thenReturn(null);

        // When
        Object result = underTest.getIfPresent(key);

        // Then
        assertThat(result).isEqualTo(null);
    }

    @Test
    void willReturnValueFromCache_ifAlreadyPresent() throws ExecutionException {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(value);

        // When
        Object cachedValue = underTest.get(key);

        // Then
        assertThat(cachedValue).isEqualTo(value);
        verifyZeroInteractions(cacheLoader);
        verify(memcachedClient, never()).set(key.toString(), expiryTime, value);
    }

    @Test
    void willLoadValueAndSetInCache_ifNotPresentInCache() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(null);
        when(cacheLoader.load(key)).thenReturn(value);

        // When
        Object cachedValue = underTest.get(key);

        // Then
        assertThat(cachedValue).isEqualTo(value);
        verify(memcachedClient).set(key.toString(), expiryTime, value);
    }

    @Test
    void whenValueNotInCache_andLoaderFails_willThrowAnException() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(null);
        when(cacheLoader.load(key)).thenThrow(new IllegalStateException("Something went wrong."));

        // Then
        assertThatThrownBy(() -> underTest.get(key))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Something went wrong.");
        verify(memcachedClient, never()).set(key.toString(), expiryTime, value);
    }

    @Test
    void willReturnValueFromCache_ifAlreadyPresent_andNotUseProvidedLoader() throws ExecutionException {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(value);
        String loadedValue = "loaded value";
        Callable callable = () -> loadedValue;

        // When
        Object cachedValue = underTest.get(key, callable);

        // Then
        assertThat(cachedValue).isEqualTo(value);
        verifyZeroInteractions(cacheLoader);
        verify(memcachedClient, never()).set(key.toString(), expiryTime, value);
    }

    @Test
    void willLoadValueFromProvidedLoaderAndSetInCache_ifNotPresentInCache() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        when(memcachedClient.get(key.toString())).thenReturn(null);
        String loadedValue = "loaded value";
        Callable callable = () -> loadedValue;

        // When
        Object cachedValue = underTest.get(key, callable);

        // Then
        assertThat(cachedValue).isEqualTo(loadedValue);
        verify(memcachedClient).set(key.toString(), expiryTime, loadedValue);
    }

    @Test
    void whenValueNotInCache_andGivenLoaderFails_willThrowAnException() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(memcachedClient.get(key.toString())).thenReturn(null);
        Callable callable = () -> { throw new IllegalStateException("Something went wrong."); };

        // Then
        assertThatThrownBy(() -> underTest.get(key, callable))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Something went wrong.");
        verify(memcachedClient, never()).set(key.toString(), expiryTime, value);
    }

    @Test
    void willPutAnItemInTheCache() {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";

        // When
        underTest.put(key, value);

        // Then
        verify(memcachedClient).set(key.toString(), expiryTime, value);
    }

    @Test
    void willInvalidateAnItemFromTheCache() {
        // Given
        UUID key = UUID.randomUUID();

        // When
        underTest.invalidate(key);

        // Then
        verify(memcachedClient).delete(key.toString());
    }

    @Test
    void willRefreshValueInCache() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(cacheLoader.load(key)).thenReturn(value);

        // When
        underTest.refresh(key);

        // Then
        verify(memcachedClient).set(key.toString(), expiryTime, value);
    }

    @Test
    void wontRefreshValueInCache_ifLoaderThrowsAnException() throws Exception {
        // Given
        UUID key = UUID.randomUUID();
        String value = "value";
        when(cacheLoader.load(key)).thenThrow(new IllegalStateException("Something went wrong."));

        // When
        underTest.refresh(key);

        // Then
        verify(memcachedClient, never()).set(key.toString(), expiryTime, value);
    }

    @Test
    void willReturnTotalSizeOfCluster() {
        // Given
        SocketAddress node1 = mock(SocketAddress.class);
        SocketAddress node2 = mock(SocketAddress.class);
        Map<SocketAddress, Map<String, String>> clusterStats = new HashMap<>();
        clusterStats.put(node1, Map.of("curr_items", "35"));
        clusterStats.put(node2, Map.of("curr_items", "65"));
        when(memcachedClient.getStats()).thenReturn(clusterStats);

        // When
        long size = underTest.size();

        // Then
        assertThat(size).isEqualTo(100L);
    }

    @Test
    void willReturnTotalStatsFromClusterNodes() {
        // Given
        SocketAddress node1 = mock(SocketAddress.class);
        SocketAddress node2 = mock(SocketAddress.class);
        Map<SocketAddress, Map<String, String>> clusterStats = new HashMap<>();
        clusterStats.put(node1, Map.of("get_hits", "150", "get_misses", "70", "evictions", "2"));

        clusterStats.put(node2, Map.of("get_hits", "65", "get_misses", "50", "evictions", "1"));;
        when(memcachedClient.getStats()).thenReturn(clusterStats);

        // When
        CacheStats cacheStats = underTest.stats();

        // Then
        assertThat(cacheStats.hitCount()).isEqualTo(215L);
        assertThat(cacheStats.missCount()).isEqualTo(120L);
        assertThat(cacheStats.evictionCount()).isEqualTo(3L);
    }
}
