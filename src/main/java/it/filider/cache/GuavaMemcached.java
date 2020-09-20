package it.filider.cache;

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import net.spy.memcached.MemcachedClient;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class GuavaMemcached<K, V> extends AbstractLoadingCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(GuavaMemcached.class);
    private final MemcachedClient memcachedClient;
    private final int expiryTimeInSeconds;
    private final CacheLoader<K, V> defaultLoader;

    public GuavaMemcached(MemcachedClient memcachedClient,
                          int expiryTimeInSeconds,
                          CacheLoader<K, V> defaultLoader) {
        this.memcachedClient = memcachedClient;
        this.expiryTimeInSeconds = expiryTimeInSeconds;
        this.defaultLoader = defaultLoader;
    }

    public V get(K key) throws ExecutionException {
        V cachedValue = (V) this.getIfPresent(key);
        if (cachedValue == null) {
            try {
                cachedValue = defaultLoader.load(key);
                memcachedClient.set(key.toString(), expiryTimeInSeconds, cachedValue);
            } catch (Exception e) {
                logger.error("Something went wrong fetching value for key {} ", key.toString());
                throw new ExecutionException(e);
            }
        }
        return cachedValue;
    }

    @Nullable
    public V getIfPresent(Object key) {
        return (V) memcachedClient.get(key.toString());
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        V cachedValue = (V) this.getIfPresent(key);
        if (cachedValue == null) {
            try {
                cachedValue = valueLoader.call();
                memcachedClient.set(key.toString(), expiryTimeInSeconds, cachedValue);
            } catch (Exception e) {
                logger.error("Something went wrong fetching value for key {} ", key.toString());
                throw new ExecutionException(e);
            }
        }
        return cachedValue;
    }

    @Override
    public void put(K key, V value) {
        // set command, if the key is already present it will add it on top of LRU
        memcachedClient.set(key.toString(), expiryTimeInSeconds, value);
    }

    @Override
    public void invalidate(Object key) {
        memcachedClient.delete(key.toString());
    }

    @Override
    public void refresh(K key) {
        try {
            V value = defaultLoader.load(key);
            memcachedClient.set(key.toString(), expiryTimeInSeconds, value);
        } catch (Exception e) {
            logger.error("Something went wrong refreshing value for key {}.", key.toString(), e);
        }
    }

    @Override
    public long size() {
        // Get stats from all the nodes for all the clusters that this Memcached client is connected to
        Collection<Map<String, String>> stats = memcachedClient.getStats().values();

        // Sum up all the nodes sizes will provide the overall size of the cluster
        AtomicInteger currentItems = new AtomicInteger(0);
        for (Map<String, String> nodeStats : stats) {
            currentItems.addAndGet(Integer.parseInt(nodeStats.get("curr_items")));
        }

        return currentItems.get();
    }

    @Override
    public CacheStats stats() {
        // Get stats from all the nodes for all the clusters that this Memcached client is connected to
        Collection<Map<String, String>> stats = memcachedClient.getStats().values();

        // Sum up all the relevant stats from all the nodes
        AtomicInteger hits = new AtomicInteger(0);
        AtomicInteger misses = new AtomicInteger(0);
        AtomicInteger evictions = new AtomicInteger(0);
        for (Map<String, String> nodeStats : stats) {
            hits.addAndGet(Integer.parseInt(nodeStats.get("get_hits")));
            misses.addAndGet(Integer.parseInt(nodeStats.get("get_misses")));
            evictions.addAndGet(Integer.parseInt(nodeStats.get("evictions")));
        }

        return new CacheStats(hits.get(), misses.get(), 0L, 0L, 0L, evictions.get());
    }

}
