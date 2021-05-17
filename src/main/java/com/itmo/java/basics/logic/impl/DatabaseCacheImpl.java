package com.itmo.java.basics.logic.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import com.itmo.java.basics.logic.DatabaseCache;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;
    private Map<String, byte[]> cache;

    private class CachingMap<T, V> extends LinkedHashMap<T, V> {
        private int capacity;

        public CachingMap(int capacity) {
            super(capacity, 1f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<T, V> eldest) {
            return size() > capacity;
        }
    }

    public DatabaseCacheImpl(int capacity) {
        cache = new CachingMap<String, byte[]>(capacity);
    }

    @Override
    public byte[] get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
