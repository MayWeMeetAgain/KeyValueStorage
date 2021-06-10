package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache cache;

    private static final int CACHE_CAPACITY = 5_000;  
    
    public CachingTable(Table table) {
        this.table = table;
        cache = new DatabaseCacheImpl(CACHE_CAPACITY);
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<byte[]> value = Optional.ofNullable(cache.get(objectKey));
        if (value.isPresent()) {
            return value;
        }
        return table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }
}
