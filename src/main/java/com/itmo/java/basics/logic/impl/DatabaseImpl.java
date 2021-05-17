package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path dbRoot;
    private final Map<String, Table> tables;

    private DatabaseImpl (String dbName, Path dbRoot) {
        this.dbName = dbName;
        this.dbRoot = dbRoot;
        tables = new HashMap<String, Table>();
    }

    private DatabaseImpl (String dbName, Path dbRoot, Map<String, Table> tables) {
        this.dbName = dbName;
        this.dbRoot = dbRoot;
        this.tables = tables;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName.isEmpty() || dbName == null) {
            throw new DatabaseException("Given database name is empty or null");
        }
        try {
            Files.createDirectory(Paths.get(databaseRoot.toString(), dbName));
        } catch (FileAlreadyExistsException e) {
            throw new DatabaseException(String.format("Database with name %s at path %s already exist", dbName, databaseRoot.toString()), e);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO Exception while creating database %s at path %s", dbName, databaseRoot.toString()), e);
        }
        
        return new DatabaseImpl(dbName, databaseRoot);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context.getDbName(), context.getDatabasePath().getParent(), context.getTables());
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName.isEmpty() || tableName == null ) {
            throw new DatabaseException("Given table name is empty or null");   
        }
        Table curTable = TableImpl.create(tableName, Paths.get(dbRoot.toString(), dbName), new TableIndex());
        tables.put(tableName, curTable);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        searchTable(tableName).write(objectKey, objectValue);  
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        return searchTable(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        searchTable(tableName).delete(objectKey);
    }

    private Table searchTable(String tableName) throws DatabaseException {
        Table requiredTable = tables.get(tableName);
        
        if (requiredTable == null) {
            throw new DatabaseException("Database doesn't have table with name" + tableName);
        }
        return requiredTable;
    }

}
