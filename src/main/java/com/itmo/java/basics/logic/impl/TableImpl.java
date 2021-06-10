package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
<<<<<<< HEAD
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String tableName;
    private final Path tableRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment; 

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.tableRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
        this.currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), getTablePath());
    } 

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            Files.createDirectory(Paths.get(pathToDatabaseRoot.toString(), tableName));
        } catch (FileAlreadyExistsException e) {
            throw new DatabaseException(String.format("Table with name %s at path %s already exist", tableName, pathToDatabaseRoot.toString()), e);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO Exception while creating table %s at path %s", tableName, pathToDatabaseRoot.toString()), e);
        }

        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
    }
    @Override
    public String getName() {
        return tableName;
=======
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return null;
    }

    @Override
    public String getName() {
        return null;
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
<<<<<<< HEAD
        try {
            if (!currentSegment.write(objectKey, objectValue)) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), getTablePath());
                currentSegment.write(objectKey, objectValue);                     
            }
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("Writing value %s by key %s failed because of IO exception", objectValue.toString(), objectKey), e);
        }
=======

>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
<<<<<<< HEAD
        Optional<Segment> oSegment = tableIndex.searchForKey(objectKey);
        if (oSegment.isEmpty()) {
            return Optional.empty();
        }
        try { 
            return oSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Reading value by key" + objectKey + "failed because of IO exception", e);
        }
=======
        return Optional.empty();
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
<<<<<<< HEAD
        try {
            if (!currentSegment.delete(objectKey)) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), getTablePath());
                currentSegment.delete(objectKey);                     
            }
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException("Deleting by key" + objectKey + "failed because of IO exception", e);
        }
    }

    private Path getTablePath() {
        return Paths.get(tableRoot.toString(), tableName);
=======

>>>>>>> 44c7869 (Initial commit)
    }
}
