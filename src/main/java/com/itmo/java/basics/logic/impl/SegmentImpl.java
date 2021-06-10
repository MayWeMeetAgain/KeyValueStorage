package com.itmo.java.basics.logic.impl;

<<<<<<< HEAD
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
=======
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
import com.itmo.java.basics.exceptions.DatabaseException;
>>>>>>> 99f644e (Lab1 (#1))
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private final String segmentName;
    private final Path segmentRoot;
<<<<<<< HEAD
    private SegmentIndex index;
=======
    private final SegmentIndex index;
>>>>>>> 99f644e (Lab1 (#1))
    private long freeSize;
    private boolean isReadOnly;
    private static final long SEGMENT_SIZE = 100_000;

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.segmentRoot = tableRootPath;
        index = new SegmentIndex();

        freeSize = SEGMENT_SIZE;
        isReadOnly = false;
    }

<<<<<<< HEAD
    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex index, long freeSize, boolean isReadOnly) {
        this.segmentName = segmentName;
        this.segmentRoot = tableRootPath;
        this.index = index;
        this.freeSize = freeSize;
        this.isReadOnly = isReadOnly;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
=======
    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
>>>>>>> 99f644e (Lab1 (#1))
        try {
            Files.createFile(Paths.get(tableRootPath.toString(), segmentName));
        } catch (FileAlreadyExistsException e) {
            throw new DatabaseException(String.format("Segment with name %s at path %s already exist", segmentName, tableRootPath.toString()), e);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO Exception while creating segment %s at path %s", segmentName, tableRootPath.toString()), e);
        }

        return new SegmentImpl(segmentName, tableRootPath);
<<<<<<< HEAD
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        boolean isReadOnly = false; 
        long freeSize = SEGMENT_SIZE - context.getCurrentSize();

        if (freeSize <= 0) {
            isReadOnly = true;
        }
        return new SegmentImpl(context.getSegmentName(), context.getSegmentPath().getParent(), context.getIndex(), freeSize, isReadOnly);
=======
>>>>>>> 99f644e (Lab1 (#1))
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectValue == null) {
            return delete(objectKey);
        }
            SetDatabaseRecord sRecord = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        return putInFile(sRecord, new SegmentOffsetInfoImpl(SEGMENT_SIZE - freeSize));
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> offset = index.searchForKey(objectKey);

        if (offset.isEmpty()) {
            return Optional.empty();
        }
        try (DatabaseInputStream inStream = new DatabaseInputStream(new FileInputStream(getSegmentPath()))) {
            if (offset.get().getOffset() != inStream.skip(offset.get().getOffset())) {
                throw new IOException("Offset doesn't equal for the number of skipped bytes");
            }
            Optional<DatabaseRecord> record = inStream.readDbUnit();

            if (record.isEmpty()) {
               return Optional.empty();
            }
            return Optional.of(record.get().getValue());
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        RemoveDatabaseRecord rRecord = new RemoveDatabaseRecord(objectKey.getBytes());
        return putInFile(rRecord, null);
    }

    private boolean putInFile(WritableDatabaseRecord wRecord, SegmentOffsetInfoImpl offset) throws IOException {
        if (isReadOnly) {
            return false;
        }
        if (wRecord.size() >= freeSize) {
            isReadOnly = true;
        }
        try (DatabaseOutputStream outStream = new DatabaseOutputStream(new FileOutputStream(getSegmentPath(), true))) {
            outStream.write(wRecord);
        }
        index.onIndexedEntityUpdated(new String(wRecord.getKey()), offset);
        freeSize -=  wRecord.size();

        return true;                
    }

    private String getSegmentPath() {
        return Paths.get(segmentRoot.toString(), segmentName).toString();
    }
}
