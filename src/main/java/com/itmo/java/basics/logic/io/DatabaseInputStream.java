package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
<<<<<<< HEAD
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
=======

import java.io.DataInputStream;
>>>>>>> 44c7869 (Initial commit)
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
<<<<<<< HEAD
    private static final int SIZE_OF_REMOVED = -1;
=======
    private static final int REMOVED_OBJECT_SIZE = -1;
>>>>>>> 44c7869 (Initial commit)

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
<<<<<<< HEAD
        try {
            WritableDatabaseRecord record; 

            byte[] key = new byte[readInt()];
            read(key);
            int valueSize = readInt();
            if (valueSize == SIZE_OF_REMOVED) {
                record = new RemoveDatabaseRecord(key);
            } else {
                byte[] value = new byte[valueSize];
                read(value);
                record = new SetDatabaseRecord(key, value); 
            }
            return Optional.of(record);
        } catch (EOFException e) {
            return Optional.empty();
        }
=======
        return null;
>>>>>>> 44c7869 (Initial commit)
    }
}
