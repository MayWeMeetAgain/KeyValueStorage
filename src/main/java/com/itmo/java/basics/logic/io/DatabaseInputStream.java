package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int SIZE_OF_REMOVED = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        try {
            byte[] key = new byte[readInt()];
            read(key);
            int valueSize = readInt();
            if (valueSize == SIZE_OF_REMOVED) {
                return Optional.empty();
            }
            byte[] value = new byte[valueSize];
            read(value);
            WritableDatabaseRecord wRecord = new SetDatabaseRecord(key, value); 
            return Optional.of(wRecord);
        } catch (EOFException e) {
            return Optional.empty();
        }
    }
}
