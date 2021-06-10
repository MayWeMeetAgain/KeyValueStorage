package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
<<<<<<< HEAD
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
=======
>>>>>>> 99f644e (Lab1 (#1))
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
<<<<<<< HEAD
            WritableDatabaseRecord record; 

=======
>>>>>>> 99f644e (Lab1 (#1))
            byte[] key = new byte[readInt()];
            read(key);
            int valueSize = readInt();
            if (valueSize == SIZE_OF_REMOVED) {
<<<<<<< HEAD
                record = new RemoveDatabaseRecord(key);
            } else {
                byte[] value = new byte[valueSize];
                read(value);
                record = new SetDatabaseRecord(key, value); 
            }
            return Optional.of(record);
=======
                return Optional.empty();
            }
            byte[] value = new byte[valueSize];
            read(value);
            WritableDatabaseRecord wRecord = new SetDatabaseRecord(key, value); 
            return Optional.of(wRecord);
>>>>>>> 99f644e (Lab1 (#1))
        } catch (EOFException e) {
            return Optional.empty();
        }
    }
}
