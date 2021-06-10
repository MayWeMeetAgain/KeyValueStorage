package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
<<<<<<< HEAD
     * - Размер ключа в байтах используя {@link WritableDatabaseRecord#getKeySize()}
=======
     * - Размер ключа в байтахб используя {@link WritableDatabaseRecord#getKeySize()}
>>>>>>> 44c7869 (Initial commit)
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
<<<<<<< HEAD
        writeInt(databaseRecord.getKeySize());
        write(databaseRecord.getKey());
        writeInt(databaseRecord.getValueSize());
        if (databaseRecord.isValuePresented()) {
            write(databaseRecord.getValue());
        }
        return (int)databaseRecord.size();
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }
}