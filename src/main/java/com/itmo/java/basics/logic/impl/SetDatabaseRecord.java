package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

<<<<<<< HEAD
/**
 * Запись в БД, означающая добавление значения по ключу
 */
public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
=======
public class SetDatabaseRecord implements WritableDatabaseRecord {

    @Override
    public byte[] getKey() {
        return new byte[0];
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public byte[] getValue() {
<<<<<<< HEAD
        return value;
=======
        return new byte[0];
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public long size() {
<<<<<<< HEAD
        return 8 + getKeySize() + getValueSize();
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public boolean isValuePresented() {
<<<<<<< HEAD
        return true;
=======
        return false;
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public int getKeySize() {
<<<<<<< HEAD
        return key.length;
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public int getValueSize() {
<<<<<<< HEAD
        return value.length;
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }
}
