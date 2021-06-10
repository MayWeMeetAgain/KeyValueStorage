package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

<<<<<<< HEAD
/**
 * Запись в БД, означающая удаление значения по ключу
 */
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;

    public RemoveDatabaseRecord(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
=======
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    @Override
    public byte[] getKey() {
        return new byte[0];
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public byte[] getValue() {
<<<<<<< HEAD
        return null;
=======
        return new byte[0];
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public long size() {
<<<<<<< HEAD
        return 8 + getKeySize();
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }

    @Override
    public boolean isValuePresented() {
        return false;
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
        return -1;
=======
        return 0;
>>>>>>> 44c7869 (Initial commit)
    }
}
