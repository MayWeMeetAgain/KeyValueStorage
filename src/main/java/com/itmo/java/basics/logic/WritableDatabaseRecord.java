package com.itmo.java.basics.logic;

/**
 * Содержит информацию о параметрах {@link DatabaseRecord} для хранения в БД
 */
public interface WritableDatabaseRecord extends DatabaseRecord {

    /**
     * Возвращает размер ключа в байтах
     */
    int getKeySize();


    /**
<<<<<<< HEAD
     * Возвращает размер значения в байтах. -1, если значение отсутствует
=======
     * Возвращает размер значения в байтах. -1, если значение отсутвует
>>>>>>> 44c7869 (Initial commit)
     */
    int getValueSize();
}
