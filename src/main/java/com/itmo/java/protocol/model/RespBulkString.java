package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;
    private final byte[] stringData;

    public RespBulkString(byte[] data) {
        this.stringData = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        if (stringData == null) {
            return null;
        }
        return new String(stringData);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (stringData == null) {
            os.write(String.valueOf(NULL_STRING_SIZE).getBytes());
        } else {
            os.write(String.valueOf(stringData.length).getBytes());
            os.write(CRLF);
            os.write(stringData);
        }
        os.write(CRLF);
    }
}
