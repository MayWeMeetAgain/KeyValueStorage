package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';
    private final List<RespObject> objects;

    public RespArray(RespObject... objects) {
        this.objects = new LinkedList<RespObject>();
        for (RespObject object : objects) {
            this.objects.add(object);
        }
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        List<String> stringObjects = new LinkedList<String>();
        objects.forEach(obj -> stringObjects.add(obj.asString()));
        return String.join(" ", stringObjects);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.size()).getBytes());
        os.write(CRLF);
        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}
