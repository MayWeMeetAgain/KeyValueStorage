package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private static final int NUM_OF_ARG = 6;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs == null) {
            throw new NullPointerException("Arguments list for SetKey command is null");
        }
        if (commandArgs.size() != NUM_OF_ARG) {
            throw new IllegalArgumentException(String.format("Wrong number of arguments for SetKey, expected: %d, given: %d", NUM_OF_ARG, commandArgs.size()));
        }
        for (RespObject arg : commandArgs) {
            if (arg == null) {
                throw new IllegalArgumentException("Some arguments are null");
            }
        }

        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        byte[] value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes();
        byte[] previous;
        
        try {
            previous = env.getDatabase(dbName).get().read(tableName, key).get();
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(String.format("Reading from storage cell with parameters: %s, %s, %s failed, because %s", dbName, tableName, key, e.getMessage()));
        } catch (NoSuchElementException e) {
            previous = null;
        }

        try {    
            env.getDatabase(dbName).get().write(tableName, key, value);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(String.format("Writing to storage cell with parameters: %s, %s, %s failed, because %s", dbName, tableName, key, e.getMessage()));
        } catch (NoSuchElementException e) {
            return DatabaseCommandResult.error(String.format("There is no database %s", dbName));
        }

        return DatabaseCommandResult.success(previous);
    }
}
