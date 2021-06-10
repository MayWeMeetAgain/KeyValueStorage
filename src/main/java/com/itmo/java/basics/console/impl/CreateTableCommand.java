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
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private static final int NUM_OF_ARG = 4;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs == null) {
            throw new NullPointerException("Arguments list for CreateTable command is null");
        }
        if (commandArgs.size() != NUM_OF_ARG) {
            throw new IllegalArgumentException(String.format("Wrong number of arguments for CreateTable, expected: %d, given: %d", NUM_OF_ARG, commandArgs.size()));
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
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        
        try {
            env.getDatabase(dbName).get().createTableIfNotExists(tableName);
            return DatabaseCommandResult.success((String.format("Table %s in database %s created successfully", tableName, dbName)).getBytes());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(String.format("Can't create table %s in database %s, because %s", tableName, dbName, e.getMessage()));
        } catch (NoSuchElementException e) {
            return DatabaseCommandResult.error(String.format("Database %s doesn't exist", dbName));
        }
    }
}
