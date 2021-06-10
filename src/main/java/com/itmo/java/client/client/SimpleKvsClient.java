package com.itmo.java.client.client;

import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.DeleteKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.KvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        try {
            return getResponseAndCheck(new CreateDatabaseKvsCommand(databaseName));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Can't create database %s", databaseName), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        try {
            return getResponseAndCheck(new CreateTableKvsCommand(databaseName, tableName));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Can't create table %s in database %s", tableName, databaseName), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        try {
            return getResponseAndCheck(new GetKvsCommand(databaseName, tableName, key));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Can't get value by key %s in table %s in database %s", key, tableName, databaseName), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        try {
            return getResponseAndCheck(new SetKvsCommand(databaseName, tableName, key, value));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Can't set value %s by key %s in table %s in database %s", value, key, tableName, databaseName), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        try {
            return getResponseAndCheck(new DeleteKvsCommand(databaseName, tableName, key));
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Can't delete key %s in table %s in database %s", key, tableName, databaseName), e);
        }
    }

    private String getResponseAndCheck(KvsCommand command) throws ConnectionException, DatabaseExecutionException {
        RespObject result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        if (result.isError()) {
            throw new DatabaseExecutionException(result.asString());
        }
        return result.asString();
    }
}
