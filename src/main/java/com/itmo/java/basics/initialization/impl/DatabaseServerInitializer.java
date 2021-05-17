package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        ExecutionEnvironment env = context.executionEnvironment();
        Path serverPath = env.getWorkingPath();

        if (!Files.exists(serverPath)) {
           try {
               Files.createDirectory(serverPath);
           } catch (IOException e) {
                throw new DatabaseException(String.format("Creating %s directory failed by IOException", env.getWorkingPath().toString(), e));
           } 
        }
        File server = new File(serverPath.toString());

        if (!server.isDirectory()) {
            throw new DatabaseException(serverPath.toString() + "is a file, expected for directory");
        }

        for (File db : server.listFiles()) {
            var dbContext = new DatabaseInitializationContextImpl(db.getName(), serverPath);
            databaseInitializer.perform(InitializationContextImpl.builder()
                                            .executionEnvironment(env)
                                            .currentDatabaseContext(dbContext).build());
        }

    }
}
