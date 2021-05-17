package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.nio.file.Path;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;
    
    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        var dbContext = initialContext.currentDbContext();
        Path dbPath = dbContext.getDatabasePath();
        File db = new File(dbPath.toString());

        if (!db.exists()) {
            throw new DatabaseException(String.format("Directory %s doesn't exist", dbPath.toString()));
        }

        if (!db.isDirectory()) {
            throw new DatabaseException(dbPath.toString() + "is a file, expected for directory");
        }

        for (File table : db.listFiles()) {
            var tableContext = new TableInitializationContextImpl(table.getName(), dbPath, new TableIndex());

            tableInitializer.perform(InitializationContextImpl.builder()
                                        .currentDatabaseContext(dbContext)
                                            .currentTableContext(tableContext).build());
        }

        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(dbContext));
    }
}
