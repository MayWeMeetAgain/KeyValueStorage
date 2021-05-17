package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var tableContext = context.currentTableContext();
        Path tablePath = tableContext.getTablePath();
        File table = new File(tablePath.toString());

        if (!table.exists()) {
            throw new DatabaseException(String.format("Directory %s doesn't exist", tablePath.toString()));
        }

        if (!table.isDirectory()) {
            throw new DatabaseException(tablePath.toString() + "is a file, expected for directory");
        }

        var segmentList = table.listFiles();
        Arrays.sort(segmentList);

        for (File segment : segmentList) {
            var segmentContext = new SegmentInitializationContextImpl(segment.getName(), tablePath, 0);

            segmentInitializer.perform(InitializationContextImpl.builder()
                                        .currentTableContext(tableContext)
                                        .currentSegmentContext(segmentContext).build());
        }

        context.currentDbContext().addTable(TableImpl.initializeFromContext(tableContext));
    }
}
