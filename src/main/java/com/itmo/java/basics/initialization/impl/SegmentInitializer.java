package com.itmo.java.basics.initialization.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var segmentContext = context.currentSegmentContext();
        Path segmentPath = segmentContext.getSegmentPath();
        SegmentIndex segmentIndex = segmentContext.getIndex();
        long currentSize = 0;
        List<String> listOfKeys = new LinkedList<>();

        if (!Files.exists(segmentPath)) {
            throw new DatabaseException(String.format("Segment %s doesn't exist", segmentPath));
        }

        try (DatabaseInputStream inStream = new DatabaseInputStream(new FileInputStream(segmentPath.toString()))) {
            Optional<DatabaseRecord> record;
            for (record = inStream.readDbUnit(); record.isPresent(); record = inStream.readDbUnit()) {
                segmentIndex.onIndexedEntityUpdated(new String(record.get().getKey()), new SegmentOffsetInfoImpl(currentSize));
                currentSize += record.get().size();
                listOfKeys.add(new String(record.get().getKey()));
            }
        } catch (IOException e) {
            throw new DatabaseException("Initialising segment failed because of IOException", e);
        }

        Segment segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
                segmentContext.getSegmentName(), segmentContext.getSegmentPath(), currentSize, segmentIndex));
        
        for (String key : listOfKeys) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
        }
        context.currentTableContext().updateCurrentSegment(segment);
    }
}
