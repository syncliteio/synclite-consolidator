package com.synclite.consolidator.oper;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class Delete extends DML {
    public Delete(Table tbl, List<Object> beforeValues) {
        super(tbl, beforeValues, Collections.EMPTY_LIST);
        this.operType = OperType.DELETE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.delete(this);
    }

    @Override
    public boolean isBatchable() {
        return true;
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getDeleteSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }

    public String getFileLoaderSQL(SQLGenerator generator, Path csvFilePath) {
    	return generator.getFileLoaderDeleteSQL(this, csvFilePath);
    }

}
