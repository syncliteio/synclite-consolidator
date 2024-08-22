package com.synclite.consolidator.oper;

import java.nio.file.Path;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class Update extends DML {
	private boolean applyIdempotently;
    public Update(Table tbl, List<Object> beforeValues, List<Object> afterValues) {
        super(tbl, beforeValues, afterValues);
        this.operType = OperType.UPDATE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.update(this);
    }

    @Override
    public boolean isBatchable() {
        return true;
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getUpdateSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }

    public boolean getApplyIdempotently() {
    	return applyIdempotently;
    }

    public String getFileLoaderSQL(SQLGenerator generator, Path csvFilePath) {
    	return generator.getFileLoaderUpdateSQL(this, csvFilePath);
    }

}

