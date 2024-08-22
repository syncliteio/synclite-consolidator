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

public class Insert extends DML {

	private boolean applyInsertidempotently;
    public Insert(Table tbl, List<Object> values, boolean applyIdempotently) {
        super(tbl, Collections.EMPTY_LIST, values);
        this.applyInsertidempotently = applyIdempotently;
        this.operType = OperType.INSERT; 
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.insert(this);
    }

    @Override
    public boolean isBatchable() {
        return true;
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getInsertSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }


    @Override
    public String getFileLoaderSQL(SQLGenerator generator, Path csvFilePath) {
    	return generator.getFileLoaderInsertSQL(this, csvFilePath);
    }

	public boolean applyInsertIdempotently() {
		return this.applyInsertidempotently;
	}
}
