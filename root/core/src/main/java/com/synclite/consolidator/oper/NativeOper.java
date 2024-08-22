package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class NativeOper extends Oper {

	String stmt;
	public NativeOper(Table tbl, String stmt) {
		super(tbl);
		this.stmt = stmt;
	}

	@Override
    public boolean isBatchable() {
        return false;
    }
	
	@Override
	public void execute(SQLExecutor executor) throws DstExecutionException {
		executor.executeSQL(this);
	}

	@Override
	public String getSQL(SQLGenerator generator) {
		return stmt;
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
		return Collections.singletonList(this);
	}

}
