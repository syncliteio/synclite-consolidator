package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class FinishBatch extends Oper {

    public FinishBatch(Table tbl) {
    	super(tbl);
    	this.operType = OperType.FINISHBATCH;
    }

	@Override
	public void execute(SQLExecutor executor) throws DstExecutionException {
		executor.executeSQL(this);
	}

	@Override
    public boolean isBatchable() {
        return false;
    }

	@Override
	public String getSQL(SQLGenerator generator) {
		return "FINISHBATCH";
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
		return tableMapper.mapOper(this);
	}

}
