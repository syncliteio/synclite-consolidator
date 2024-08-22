package com.synclite.consolidator.schema;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.oper.DDL;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.processor.SQLExecutor;

public class CommitTran extends DDL {

    public CommitTran() {
        super(null, Collections.EMPTY_LIST);
        this.operType = OperType.COMMITTRAN;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.commitTran();
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCommitTranSQL();
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
