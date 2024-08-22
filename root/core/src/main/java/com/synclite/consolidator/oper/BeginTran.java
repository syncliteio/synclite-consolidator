package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.TableMapper;

public class BeginTran extends DDL {

    public BeginTran() {
        super(null, Collections.EMPTY_LIST);
        this.operType = OperType.BEGINTRAN;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.beginTran();
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getBeginTranSQL();
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
