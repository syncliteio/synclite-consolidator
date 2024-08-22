package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class NoOp extends Oper {

    public NoOp(Table tbl) {
        super(tbl);
        this.operType = OperType.NOOP;
    }

    @Override
    public void execute(SQLExecutor executor) {
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return null;
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
