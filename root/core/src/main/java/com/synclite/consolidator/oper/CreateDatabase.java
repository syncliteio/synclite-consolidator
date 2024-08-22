package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class CreateDatabase extends DDL {

    public CreateDatabase(Table tbl) {
        super(tbl, Collections.EMPTY_LIST);
        this.operType = OperType.CREATEDATABASE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.createDatabase(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCreateDatabseSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
