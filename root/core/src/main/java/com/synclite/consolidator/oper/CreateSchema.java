package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class CreateSchema extends DDL {


    public CreateSchema(Table tbl) {
        super(tbl, Collections.EMPTY_LIST);
        this.operType = OperType.CREATESCHEMA;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.createSchema(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCreateSchemaSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
