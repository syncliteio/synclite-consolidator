package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class AddColumn extends DDL {

    public AddColumn(Table tbl, Column newColumn) {
        super(tbl, Collections.singletonList(newColumn));
        this.operType = OperType.ADDCOLUMN;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        tbl.addColumnIfNotExists(columns.get(0));
        executor.addColumn(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getAddColumnSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
