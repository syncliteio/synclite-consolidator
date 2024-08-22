package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class CopyTable extends DML {

    public Table copyFromTable;
    public CopyTable(Table tbl, Table copyFromTable) {
        super(tbl, Collections.emptyList(), Collections.emptyList());
        this.operType = OperType.COPYTABLE;
        this.copyFromTable = copyFromTable;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.copyTable(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCopyTableSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
