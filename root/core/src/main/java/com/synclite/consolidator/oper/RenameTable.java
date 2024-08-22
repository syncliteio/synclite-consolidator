package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class RenameTable extends DDL {

    public Table oldTable;
    public Table newTable;
    public RenameTable(Table tbl, Table oldTable, Table newTable) {
        super(tbl, Collections.EMPTY_LIST);
        this.operType = OperType.RENAMETABLE;
        this.oldTable = oldTable;
        this.newTable = newTable;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.renameTable(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getRenameTableSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
