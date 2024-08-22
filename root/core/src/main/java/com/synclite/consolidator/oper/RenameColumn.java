package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class RenameColumn extends DDL {

    public String oldName;
    public String newName;
    public RenameColumn(Table tbl, Column column, String oldName, String newName) {
        super(tbl, Collections.singletonList(column));
        this.operType = OperType.RENAMECOLUMN;
        this.newName = newName;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        this.tbl.renameColumn(this.columns.get(0), newName);
        executor.renameColumn(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getRenameColumnSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
