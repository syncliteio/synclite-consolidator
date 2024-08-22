package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class DropColumn extends DDL {

    public DropColumn(Table tbl, Column column) {
        super(tbl, Collections.singletonList(column));
        this.operType = OperType.DROPCOLUMN;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        tbl.dropColumnIfExists(columns.get(0));
        executor.dropColumn(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getDropColumnSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }

}
