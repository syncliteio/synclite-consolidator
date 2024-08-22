package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class CopyColumn extends DML {

    public Column srcCol;
    public Column dstCol;
    public CopyColumn(Table tbl, Column srcCol, Column dstCol) {
        super(tbl, Collections.emptyList(), Collections.emptyList());
        this.operType = OperType.COPYCOLUMN;
        this.srcCol = srcCol;
        this.dstCol = dstCol;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.copyColumn(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCopyColumnSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
