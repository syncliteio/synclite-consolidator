package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class TruncateTable extends DML {

    public TruncateTable(Table tbl) {
        super(tbl, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        this.operType = OperType.TRUNCATE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.truncate(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getTruncateSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
