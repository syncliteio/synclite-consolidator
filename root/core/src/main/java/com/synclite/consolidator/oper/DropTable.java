package com.synclite.consolidator.oper;

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class DropTable extends DDL {

    public DropTable(Table tbl) {
        super(tbl, Collections.EMPTY_LIST);
        this.operType = OperType.DROPTABLE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.dropTable(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getDropTableSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        // TODO Auto-generated method stub
        return tableMapper.mapOper(this);
    }

}
