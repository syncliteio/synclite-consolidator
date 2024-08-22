package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.Table;

public abstract class DDL extends Oper {

    public List<Column> columns;
    public DDL(Table tbl, List<Column> columns) {
        super(tbl);
        this.columns = columns;
    }

}
