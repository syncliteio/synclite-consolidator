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

public class AlterColumn extends DDL {

    public AlterColumn(Table tbl, Column alteredColumn) {
        super(tbl, Collections.singletonList(alteredColumn));
        this.operType = OperType.ALTERCOLUMN;
    }

	@Override
	public void execute(SQLExecutor executor) throws DstExecutionException {
        tbl.replaceColumnIfExists(columns.get(0));
        executor.alterColumn(this);
	}

	@Override
	public String getSQL(SQLGenerator generator) {
        return generator.getAlterColumnSQL(this);
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
	}

}
