package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class DeleteIfPredicate extends Oper {

	public String sql;
	public String predicate;
    public DeleteIfPredicate(Table tbl, String sql) {
        super(tbl);
        this.sql = sql;
        parsePredicate();
        this.operType = OperType.DELETE_IF_PREDICATE;
    }

    public DeleteIfPredicate(ConsolidatorDstTable tbl, String sql, String mappedPredicate) {
        super(tbl);
        this.sql = sql;
        this.predicate = mappedPredicate;
        this.operType = OperType.DELETE_IF_PREDICATE;
	}

	private final void parsePredicate() {
    	 int whereIndex = this.sql.toLowerCase().indexOf("where");

         if (whereIndex != -1) {
             this.predicate = sql.substring(whereIndex + 5).trim();
             //Remove semicolon if any
             predicate = predicate.replaceAll(";$", "");
         } else {
        	 //If unable to find then make it a NOP
             this.predicate = "1 = 0";
         }
	}

	@Override
    public boolean isBatchable() {
        return false;
    }

	@Override
	public void execute(SQLExecutor executor) throws DstExecutionException {
		executor.executeSQL(this);
	}

	@Override
	public String getSQL(SQLGenerator generator) {
		return generator.getDeleteIfPredicateSQL(this);
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
		return tableMapper.mapOper(this);
	}

}
