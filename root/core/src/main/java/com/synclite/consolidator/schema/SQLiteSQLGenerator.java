package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class SQLiteSQLGenerator extends JDBCSQLGenerator {

	protected SQLiteSQLGenerator(int dstIndex) {
		super(dstIndex);
	}

	@Override
	public boolean supportsIfClause() {
		return true;
	}

	@Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		return "SELECT 1";
	}

	@Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		// TODO Auto-generated method stub
		return "SELECT 1";
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)='" + tbl.id.table + "'";
	}

	@Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
		// TODO Auto-generated method stub
		return "SELECT name FROM pragma_table_info('" + tbl.id.table + "') WHERE lower(name)='" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean isDatabaseAllowed() {
		return false;
	}

	@Override
	public boolean isSchemaAllowed() {
		return false;
	}

	@Override
	public boolean supportsIfClauseInAlterColumn() {
		return false;
	}

	@Override
	public String getFileLoaderInsertSQL(Insert insert, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderUpsertSQL(Upsert upsert, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderReplaceSQL(Replace replace, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderUpdateSQL(Update update, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderDeleteSQL(Delete delete, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isPKUpdateAllowed() {
		return true;
	}

    @Override
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
    	//SQLITE does not support alter column.
    	//
    	return "SELECT 1";
    }

    @Override
    public boolean supportsNullableColsInPK() {
		return true;
	}

}

