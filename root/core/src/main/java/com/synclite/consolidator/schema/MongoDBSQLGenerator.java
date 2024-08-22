package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class MongoDBSQLGenerator extends JDBCSQLGenerator {

	protected MongoDBSQLGenerator(int dstIndex) {
		super(dstIndex);
	}

	@Override
	public boolean supportsIfClause() {
		return true;
	}

	@Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public boolean isDatabaseAllowed() {
		return true;
	}

	@Override
	public boolean isSchemaAllowed() {
		return false;
	}

	@Override
	public boolean supportsIfClauseInAlterColumn() {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getFileLoaderInsertSQL(Insert insert, Path csvFilePath) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getFileLoaderUpsertSQL(Upsert upsert, Path csvFilePath) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getFileLoaderReplaceSQL(Replace replace, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderUpdateSQL(Update update, Path csvFilePath) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public String getFileLoaderDeleteSQL(Delete delete, Path csvFilePath) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
	}

	@Override
	public boolean isPKUpdateAllowed() {
		return true;
	}

    @Override
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
		throw new UnsupportedOperationException("Unsupported Operation in MongoDB");
    }

    @Override
    public boolean supportsNullableColsInPK() {
		return true;
	}

    @Override
    public boolean supportsUpsert() {
		return true;
	}

    @Override
    public boolean supportsReplace() {
		return true;
	}

}

