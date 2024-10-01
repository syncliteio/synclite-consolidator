package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class MSSQLSQLGenerator extends JDBCSQLGenerator {

	protected MSSQLSQLGenerator(int dstIndex) {
        super(dstIndex);
    }
	
	@Override
    public String getSchemaNameSQL(Table tbl) {
        return tbl.id.schema;
    }

    @Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		return "SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(CATALOG_NAME) = '" + tbl.id.database.toLowerCase() + "'";		
	}

    @Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(CATALOG_NAME) = '" + tbl.id.database.toLowerCase() + "' AND LOWER(SCHEMA_NAME) = '" + tbl.id.schema.toLowerCase() + "'";		
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_CATALOG) = '" + tbl.id.database.toLowerCase() + "' AND LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "' AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'";		
	}
    
    @Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
    	return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_CATALOG) = '" + tbl.id.database.toLowerCase() + "' AND LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "' AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'" + " AND LOWER(COLUMN_NAME) = '" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean supportsIfClause() {
		return false;
	}

	@Override
	public boolean isDatabaseAllowed() {
		return true;
	}

	@Override
	public boolean isSchemaAllowed() {
		return true;
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
    public String getAddColumnSQL(AddColumn addColumn) {
        return "ALTER TABLE " + getTableNameSQL(addColumn.tbl.id) + " ADD " + 
        		(supportsIfClauseInAlterColumn() ? " IF NOT EXISTS " : "") + 
        		getColumnSchemaSQL(addColumn.columns.get(0));
    }

    @Override
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " ALTER COLUMN " +        		
        		getColumnNameSQL(alteredColumn.columns.get(0)) + " " + getColumnTypeSQLNoConstraint(alteredColumn.columns.get(0));   
    }

    @Override
    public String getUpsertSQL(Upsert upsert) {
    	StringBuilder colList = new StringBuilder();
    	StringBuilder sourceColList = new StringBuilder();
    	StringBuilder placeHolderList = new StringBuilder();
    	StringBuilder pkEqualities = new StringBuilder();
    	StringBuilder nonPKEqualities = new StringBuilder();
    	
    	boolean firstKey = true;
    	boolean firstNonKey = true;
    	
    	for(int i=0 ; i < upsert.tbl.columns.size() ; ++i) {
    		Column c = upsert.tbl.columns.get(i);
    		if (i > 0) {
    			colList.append(",");
    			sourceColList.append(",");
    			placeHolderList.append(",");
    		}
    		colList.append(getColumnNameSQL(c));
    		sourceColList.append("source.");
    		sourceColList.append(getColumnNameSQL(c));
    		placeHolderList.append("?");
    		
    		if(c.pkIndex > 0 ) {    			
    			if (!firstKey) {
    				pkEqualities.append(" AND ");
    			}
                if (c.isNotNull > 0) {
                	pkEqualities.append("(target." + getColumnNameSQL(c) + " = " + "source." + getColumnNameSQL(c) + ")");
                } else {
                	pkEqualities.append("((target." + getColumnNameSQL(c) + " IS NULL AND source." + getColumnNameSQL(c) + " IS NULL) OR (target." + getColumnNameSQL(c) + " = " + "source." + getColumnNameSQL(c) + "))");
                }
    			firstKey = false;
    		} else {
    			if (!firstNonKey) {
    				nonPKEqualities.append(",");
    			}
    			nonPKEqualities.append("target." + getColumnNameSQL(c) + " = " + "source." + getColumnNameSQL(c));
    			firstNonKey = false;    			
    		}    	
    	}

    	/*
   	 String mergeQuery = "MERGE INTO t1 AS target "
                + "USING (VALUES (?, ?, ?)) AS source (a, b, c) "
                + "ON (target.a = source.a) "
                + "WHEN MATCHED THEN "
                + "  UPDATE SET target.b = source.b, target.c = source.c "
                + "WHEN NOT MATCHED THEN "
                + "  INSERT (a, b, c) VALUES (source.a, source.b, source.c);";
   	 */

    	StringBuilder upsertBuilder = new StringBuilder();
    	upsertBuilder.append("MERGE INTO ");
    	upsertBuilder.append(getTableNameSQL(upsert.tbl.id));
    	upsertBuilder.append(" AS target USING (VALUES(");
    	upsertBuilder.append(placeHolderList.toString());
    	upsertBuilder.append(")) AS source(");
    	upsertBuilder.append(colList.toString());
    	upsertBuilder.append(") ON (");
    	upsertBuilder.append(pkEqualities.toString());
    	if (!nonPKEqualities.toString().isBlank()) {
    		upsertBuilder.append(") WHEN MATCHED THEN UPDATE SET ");
        	upsertBuilder.append(nonPKEqualities.toString());
    	} else {
    		upsertBuilder.append(")");
    	}
    	upsertBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
    	upsertBuilder.append(colList.toString());
    	upsertBuilder.append(") VALUES (");
    	upsertBuilder.append(sourceColList.toString());
    	upsertBuilder.append(");");
    	
    	return upsertBuilder.toString();
    	
    }


	@Override
	public boolean supportsUpsert() {
		return true;
	}

}
