package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class PGSQLGenerator extends JDBCSQLGenerator {

    protected PGSQLGenerator(int dstIndex) {
        super(dstIndex);
    }

    @Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		return "SELECT datname FROM pg_database WHERE LOWER(datname) = '" + tbl.id.database.toLowerCase() + "'";		
	}

    @Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(CATALOG_NAME) = '" + tbl.id.database.toLowerCase() + "' AND LOWER(SCHEMA_NAME) = '" + tbl.id.schema.toLowerCase() + "'";		
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "'" + " AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'";		
	}
    
    @Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
    	return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "'" + " AND TABLE_NAME = '" + tbl.id.table + "'" + " AND LOWER(COLUMN_NAME) = '" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean supportsIfClause() {
		return true;
	}

	
    @Override
    public String getCreateSchemaSQL(CreateSchema createSchema) {
        return "CREATE SCHEMA " +
        		(supportsIfClause() ? " IF NOT EXISTS " : "") + 
        		createSchema.tbl.id.schema;
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
		return true;
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
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " ALTER COLUMN " +        		
        		getColumnNameSQL(alteredColumn.columns.get(0)) + " TYPE " + getColumnTypeSQLNoConstraint(alteredColumn.columns.get(0));   
    }
    
    @Override
    public String getUpsertSQL(Upsert upsert) {
        StringBuilder colList = new StringBuilder();
        StringBuilder placeHolderList = new StringBuilder();
        StringBuilder updateList = new StringBuilder();
    	StringBuilder pkColList = new StringBuilder();
    	boolean firstKey = true;
        for (int i = 0; i < upsert.tbl.columns.size(); ++i) {
            Column c = upsert.tbl.columns.get(i);
            if (i > 0) {
                colList.append(",");
                placeHolderList.append(",");
                updateList.append(",");
            }
            colList.append(getColumnNameSQL(c));
            placeHolderList.append("?");
            updateList.append(getColumnNameSQL(c) + "=EXCLUDED." + getColumnNameSQL(c));
            
    		if(c.pkIndex > 0 ) {    			
    			if (!firstKey) {
    				pkColList.append(",");
    			}
				pkColList.append(getColumnNameSQL(c));
    			firstKey = false;
    		}
        }

        StringBuilder upsertBuilder = new StringBuilder();
        upsertBuilder.append("INSERT INTO ");
        upsertBuilder.append(getTableNameSQL(upsert.tbl.id));
        upsertBuilder.append(" (");
        upsertBuilder.append(colList.toString());
        upsertBuilder.append(") VALUES (");
        upsertBuilder.append(placeHolderList.toString());
        upsertBuilder.append(") ON CONFLICT (");
        upsertBuilder.append(pkColList.toString());
        upsertBuilder.append(") DO UPDATE SET ");
        upsertBuilder.append(updateList.toString());

        return upsertBuilder.toString();
    }

	@Override
	public boolean supportsUpsert() {
		return true;
	}

}
