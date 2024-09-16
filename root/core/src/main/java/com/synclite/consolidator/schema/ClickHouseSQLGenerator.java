package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class ClickHouseSQLGenerator extends JDBCSQLGenerator {

	protected ClickHouseSQLGenerator(int dstIndex) {
        super(dstIndex);
    }

    @Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
    	//in Db2, we are connected to the specific database only hence return a dummy sql;
		return "SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(CATALOG_NAME) = '" + tbl.id.database.toLowerCase() + "'";		
	}

    @Override
	public String getSchemaExistsCheckSQL(Table tbl) {
        return "SELECT 1";
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(CATALOG_NAME) = '" + tbl.id.database.toLowerCase() + "' LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'";		
	}
    
    @Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
    	return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_CATALOG) ='" + tbl.id.database.toLowerCase() + "' AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'" + " AND LOWER(COLUMN_NAME) = '" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean supportsIfClause() {
		return true;
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
	public String getLoadFileSQL(LoadFile loadFile) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(getTableNameSQL(loadFile.tbl.id));
        builder.append(" (");
        boolean first = true;
        StringBuilder insertColListBuilder = new StringBuilder();
        for (Column c : loadFile.tbl.columns) {
            if (!first) {
                insertColListBuilder.append(", ");
            }
            insertColListBuilder.append(getColumnNameSQL(c));
            first = false;
        }
        builder.append(insertColListBuilder.toString());
        builder.append(") SELECT ");
        
        first = false;
        
        StringBuilder selectColListBuilder = new StringBuilder();
        for (Column c : loadFile.tbl.columns) {
            if (!first) {
                selectColListBuilder.append(", ");
            }
            
            if (loadFile.defaultValues.containsKey(c.column)) {
            	selectColListBuilder.append("'");
            	selectColListBuilder.append(loadFile.defaultValues.get(c.column));
            	selectColListBuilder.append("' AS ");
            	selectColListBuilder.append(c.column);
            } else {
            	selectColListBuilder.append(getColumnNameSQL(c));
            }
            first = false;
        }
        
        builder.append(selectColListBuilder.toString());        
        builder.append(" FROM read_csv(");
        builder.append("'" + loadFile.dataFilePath + "'");
        builder.append(", DELIM ='" + loadFile.delimiter + "'");
        builder.append(", HEADER ='" + loadFile.header + "'");
        builder.append(", NULLSTR ='" + loadFile.nullString + "'");
        builder.append(", QUOTE ='" + loadFile.quote + "'");
        builder.append(", ESCAPE ='" + loadFile.escape + "'");
        builder.append(")");
        
        return builder.toString();
	}

	@Override
	public boolean isPKUpdateAllowed() {
		return true;
	}

    @Override
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " MODIFY COLUMN " +        		
        		getColumnNameSQL(alteredColumn.columns.get(0)) + " " + getColumnTypeSQLNoConstraint(alteredColumn.columns.get(0));   
    }

    @Override
    public String getAddColumnSQL(AddColumn addColumn) {
        return "ALTER TABLE " + getTableNameSQL(addColumn.tbl.id) + " ADD COLUMN IF NOT EXISTS " +        		
        		getColumnNameSQL(addColumn.columns.get(0)) + " " + getColumnTypeSQLNoConstraint(addColumn.columns.get(0)) + " " + getColumnDefaultConstraint(addColumn.columns.get(0));   
    }

    @Override
    public boolean isUpdateAllowed() {
    	return false;
	}
    
    @Override
    public String getCreateTableSQL(CreateTable createTable) {
    	StringBuilder createTableSqlBuilder = new StringBuilder();
    	String createTableSql = super.getCreateTableSQL(createTable);
    	createTableSqlBuilder.append(createTableSql);
    	createTableSqlBuilder.append(" Engine = " + ConfLoader.getInstance().getDstClickHouseEngine(this.dstIndex));
    	if (!createTable.tbl.hasPrimaryKey()) {
    		createTableSqlBuilder.append(" ORDER BY " + getColumnNameSQL(createTable.tbl.columns.get(0)));
    	} else {
    		createTableSqlBuilder.append(" ORDER BY (" + getPKColList(createTable.tbl) + ")");
    	}
    	return createTableSqlBuilder.toString();
    }    
        
    
    @Override
	public String getDeleteIfPredicateSQL(DeleteIfPredicate deleteIfPredicate) {
    	if (deleteIfPredicate.tbl.hasPrimaryKey()) {
    		//
    		//We need to delete all records satisfying this deletePredicate.Since clickhouse maintains all variants of each primary key inserted/updated so far
    		//we need to first select all PK values satisfying delete predicate and then actually delete the records.
    		//
    		String pkColList = getPKColList(deleteIfPredicate.tbl);
    		//String sql = "DELETE FROM " + getTableNameSQL(deleteIfPredicate.tbl.id) + " WHERE (" + pkColList + ") IN (SELECT " + pkColList + " FROM " + getTableNameSQL(deleteIfPredicate.tbl.id) + " WHERE " + deleteIfPredicate.predicate + ")";
    		//String sql = "ALTER TABLE " + getTableNameSQL(deleteIfPredicate.tbl.id) + " DELETE WHERE " + deleteIfPredicate.predicate;
    		String sql = "ALTER TABLE " + getTableNameSQL(deleteIfPredicate.tbl.id) + " DELETE WHERE (" + pkColList + ") IN (SELECT " + pkColList + " FROM " + getTableNameSQL(deleteIfPredicate.tbl.id) + " WHERE " + deleteIfPredicate.predicate + ")";

    		return sql;
    	} else {
    		return super.getDeleteIfPredicateSQL(deleteIfPredicate);
    	}
	}
    
	@Override
	public String getMinusSQL(Minus minus) {
    	if (minus.tbl.hasPrimaryKey()) {
    		String pkColList = getPKColList(minus.tbl);
    		String sql = "ALTER TABLE " + getTableNameSQL(minus.tbl.id) + " DELETE WHERE (" + pkColList + ") NOT IN (SELECT " + pkColList + " FROM " + getTableNameSQL(minus.rhsTable.id) + ")";
    		return sql;
    	} else {
    		return "SELECT 1";
    	}
	}

}
