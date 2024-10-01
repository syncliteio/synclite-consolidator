/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.consolidator.schema;

import java.nio.file.Path;

import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CopyColumn;
import com.synclite.consolidator.oper.CopyTable;
import com.synclite.consolidator.oper.CreateDatabase;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public abstract class SQLGenerator {

	protected int dstIndex; 
	private static final class InstanceHolder {
		private static SQLGenerator[] INSTANCES = getInstanceInternal(); 
	}

	public static SQLGenerator getInstance(int dstIndex) {
		return InstanceHolder.INSTANCES[dstIndex];
	}

	protected SQLGenerator(int dstIndex) {
		this.dstIndex = dstIndex;
	}
    private static SQLGenerator[] getInstanceInternal() {    	
    	SQLGenerator[] INSTANCES = new SQLGenerator[ConfLoader.getInstance().getNumDestinations() + 1];
		for (int dstIndex = 1 ; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++ dstIndex) {
	        switch(ConfLoader.getInstance().getDstType(dstIndex)) {
	        case APACHE_ICEBERG:
	        	INSTANCES[dstIndex] = new ApacheIcebergSQLGenerator(dstIndex);
	        	break;	        	

	        case CLICKHOUSE:
	        	INSTANCES[dstIndex] = new ClickHouseSQLGenerator(dstIndex);
	        	break;	        	

	        case DUCKDB:
	        	INSTANCES[dstIndex] = new DuckDBSQLGenerator(dstIndex);
	        	break;

	        case FERRETDB:
	        	INSTANCES[dstIndex] = new MongoDBSQLGenerator(dstIndex);
	        	break;

	        case MONGODB:
	        	INSTANCES[dstIndex] = new MongoDBSQLGenerator(dstIndex);
	        	break;
	        	
	        case MYSQL:
	        	INSTANCES[dstIndex] = new MySQLSQLGenerator(dstIndex);
	        	break;

	        case MSSQL:
	        	INSTANCES[dstIndex] = new MSSQLSQLGenerator(dstIndex);
	        	break;

	        case POSTGRESQL:
	        	INSTANCES[dstIndex] = new PGSQLGenerator(dstIndex);
	        	break;

	        case SQLITE:
	            INSTANCES[dstIndex] = new SQLiteSQLGenerator(dstIndex);
	            break;
	        	
	        default:
	            throw new RuntimeException("Unsupported dst type : " +  ConfLoader.getInstance().getDstType(dstIndex));
	        }
		}
		return INSTANCES;		
    }

    public abstract String getInsertSQL(Insert insert);
	public abstract String getUpsertSQL(Upsert upsert);
	public abstract String getReplaceSQL(Replace replace);
    public abstract String getUpdateSQL(Update update);
    public abstract String getDeleteSQL(Delete delete);
    public abstract String getLoadFileSQL(LoadFile load);
	public abstract String getDeleteIfPredicateSQL(DeleteIfPredicate deleteIfPredicate);
    
    public abstract String getFileLoaderInsertSQL(Insert insert, Path csvFilePath);
    public abstract String getFileLoaderUpsertSQL(Upsert upsert, Path csvFilePath);
	public abstract String getFileLoaderReplaceSQL(Replace replace, Path csvFilePath);
    public abstract String getFileLoaderUpdateSQL(Update update, Path csvFilePath);
    public abstract String getFileLoaderDeleteSQL(Delete delete, Path csvFilePath);

    public abstract String getTruncateSQL(TruncateTable truncate);
    public abstract String getCreateTableSQL(CreateTable createTable);
    public abstract String getDropTableSQL(DropTable dropTable);
    public abstract String getRenameTableSQL(RenameTable renameTable);
    public abstract String getCopyTableSQL(CopyTable copyTable);
    public abstract String getAddColumnSQL(AddColumn addColumn);
    public abstract String getDropColumnSQL(DropColumn dropColumn);
    public abstract String getRenameColumnSQL(RenameColumn renameColumn);
    public abstract String getAlterColumnSQL(AlterColumn alterColumn);
    public abstract String getCopyColumnSQL(CopyColumn copyColumn);
    public abstract String getCreateDatabseSQL(CreateDatabase createDatabase);
    public abstract String getCreateSchemaSQL(CreateSchema createSchema);
    public abstract String getBeginTranSQL();
    public abstract String getCommitTranSQL();
    public abstract boolean supportsIfClause();
    public abstract boolean supportsIfClauseInAlterColumn();
	public abstract String getMinusSQL(Minus minus);
	public abstract String getCreateTempTableSql(Table tbl, TableID tmpTableID, String csvFilePath);
	public abstract String getCreateUpdateTempTableSql(Table tbl, TableID tmpTableID, String csvFilePath);
	public abstract String getDropTempTableSql(Table tbl, TableID tmpTableID);
	public abstract String getLoadIntoTempTableSql(TableID tmpTableID, Path csvFilePath);
	public abstract String getInsertFromTempTableSql(Table tbl, TableID tmpTableID);
	public abstract String getDeleteFromTempTableSql(Table tbl, TableID tmpTableID);
	public abstract String getUpdateFromTempTableSql(Table tbl, TableID tmpTableID);
	public abstract String getMergeFromTempTableSql(Table tbl, TableID tmpTableID);

    public String getTableNameSQL(TableID id) {
        //TableID id = tbl.id;
    	boolean prefixCatalog = ConfLoader.getInstance().getDstUseCatalogScopeResolution(dstIndex);
    	boolean prefixSchema = ConfLoader.getInstance().getDstUseSchemaScopeResolution(dstIndex);
    	
        StringBuilder builder = new StringBuilder();
        if (isDatabaseAllowed() && (id.database != null)) {
        	if (prefixCatalog) {
	            builder.append(quoteObjectNameIfNeeded(id.database));
	            builder.append(".");
        	}
        }
        if (isSchemaAllowed() && (id.schema != null)) {
        	if (prefixSchema) {
	        	builder.append(quoteObjectNameIfNeeded(id.schema));
	            builder.append(".");
        	}
        }
        builder.append(quoteObjectNameIfNeeded(id.table));
        return builder.toString();
    }
    
    private String quoteObjectNameIfNeeded(String item) {
    	if (ConfLoader.getInstance().getDstQuoteObjectNames(dstIndex)) {
    		return quote(item);
    	}  
    	return item;
    }

    private String quote(String item) {
    	return "\"" + item + "\""; 
    }
    
    public String getFullColumnNameSQL(TableID tblID, Column c) {
        return getTableNameSQL(tblID) + "." + getColumnNameSQL(c);
    }

    public String getColumnNameSQL(Column c) {
    	if (ConfLoader.getInstance().getDstQuoteColumnNames(dstIndex)) {
    		return quote(c.column);
    	} else if (c.column.matches(".*\\s+.*")) {
    		return quote(c.column);
    	}
        return c.column;
    }

    public String getDatabaseNameSQL(Table tbl) {
        return tbl.id.database;
    }

    public String getSchemaNameSQL(Table tbl) {
    	if (tbl.id.database != null) {
    		if (ConfLoader.getInstance().getDstUseCatalogScopeResolution(dstIndex)) {
    			return tbl.id.database + "." + tbl.id.schema;
    		} else {
    			return tbl.id.schema;
    		}
    	} else {
    		return tbl.id.schema;
    	}
    }

    public abstract String getCheckpointTableSelectSql(String deviceUUID, String deviceName, ConsolidatorDstTable dstControlTable);
    
	public abstract String getDatabaseExistsCheckSQL(Table tbl);

	public abstract String getSchemaExistsCheckSQL(Table tbl);

    public abstract String getTableExistsCheckSQL(Table tbl);

	public abstract String getColumnExistsCheckSQL(Table tbl, Column c);

    public abstract boolean isDatabaseAllowed();
    public abstract boolean isSchemaAllowed();
    public abstract boolean isPKUpdateAllowed();
    public abstract boolean isUpdateAllowed();
	public abstract boolean supportsUpsert();
	public abstract boolean supportsReplace();
    public abstract boolean supportsNullableColsInPK();

}
