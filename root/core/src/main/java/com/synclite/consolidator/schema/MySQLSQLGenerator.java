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

import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class MySQLSQLGenerator extends JDBCSQLGenerator {

	protected MySQLSQLGenerator(int dstIndex) {
        super(dstIndex);
    }
	
	@Override
    public String getSchemaNameSQL(Table tbl) {
        return tbl.id.schema;
    }

    @Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		return "SELECT 1";		
	}

    @Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(SCHEMA_NAME) = '" + tbl.id.schema.toLowerCase() + "'";		
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "' LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'";		
	}
    
    @Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
    	return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_SCHEMA) ='" + tbl.id.schema.toLowerCase() + "' AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'" + " AND LOWER(COLUMN_NAME) = '" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean supportsIfClause() {
		return true;
	}

	@Override
	public boolean isDatabaseAllowed() {
		return false;
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
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " MODIFY COLUMN " +        		
        		getColumnNameSQL(alteredColumn.columns.get(0)) + " " + getColumnTypeSQLNoConstraint(alteredColumn.columns.get(0));   
    }

    
    @Override
    public String getUpsertSQL(Upsert upsert) {
        StringBuilder colList = new StringBuilder();
        StringBuilder placeHolderList = new StringBuilder();
        StringBuilder updateList = new StringBuilder();

        for (int i = 0; i < upsert.tbl.columns.size(); ++i) {
            Column c = upsert.tbl.columns.get(i);
            if (i > 0) {
                colList.append(",");
                placeHolderList.append(",");
                updateList.append(",");
            }
            colList.append(getColumnNameSQL(c));
            placeHolderList.append("?");
            updateList.append(getColumnNameSQL(c) + "=VALUES(" + getColumnNameSQL(c) + ")");
        }

        StringBuilder upsertBuilder = new StringBuilder();
        upsertBuilder.append("INSERT INTO ");
        upsertBuilder.append(getTableNameSQL(upsert.tbl.id));
        upsertBuilder.append(" (");
        upsertBuilder.append(colList.toString());
        upsertBuilder.append(") VALUES (");
        upsertBuilder.append(placeHolderList.toString());
        upsertBuilder.append(") ON DUPLICATE KEY UPDATE ");
        upsertBuilder.append(updateList.toString());

        return upsertBuilder.toString();
    }

	@Override
	public boolean supportsUpsert() {
		return true;
	}

}
