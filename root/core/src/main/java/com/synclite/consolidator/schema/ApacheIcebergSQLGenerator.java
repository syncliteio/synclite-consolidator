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

import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class ApacheIcebergSQLGenerator extends JDBCSQLGenerator {

	protected ApacheIcebergSQLGenerator(int dstIndex) {
		super(dstIndex);
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
	public boolean supportsIfClause() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean supportsIfClauseInAlterColumn() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
		// TODO Auto-generated method stub
		return "SHOW DATABASES LIKE '" + tbl.id.database + "'";
	}

	@Override
	public String getSchemaExistsCheckSQL(Table tbl) {		
		return "SELECT 1";
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SHOW TABLES IN " + tbl.id.database + " LIKE '" + tbl.id.table + "'";
	}

	@Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
		return "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + tbl.id.database + "'  AND table_name = '" + tbl.id.table + "' AND column_name = '" + c.column + "'";
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
	public boolean isPKUpdateAllowed() {
		return true;
	}
	
	@Override
	public boolean supportsUpsert() {
		return true;
	}

    @Override
    public String getAddColumnSQL(AddColumn addColumn) {
        return "ALTER TABLE " + getTableNameSQL(addColumn.tbl.id) + " ADD COLUMNS (" + 
        		(supportsIfClauseInAlterColumn() ? " IF NOT EXISTS " : "") + 
        		getColumnSchemaSQLNoConstraint(addColumn.columns.get(0)) + ")";
    }

    public boolean supportsNullableColsInPK() {
		return true;
	}

    @Override
    public String getCreateTableSQL(CreateTable createTable) {
    	StringBuilder builder = new StringBuilder();
    	
        builder.append("CREATE TABLE ");
        if (supportsIfClause()) {
        	builder.append(" IF NOT EXISTS ");
        }
        builder.append(getTableNameSQL(createTable.tbl.id));
        builder.append("(");
        boolean first = true;
        for (Column c : createTable.tbl.columns) {
            if (!first) {
                builder.append(", ");
            }
            builder.append(getColumnSchemaSQLNoConstraint(c));
            first = false;
        }
    	builder.append(") USING iceberg");
        return builder.toString();
    }
    


    //
    //For Apache Iceberg, use MERGE statement ( without insert clause) for update from temp table.
    //
	@Override
	public String getUpdateFromTempTableSql(Table tbl, TableID tmpTableID) {
        boolean includeAllColsInWhere = true;
        if (tbl.hasPrimaryKey()) {
            includeAllColsInWhere = false;
        }

        boolean first = true;
        StringBuilder setListBuilder = new StringBuilder();
        for (Column c : tbl.columns) {
        	if (!first) {
                setListBuilder.append(", ");
        	}
        	setListBuilder.append("");
            setListBuilder.append(getFullColumnNameSQL(tbl.id, c));
            setListBuilder.append(" = ");
            setListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "a");
            first = false;
        }

        first = true;
        StringBuilder whereListBuilder = new StringBuilder();
        if (includeAllColsInWhere) {
            for (Column c : tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
            	if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                	whereListBuilder.append(")");
                } else {
                  	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        } else {
            for (Column c : tbl.columns) {
                if (c.pkIndex == 0) {
                    continue;
                }
                if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                	whereListBuilder.append(")");
                } else { 
                   	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c) + "b");
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("MERGE INTO ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" USING ");
        builder.append(getTableNameSQL(tmpTableID));
        builder.append(" ON ");
        builder.append(whereListBuilder.toString());
        builder.append(" WHEN MATCHED THEN UPDATE SET ");
        builder.append(setListBuilder.toString());
        return builder.toString();
	}

}
