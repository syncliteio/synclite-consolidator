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
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public class DuckDBSQLGenerator extends JDBCSQLGenerator {


    protected DuckDBSQLGenerator(int dstIndex) {
		super(dstIndex);
	}

	@Override
	public String getDatabaseExistsCheckSQL(Table tbl) {
    	//Database always exists as DuckDB is a file database
    	return "SELECT 1";
	}

    @Override
	public String getSchemaExistsCheckSQL(Table tbl) {
		return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(SCHEMA_NAME) = '" + tbl.id.schema.toLowerCase() + "'";		
	}

	@Override
	public String getTableExistsCheckSQL(Table tbl) {
		return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "'" + " AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'";		
	}
    
    @Override
	public String getColumnExistsCheckSQL(Table tbl, Column c) {
    	return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_SCHEMA) = '" + tbl.id.schema.toLowerCase() + "'" + " AND LOWER(TABLE_NAME) = '" + tbl.id.table.toLowerCase() + "'" + " AND LOWER(COLUMN_NAME) = '" + c.column.toLowerCase() + "'";
	}

	@Override
	public boolean supportsIfClause() {
		//DuckDB keeps throwing write-write cache conflict when two concurrent CREATE TABLE IF NOT EXISTS statements run 
		//
		//return true;
		//Returning false for now
		return false;
	}
	
    @Override
    public String getCreateSchemaSQL(CreateSchema createSchema) {
        return "CREATE SCHEMA " +
        		(supportsIfClause() ? " IF NOT EXISTS " : "") + 
        		createSchema.tbl.id.schema;
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
		return "COPY " + getTableNameSQL(insert.tbl.id) + " FROM '" + csvFilePath + "' WITH (HEADER 1, DELIMITER ',')";
		//return "INSERT INTO 
	}

	public String getFileLoaderUpsertSQL(Upsert upsert, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderReplaceSQL(Replace replace, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderUpdateSQL(Update update, Path csvFilePath) {
		Path csvFileName = csvFilePath.getFileName();
		
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ");
        builder.append(getTableNameSQL(update.tbl.id));
        builder.append(" SET ");
        boolean first = true;
        StringBuilder setListBuilder = new StringBuilder();
        StringBuilder whereListBuilder = new StringBuilder();
        int idx = update.tbl.columns.size() + 1;
        for (Column c : update.tbl.columns) {
            if (!first) {
                setListBuilder.append(", ");
            }
            setListBuilder.append(getColumnNameSQL(c));
            setListBuilder.append(" = stg." + getColumnNameSQL(c) + "b");
            first = false;
            ++idx;
        }

        boolean includeAllColsInWhere = true;
        if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
            if (update.tbl.hasPrimaryKey()) {
                includeAllColsInWhere = false;
            }
        }

        if (includeAllColsInWhere) {
            first = true;
            idx = 1;
            for (Column c : update.tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
            	if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = stg." + getColumnNameSQL(c) + "b");
                } else {
                    whereListBuilder.append("(("+ "stg." + getColumnNameSQL(c) + "b IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = "+ "stg." + getColumnNameSQL(c) +"b))");
                }
                first = false;
                ++idx;
            }
        } else {
            first = true;
            idx = 1;
            for (Column c : update.tbl.columns) {
            	if (c.pkIndex != 0) {
            		if (!first) {
            			whereListBuilder.append(" AND ");
            		}
            		if (c.isNotNull > 0) {
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" = " + "stg." + getColumnNameSQL(c) + "b");
            		} else {
            			whereListBuilder.append("(("+ "stg." + getColumnNameSQL(c) + "b IS NULL AND ");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" IS NULL) OR (");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" = "+ "stg." +  getColumnNameSQL(c) + "b ))");
            		}
            		first = false;
            	}
            	++idx;
            }
        }

        builder.append(setListBuilder.toString());
        builder.append(" FROM '" + csvFilePath + "' as stg");
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());

        return builder.toString();


	}

	@Override
	public String getFileLoaderDeleteSQL(Delete delete, Path csvFilePath) {
		Path csvFileName = csvFilePath.getFileName();
		
        StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ");
        builder.append(getTableNameSQL(delete.tbl.id));
        builder.append(" USING ");
        StringBuilder whereListBuilder = new StringBuilder();
        boolean includeAllColsInWhere = true;
        if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
            if (delete.tbl.hasPrimaryKey()) {
                includeAllColsInWhere = false;
            }
        }

        if (includeAllColsInWhere) {
            boolean first = true;
            long idx = 1;
            for (Column c : delete.tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
                if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                    whereListBuilder.append(getFullColumnNameSQL(delete.tbl.id, c));
                    whereListBuilder.append(" = stg." + getColumnNameSQL(c));
                } else {
                    whereListBuilder.append("(("+ getFullColumnNameSQL(delete.tbl.id, c) +" IS NULL AND ");
                    whereListBuilder.append("stg." + getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getFullColumnNameSQL(delete.tbl.id, c));
                    whereListBuilder.append(" = "+ "stg." + getColumnNameSQL(c) +"))");
                }
                first = false;
                ++idx;
            }
        } else {
            boolean first = true;
            long idx = 1;
            for (Column c : delete.tbl.columns) {
            	if (c.pkIndex != 0) {
            		if (!first) {
            			whereListBuilder.append(" AND ");
            		}
            		if (c.isNotNull > 0) {
            			whereListBuilder.append(getFullColumnNameSQL(delete.tbl.id, c));
            			whereListBuilder.append(" = " + "stg." + getColumnNameSQL(c));
            		} else {
            			whereListBuilder.append("(("+ "stg." + getColumnNameSQL(c) +" IS NULL AND ");
            			whereListBuilder.append(getFullColumnNameSQL(delete.tbl.id, c));
            			whereListBuilder.append(" IS NULL) OR (");
            			whereListBuilder.append(getFullColumnNameSQL(delete.tbl.id, c));
            			whereListBuilder.append(" = "+ "stg." +  getColumnNameSQL(c) + "))");
            		}
            		first = false;
            	}
            	++idx;
            }
        }

        builder.append("'" + csvFilePath +"' as stg");
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());

        return builder.toString();
	}

	@Override
	public boolean isPKUpdateAllowed() {
		return false;
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
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " ALTER COLUMN " +        		
        		getColumnNameSQL(alteredColumn.columns.get(0)) + " SET DATA TYPE " + getColumnTypeSQLNoConstraint(alteredColumn.columns.get(0));   
    }

}
