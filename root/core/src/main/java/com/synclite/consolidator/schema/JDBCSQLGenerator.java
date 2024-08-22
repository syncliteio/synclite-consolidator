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

public abstract class JDBCSQLGenerator extends SQLGenerator {

    protected JDBCSQLGenerator(int dstIndex) {
    	super(dstIndex);
    }
    
    protected String doGetInsertSQL(Table tbl) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" (");
        boolean first = true;
        StringBuilder colListBuilder = new StringBuilder();
        StringBuilder colValueBuilder = new StringBuilder();
        for (Column c : tbl.columns) {
            if (!first) {
                colListBuilder.append(", ");
                colValueBuilder.append(", ");
            }
            colListBuilder.append(getColumnNameSQL(c));
            colValueBuilder.append("?");
            first = false;
        }
        builder.append(colListBuilder.toString());
        builder.append(") VALUES(");
        builder.append(colValueBuilder.toString());
        builder.append(")");
        return builder.toString();    	
    }
    
    @Override
    public String getInsertSQL(Insert insert) {
    	return doGetInsertSQL(insert.tbl);
    }

    @Override
    public String getUpdateSQL(Update update) {
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ");
        builder.append(getTableNameSQL(update.tbl.id));
        builder.append(" SET ");
        boolean first = true;
        StringBuilder setListBuilder = new StringBuilder();
        StringBuilder whereListBuilder = new StringBuilder();
        for (Column c : update.tbl.columns) {
            if (!first) {
                setListBuilder.append(", ");
            }
            setListBuilder.append(getColumnNameSQL(c));
            setListBuilder.append(" = ?");
            first = false;
        }

        boolean includeAllColsInWhere = true;
        if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
            if (update.tbl.hasPrimaryKey()) {
                includeAllColsInWhere = false;
            }
        } else if (update.tbl.getIsSystemTable()){
        	//
        	//For system table we must use only PK cols in where list 
        	//as we do not have before value of synclite_update_timestamp column
        	//
        	includeAllColsInWhere = false;
        }                   

        if (includeAllColsInWhere) {
            first = true;
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
                    whereListBuilder.append(" = ?");
                } else {
                    whereListBuilder.append("((? IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?))");
                }
                first = false;
            }
        } else {
            first = true;
            for (Column c : update.tbl.columns) {
                if (c.pkIndex == 0) {
                    continue;
                }
                if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?");
                } else {
                    whereListBuilder.append("((? IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?))");
                }
                first = false;
            }
        }

        builder.append(setListBuilder.toString());
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());

        return builder.toString();
    }
    
    @Override
    public String getDeleteSQL(Delete delete) {
        StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ");
        builder.append(getTableNameSQL(delete.tbl.id));
        builder.append(" WHERE ");

        boolean includeAllColsInWhere = true;
        if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
            if (delete.tbl.hasPrimaryKey()) {
                includeAllColsInWhere = false;
            }
        }

        boolean first = true;
        StringBuilder whereListBuilder = new StringBuilder();
        if (includeAllColsInWhere) {
            for (Column c : delete.tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
            	if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?");
                } else {
                    whereListBuilder.append("((? IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?))");
                }
                first = false;
            }
        } else {
            for (Column c : delete.tbl.columns) {
                if (c.pkIndex == 0) {
                    continue;
                }
                if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?");
                } else {
                    whereListBuilder.append("((? IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = ?))");
                }
                first = false;
            }
        }
        builder.append(whereListBuilder.toString());
        return builder.toString();
    }

    @Override
	public String getDeleteIfPredicateSQL(DeleteIfPredicate deleteIfPredicate) {
		return "DELETE FROM " + getTableNameSQL(deleteIfPredicate.tbl.id) + " WHERE " + deleteIfPredicate.predicate;
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
            builder.append(getColumnSchemaSQL(c));
            first = false;
        }
    	//
        //Add PK constraint only if PK update is allowed on the DB
        //DuckDB does not allow PK update hence skip specifying this clause.
        //We intentionally kept PK info in the dstTable object since we want to support idempotent 
        //insertions for databases like DuckDB so that the where clause based deletes and inserts will work.
        //
        //Added another condition to not actually try create a PK if dst does not allow nullable cols in PK and PK has nullablke cols.
        //This was added for databases like SQL Server
        //
        
        boolean generatePK = false;
        if (createTable.tbl.hasPrimaryKey()) {
	        if (isPKUpdateAllowed()) {
	        	//Check if PK have nullable columns and if dst supports nullable columns in PK        	
	        	if (pkHasNullableCols(createTable.tbl)) {
	        		if (supportsNullableColsInPK()) {
	        			generatePK = true;
	        		}
	        	} else {
	        		generatePK = true;
	        	}
	        }
        }
        if (generatePK == true) {
        	builder.append(getPrimaryKeySQL(createTable.tbl));
        }

        builder.append(")");
        
        //if not a system table then append create table suffix
        if (!createTable.tbl.getIsSystemTable()) {
	        String createTableSuffix = ConfLoader.getInstance().getDstCreateTableSuffix(dstIndex);
	        if (! createTableSuffix.isBlank()) {
	        	builder.append(" ").append(createTableSuffix);
	        }
        }
        return builder.toString();
    }

    @Override
    public boolean supportsNullableColsInPK() {
		return false;
	}

	protected boolean pkHasNullableCols(Table tbl) {
        for (Column c : tbl.columns) {
            if (c.pkIndex > 0) {
            	if (c.isNotNull == 0) {
            		return true;
            	}
            }
        }
        return false;
	}


	protected String getPKColList(Table tbl) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Column c : tbl.columns) {
            if (c.pkIndex > 0) {
                if (!first) {
                    builder.append(", ");
                }
                builder.append(getColumnNameSQL(c));
                first = false;
            }
        }
        String pkList = builder.toString();
        return pkList;
    }
    
    protected String getPrimaryKeySQL(Table tbl) {
    	if (!tbl.hasPrimaryKey()) {
    		return "";
    	}
    	String pkList = getPKColList(tbl);
        if (pkList.isEmpty()) {
            return pkList;
        } else {
            return ", PRIMARY KEY(" + pkList + ")";
        }
    }

    @Override
    public String getAddColumnSQL(AddColumn addColumn) {
        return "ALTER TABLE " + getTableNameSQL(addColumn.tbl.id) + " ADD COLUMN " + 
        		(supportsIfClauseInAlterColumn() ? " IF NOT EXISTS " : "") + 
        		getColumnSchemaSQL(addColumn.columns.get(0));
    }

    @Override
    public String getAlterColumnSQL(AlterColumn alteredColumn) {
        return "ALTER TABLE " + getTableNameSQL(alteredColumn.tbl.id) + " ALTER COLUMN " +        		
        		getColumnSchemaSQL(alteredColumn.columns.get(0));   
    }

    @Override
    public String getDropTableSQL(DropTable dropTable) {
        return "DROP TABLE " + 
        		(supportsIfClause() ? " IF EXISTS " : "") + 
        		getTableNameSQL(dropTable.tbl.id);
    }

    @Override
    public String getRenameTableSQL(RenameTable renameTable) {
        return "ALTER TABLE " + getTableNameSQL(renameTable.oldTable.id) + " RENAME TO " + renameTable.newTable.id.table;
    }

    @Override
    public String getDropColumnSQL(DropColumn dropColumn) {
        return "ALTER TABLE " + getTableNameSQL(dropColumn.tbl.id) + " DROP COLUMN " +
        		(supportsIfClauseInAlterColumn() ? " IF EXISTS " : "") +
        		getColumnNameSQL(dropColumn.columns.get(0));
    }

    @Override
    public String getRenameColumnSQL(RenameColumn renameColumn) {
        return "ALTER TABLE " + getTableNameSQL(renameColumn.tbl.id) + " RENAME COLUMN " + getColumnNameSQL(renameColumn.columns.get(0)) + " TO " + renameColumn.newName;
    }

    protected String getColumnTypeSQLNoConstraint(Column c) {
    	return c.type.dbNativeDataType;
    }

    protected String getColumnDefaultConstraint(Column c) {
    	if (c.defaultValue != null) {
    		return " DEFAULT '" +  c.defaultValue + "'";
    	}
    	return "";
    }

    protected String getColumnTypeSQL(Column c) {
        StringBuilder builder = new StringBuilder();
    	builder.append(c.type.dbNativeDataType);
        builder.append(" ");
        if (c.isNotNull == 0) {
            builder.append(" NULL ");
        } else {
            builder.append(" NOT NULL ");
        }
        if (c.defaultValue != null) {
            builder.append(" DEFAULT '");
            builder.append(c.defaultValue);
            builder.append("'");
        }
        return builder.toString();
    }
    
    protected String getColumnSchemaSQL(Column c) {
        StringBuilder builder = new StringBuilder();
        builder.append(getColumnNameSQL(c));
        builder.append(" ");
        builder.append(getColumnTypeSQL(c));
        return builder.toString();
    }

    protected String getColumnSchemaSQLNoConstraint(Column c) {
        StringBuilder builder = new StringBuilder();
        builder.append(getColumnNameSQL(c));
        builder.append(" ");
        builder.append(getColumnTypeSQLNoConstraint(c));
        return builder.toString();
    }

    @Override
    public String getCreateDatabseSQL(CreateDatabase createDatabase) {
        return "CREATE DATABASE " + getDatabaseNameSQL(createDatabase.tbl);
    }

    @Override
    public String getCreateSchemaSQL(CreateSchema createSchema) {
        return "CREATE SCHEMA " +
        		(supportsIfClause() ? " IF NOT EXISTS " : "") + 
        		getSchemaNameSQL(createSchema.tbl);
    }

    @Override
    public String getCheckpointTableSelectSql(String deviceUUID, String deviceName, ConsolidatorDstTable dstControlTable) {
        String sql = "SELECT commit_id, cdc_change_number, cdc_txn_change_number, cdc_log_segment_sequence_number, txn_count FROM " + getTableNameSQL(dstControlTable.id);
        if (dstControlTable.hasColumn("synclite_device_id")) {
            sql += " WHERE synclite_device_id = '" + deviceUUID + "'";
        }

        if ((deviceName != null) && (!deviceName.isEmpty()) && (dstControlTable.hasColumn("synclite_device_name"))) {
            sql+= " AND synclite_device_name = '" + deviceName + "'";
        }
        return sql;
    }

    @Override
    public String getTruncateSQL(TruncateTable truncate) {
        String sql = "DELETE FROM " + getTableNameSQL(truncate.tbl.id);
        if (truncate.tbl.hasColumn("synclite_device_id")) {
            sql += " WHERE synclite_device_id = '" + truncate.tbl.id.deviceUUID + "'";
        }

        if (truncate.tbl.hasColumn("synclite_device_name")) {
            sql+= " AND synclite_device_name = '" + truncate.tbl.id.deviceName + "'";
        }

        return sql;
    }

    @Override
    public String getBeginTranSQL() {
        return "BEGIN";
    }

    @Override
    public String getCommitTranSQL() {
        return "COMMIT";
    }

    @Override
    public String getCopyTableSQL(CopyTable copyTable) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(getTableNameSQL(copyTable.tbl.id));
        builder.append(" (");
        boolean first = true;
        StringBuilder colListBuilder = new StringBuilder();
        for (Column c : copyTable.tbl.columns) {
            if (!first) {
                colListBuilder.append(", ");
            }
            colListBuilder.append(getColumnNameSQL(c));
            first = false;
        }
        builder.append(colListBuilder.toString());
        builder.append(") SELECT ");
        builder.append(colListBuilder.toString());
        builder.append(" FROM ");
        builder.append(getTableNameSQL(copyTable.copyFromTable.id));
        if (copyTable.tbl.hasColumn("synclite_device_id")) {
            builder.append(" WHERE synclite_device_id = '" + copyTable.tbl.id.deviceUUID + "'");
        }
        if (copyTable.tbl.hasColumn("synclite_device_name")) {
            builder.append(" AND synclite_device_name = '" + copyTable.tbl.id.deviceName + "'");
        }
        return builder.toString();
    }

    @Override
    public String getCopyColumnSQL(CopyColumn copyColumn) {
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ");
        builder.append(getTableNameSQL(copyColumn.tbl.id));
        builder.append(" SET ");
        builder.append(getColumnNameSQL(copyColumn.dstCol));
        builder.append(" = ");
        builder.append(getColumnNameSQL(copyColumn.srcCol));
        if (copyColumn.tbl.hasColumn("synclite_device_id")) {
            builder.append(" WHERE synclite_device_id = '" + copyColumn.tbl.id.deviceUUID + "'");
        }
        if (copyColumn.tbl.hasColumn("synclite_device_name")) {
            builder.append(" AND synclite_device_name = '" + copyColumn.tbl.id.deviceName + "'");
        }
        return builder.toString();
    }    

	@Override
	public String getLoadFileSQL(LoadFile loadFile) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(getTableNameSQL(loadFile.tbl.id));
        builder.append(" (");
        boolean first = true;
        StringBuilder colListBuilder = new StringBuilder();
        StringBuilder colValueBuilder = new StringBuilder();
        for (Column c : loadFile.tbl.columns) {
            if (!first) {
                colListBuilder.append(", ");
                colValueBuilder.append(", ");
            }
            colListBuilder.append(getColumnNameSQL(c));
            if (loadFile.defaultValues.containsKey(c.column)) {
            	colValueBuilder.append("'" + loadFile.defaultValues.get(c.column) + "'");
            } else {
            	colValueBuilder.append("?");
            }
            first = false;
        }
        builder.append(colListBuilder.toString());
        builder.append(") VALUES(");
        builder.append(colValueBuilder.toString());
        builder.append(")");
        return builder.toString();
	}

	@Override
	public String getMinusSQL(Minus minus) {
        StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ");
        builder.append(getTableNameSQL(minus.tbl.id));
        builder.append(" WHERE NOT EXISTS(SELECT 1 FROM ");
        builder.append(getTableNameSQL(minus.rhsTable.id));
        builder.append(" WHERE ");

        boolean includeAllColsInWhere = true;
        if (minus.tbl.hasPrimaryKey()) {
            includeAllColsInWhere = false;
        }

        boolean first = true;
        StringBuilder whereListBuilder = new StringBuilder();
        if (includeAllColsInWhere) {
            for (Column c : minus.tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
            	if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                	whereListBuilder.append(")");
                } else {
                  	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        } else {
            for (Column c : minus.tbl.columns) {
                if (c.pkIndex == 0) {
                    continue;
                }
                if (!first) {
                    whereListBuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                	whereListBuilder.append(")");
                } else {
 
                   	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(minus.tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(minus.rhsTable.id, c));
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        }
        builder.append(whereListBuilder.toString());
        builder.append(")");
        return builder.toString();		
	}

	@Override
	public String getCreateTempTableSql(Table tbl, TableID tmpTableID, String csvFilePath) {
		throw new UnsupportedOperationException("Temp table not implemented");
	}

	@Override
	public String getCreateUpdateTempTableSql(Table tbl, TableID tmpTableID, String csvFilePath) {
		throw new UnsupportedOperationException("Temp table not implemented");
	}

	@Override
	public String getDropTempTableSql(Table tbl, TableID tmpTableID) {
		throw new UnsupportedOperationException("Temp table not implemented");
	}

	@Override
	public String getLoadIntoTempTableSql(TableID tmpTableID, Path csvFilePath) {
		throw new UnsupportedOperationException("Temp table not implemented");
	}

	@Override
	public String getDeleteFromTempTableSql(Table tbl, TableID tmpTableID) {
        StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" WHERE NOT EXISTS(SELECT 1 FROM ");
        builder.append(getTableNameSQL(tmpTableID));
        builder.append(" WHERE ");

        boolean includeAllColsInWhere = true;
        if (tbl.hasPrimaryKey()) {
            includeAllColsInWhere = false;
        }

        boolean first = true;
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
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(")");
                } else {
                  	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
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
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(")");
                } else { 
                   	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        }
        builder.append(whereListBuilder.toString());
        builder.append(")");
        return builder.toString();		
	}

	@Override
	public String getInsertFromTempTableSql(Table tbl, TableID tmpTableID) {
        boolean first = true;
        StringBuilder insertColListBuilder = new StringBuilder();
        for (Column c : tbl.columns) {
        	if (!first) {
                insertColListBuilder.append(",");
        	}
            insertColListBuilder.append(c.column);
            first = false;
        }
        
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" SELECT ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" FROM ");
        builder.append(getTableNameSQL(tmpTableID));
        return builder.toString();
	}
	
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
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c) + "b");
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                    whereListBuilder.append(" IS NULL)");
                	whereListBuilder.append(")");
                }
                first = false;
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ");
        builder.append(getTableNameSQL(tbl.id));
        builder.append(" SET ");
        builder.append(setListBuilder.toString());
        builder.append(" FROM ");
        builder.append(getTableNameSQL(tmpTableID));
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());
        return builder.toString();
	}

	
	@Override
	public String getMergeFromTempTableSql(Table tbl, TableID tmpTableID) {
        boolean includeAllColsInWhere = true;
        if (tbl.hasPrimaryKey()) {
            includeAllColsInWhere = false;
        }

        boolean first = true;
        StringBuilder insertColListBuilder = new StringBuilder();
        StringBuilder insertValListBuilder = new StringBuilder();
        StringBuilder setListBuilder = new StringBuilder();
        for (Column c : tbl.columns) {
        	if (!first) {
                setListBuilder.append(", ");
                insertColListBuilder.append(",");
                insertValListBuilder.append(",");
        	}
        	setListBuilder.append("");
            setListBuilder.append(getFullColumnNameSQL(tbl.id, c));
            setListBuilder.append(" = ");
            setListBuilder.append(getFullColumnNameSQL(tmpTableID, c));

            insertColListBuilder.append(c.column);
            insertValListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
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
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(")");
                } else {
                  	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
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
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(")");
                } else { 
                   	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" = ");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
                	whereListBuilder.append(") OR ");
 
                	whereListBuilder.append("(");
                	whereListBuilder.append("(");
                    whereListBuilder.append(getFullColumnNameSQL(tbl.id, c));
                    whereListBuilder.append(" IS NULL) AND (");
                    whereListBuilder.append(getFullColumnNameSQL(tmpTableID, c));
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
        builder.append(" WHEN NOT MATCHED THEN INSERT(");
        builder.append(insertColListBuilder.toString());
        builder.append(") VALUES(");
        builder.append(insertValListBuilder.toString());
        builder.append(")");
        return builder.toString();
	}

	@Override
	public boolean supportsUpsert() {
		return false;
	}

	@Override
	public boolean supportsReplace() {
		return false;
	}
	
    @Override
    public String getUpsertSQL(Upsert upsert) {
    	throw new UnsupportedOperationException("Upsert not supported");
    }

    @Override
    public String getReplaceSQL(Replace replace) {
    	throw new UnsupportedOperationException("Replace not supported");
    }

    @Override
    public boolean isUpdateAllowed() {
    	return true;
	}
   

}
