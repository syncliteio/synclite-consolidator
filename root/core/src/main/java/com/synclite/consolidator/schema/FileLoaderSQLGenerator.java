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
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public abstract class FileLoaderSQLGenerator extends JDBCSQLGenerator {

	public FileLoaderSQLGenerator(int dstIndex) {
		super(dstIndex);
		// TODO Auto-generated constructor stub
	}

	@Override
	public abstract String getFileLoaderInsertSQL(Insert insert, Path csvFilePath);

	@Override
	public String getDropTempTableSql(Table tbl, TableID tmpTableID) {
		throw new UnsupportedOperationException("Temp table not implemented");
	}
	
	@Override
	public String getFileLoaderUpsertSQL(Upsert upsert, Path csvFilePath) {
        boolean includeAllColsInWhere = true;
        if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
            if (upsert.tbl.hasPrimaryKey()) {
                includeAllColsInWhere = false;
            }
        }
        
        StringBuilder updateWhereListbuilder = new StringBuilder();
        StringBuilder updateSetListbuilder = new StringBuilder();
        StringBuilder insertColListbuilder = new StringBuilder();
        StringBuilder insertValListbuilder = new StringBuilder();
        
        updateSetListbuilder.append(" SET ");
        boolean first = true;
        int idx = 1;
        for (Column c : upsert.tbl.columns) {
            if (!first) {
            	updateSetListbuilder.append(", ");
            }
            updateSetListbuilder.append(getColumnNameSQL(c));
            updateSetListbuilder.append(" = stg.$" + idx);
            first = false;
            ++idx;
        }

        if (includeAllColsInWhere) {
            first = true;
            idx = 1;
            for (Column c : upsert.tbl.columns) {
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					continue;
				}
                if (!first) {
                    updateWhereListbuilder.append(" AND ");
                }
                if (c.isNotNull > 0) {
                	updateWhereListbuilder.append(getColumnNameSQL(c));
                	updateWhereListbuilder.append(" = stg.$" + idx);
                } else {
                	updateWhereListbuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
                	updateWhereListbuilder.append(getColumnNameSQL(c));
                	updateWhereListbuilder.append(" IS NULL) OR (");
                	updateWhereListbuilder.append(getColumnNameSQL(c));
                	updateWhereListbuilder.append(" = "+ "stg.$" + idx +"))");
                }
                first = false;
                ++idx;
            }
        } else {
            first = true;
            idx = 1;
            for (Column c : upsert.tbl.columns) {
            	if (c.pkIndex != 0) {
            		if (!first) {
            			updateWhereListbuilder.append(" AND ");
            		}
            		if (c.isNotNull > 0) {
            			updateWhereListbuilder.append(getColumnNameSQL(c));
            			updateWhereListbuilder.append(" = " + "stg.$" + idx);
            		} else {
            			updateWhereListbuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
            			updateWhereListbuilder.append(getColumnNameSQL(c));
            			updateWhereListbuilder.append(" IS NULL) OR (");
            			updateWhereListbuilder.append(getColumnNameSQL(c));
            			updateWhereListbuilder.append(" = "+ "stg.$" +  idx + "))");
            		}
            		first = false;
            	}
            	++idx;
            }
        }
        
        idx=1;
        first = true;
        for (Column c : upsert.tbl.columns) {
            if (!first) {
            	insertColListbuilder.append(", ");
            	insertValListbuilder.append(", ");
            }
            insertColListbuilder.append(getColumnNameSQL(c));
            insertValListbuilder.append("stg.$" + idx);
            first = false;
            ++idx;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("MERGE INTO ");
        builder.append(getTableNameSQL(upsert.tbl.id));
        builder.append(" USING ");
        builder.append(getStageSQL(csvFilePath) + " stg");
        builder.append(" ON (");
        builder.append(updateWhereListbuilder.toString());
        builder.append(") WHEN MATCHED THEN UPDATE SET ");
        builder.append(updateSetListbuilder.toString());
        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.append(insertColListbuilder);
        builder.append(") VALUES (");
        builder.append(insertValListbuilder);
        builder.append(")");        
        
        return builder.toString();
	}

	protected abstract String getStageSQL(Path csvFilePath);

	@Override
	public String getFileLoaderReplaceSQL(Replace replace, Path csvFilePath) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getFileLoaderUpdateSQL(Update update, Path csvFilePath) {
		
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
            setListBuilder.append(" = stg.$" + idx);
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
                    whereListBuilder.append(" = stg.$" + idx);
                } else {
                    whereListBuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = "+ "stg.$" + idx +"))");
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
            			whereListBuilder.append(" = " + "stg.$" + idx);
            		} else {
            			whereListBuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" IS NULL) OR (");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" = "+ "stg.$" +  idx + "))");
            		}
            		first = false;
            	}
            	++idx;
            }
        }

        builder.append(setListBuilder.toString());
        builder.append(" FROM '" + getStageSQL(csvFilePath) + "' stg");
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());

        return builder.toString();

	}

	@Override
	public String getFileLoaderDeleteSQL(Delete delete, Path csvFilePath) {
		
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
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = stg.$" + idx);
                } else {
                    whereListBuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" IS NULL) OR (");
                    whereListBuilder.append(getColumnNameSQL(c));
                    whereListBuilder.append(" = "+ "stg.$" + idx +"))");
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
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" = " + "stg.$" + idx);
            		} else {
            			whereListBuilder.append("(("+ "stg.$" + idx +" IS NULL AND ");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" IS NULL) OR (");
            			whereListBuilder.append(getColumnNameSQL(c));
            			whereListBuilder.append(" = "+ "stg.$" +  idx + "))");
            		}
            		first = false;
            	}
            	++idx;
            }
        }

        builder.append("'" + getStageSQL(csvFilePath) + "' stg");
        builder.append(" WHERE ");
        builder.append(whereListBuilder.toString());

        return builder.toString();

	}
}
