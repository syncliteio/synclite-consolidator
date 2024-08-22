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

package com.synclite.consolidator.processor;

import java.nio.file.Path;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;

public abstract class TempTableLoaderExecutor extends FileLoaderExecutor {
	
	public TempTableLoaderExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
		// TODO Auto-generated constructor stub
	}

	protected void executeFileLoaderUpsertSql() throws DstExecutionException {
		Table tbl = prevBatchOper.tbl;
		TableID tmpTableID = TableID.from(tbl.id.deviceUUID, tbl.id.deviceName, dstIndex, tbl.id.database, tbl.id.schema, tbl.id.table + "_tmp");

		try (Statement stmt = conn.createStatement()) {			
			//Create temp table using copied csv file 
    		//Execute MERGE from temp table into main table
    		//Drop temp table
    		
    		String createTempTableSql = sqlGenerator.getCreateTempTableSql(tbl, tmpTableID, this.remoteCSVPath);
			try {
				tracer.debug("SQL : " + createTempTableSql);
				stmt.execute(createTempTableSql);				
			} catch (Exception e) {
				throw new DstExecutionException("Failed to execute create temp table sql : " + createTempTableSql + " : " + e.getMessage(), e);
			}
    		
    		String mergeFromTmpTableSql = sqlGenerator.getMergeFromTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + mergeFromTmpTableSql);
    			stmt.execute(mergeFromTmpTableSql);
    		} catch (Exception e) {
    			throw new DstExecutionException("Failed to execute merge from temp table sql : " + mergeFromTmpTableSql + " : " + e.getMessage(), e);
    		}    		
    		
    		String dropTmpTableSql = sqlGenerator.getDropTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + dropTmpTableSql);
    			stmt.execute(dropTmpTableSql);
    		} catch (Exception e) {
    			throw new DstExecutionException("Failed to execute drop temp temp table sql : " + dropTmpTableSql + " : " + e.getMessage(), e);
    		}   
    		
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader upsert oper : " + e.getMessage(), e);
    	}
    }

	protected void executeFileLoaderUpdateSql() throws DstExecutionException {
		Table tbl = prevBatchOper.tbl;
		TableID tmpTableID = TableID.from(tbl.id.deviceUUID, tbl.id.deviceName, dstIndex, null, null, tbl.id.table + "_tmp");

		try (Statement stmt = conn.createStatement()) {
			//Create temp table using copied csv file 
    		//UPDATE main table using temp table
    		//Drop temp file

			String createTempTableSql = sqlGenerator.getCreateUpdateTempTableSql(tbl, tmpTableID, this.remoteCSVPath);
			try {
    			tracer.debug("SQL : " + createTempTableSql);
				stmt.execute(createTempTableSql);
			} catch (Exception e) {
				throw new DstExecutionException("Failed to execute create temp table sql : " + createTempTableSql + " : " + e.getMessage(), e);
			}
    		
    		String updateFromTmpTableSql = sqlGenerator.getUpdateFromTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + updateFromTmpTableSql);
    			stmt.execute(updateFromTmpTableSql);
    		} catch (Exception e) {
    			throw new DstExecutionException("Failed to execute merge from temp table sql : " + updateFromTmpTableSql + " : " + e.getMessage(), e);
    		}    		
    		
    		String dropTmpTableSql = sqlGenerator.getDropTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + dropTmpTableSql);
    			stmt.execute(dropTmpTableSql);
    		} catch (Exception e) {
    			throw new DstExecutionException("Failed to execute drop temp temp table sql : " + dropTmpTableSql + " : " + e.getMessage(), e);
    		}   
    		
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader update oper : " + e.getMessage(), e);
    	}
    }

	protected void executeFileLoaderDeleteSql() throws DstExecutionException {
		Table tbl = prevBatchOper.tbl;
		TableID tmpTableID = TableID.from(tbl.id.deviceUUID, tbl.id.deviceName, dstIndex, null, null, tbl.id.table + "_tmp");

		try (Statement stmt = conn.createStatement()) {
			//Create temp table using copied csv file
    		//DELETE from main table using temp table
    		//Drop temp file
    		
    		String createTempTableSql = sqlGenerator.getCreateTempTableSql(tbl, tmpTableID, this.remoteCSVPath);
			try {
    			tracer.debug("SQL : " + createTempTableSql);
				stmt.execute(createTempTableSql);
			} catch (Exception e) {
				throw new DstExecutionException("Failed to execute create temp table sql : " + createTempTableSql + " : " + e.getMessage(), e);
			}
    		
    		String deleteFromTmpTableSql = sqlGenerator.getDeleteFromTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + deleteFromTmpTableSql);
    			stmt.execute(deleteFromTmpTableSql);
    		} catch (Exception e) {
    			throw new DstExecutionException("Failed to execute delete from temp table sql : " + deleteFromTmpTableSql + " : " + e.getMessage(), e);
    		}

    		String dropTmpTableSql = sqlGenerator.getDropTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + dropTmpTableSql);
    			stmt.execute(dropTmpTableSql);
    		} catch(Exception e) {
    			throw new DstExecutionException("Failed to execute drop temp table sql : " + dropTmpTableSql + " : " + e.getMessage(), e);
    		}
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader delete oper : " +  e.getMessage(), e);
    	}
    }

	
	@Override
	protected final void prepareInsertStatement(Insert oper) throws DstExecutionException {
		Table tbl = prevBatchOper.tbl;
		TableID tmpTableID = TableID.from(tbl.id.deviceUUID, tbl.id.deviceName, dstIndex, null, null, tbl.id.table + "_tmp");

		String insertIntoTableSql = oper.getFileLoaderSQL(sqlGenerator, Path.of(remoteCSVPath));
		try (Statement stmt = conn.createStatement()) {
			tracer.debug("SQL : " + insertIntoTableSql);
    		stmt.execute(insertIntoTableSql);    		
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader insert SQL : " + insertIntoTableSql + " : " + e.getMessage(), e);
    	}
    }

	protected void executeFileLoaderDeleteInsertSql() throws DstExecutionException {
		Table tbl = prevBatchOper.tbl;
		TableID tmpTableID = TableID.from(tbl.id.deviceUUID, tbl.id.deviceName, dstIndex, null, null, tbl.id.table + "_tmp");

		try (Statement stmt = conn.createStatement()) {
			//Create temp table using copied csv file
    		//DELETE from main table using temp table
    		//INSERT into main table from temp table
    		//Drop temp file
    		
    		String createTempTableSql = sqlGenerator.getCreateTempTableSql(tbl, tmpTableID, this.remoteCSVPath);
			try {
    			tracer.debug("SQL : " + createTempTableSql);
				stmt.execute(createTempTableSql);
			} catch (Exception e) {
				throw new DstExecutionException("Failed to execute create temp table sql : " + createTempTableSql + " : " + e.getMessage(), e);
			}
    		
    		String deleteFromTmpTableSql = sqlGenerator.getDeleteFromTempTableSql(tbl, tmpTableID);
    		try {
    			tracer.debug("SQL : " + deleteFromTmpTableSql);
    			stmt.execute(deleteFromTmpTableSql);
    		} catch(Exception e) {
    			throw new DstExecutionException("Failed to execute delete from temp table sql : " + deleteFromTmpTableSql + " : " + e.getMessage(), e);
    		}
    		
    		String insertFromTmpTableSql = sqlGenerator.getInsertFromTempTableSql(tbl, tmpTableID);
    		try {   		
    			tracer.debug("SQL : " + insertFromTmpTableSql);
    			stmt.execute(insertFromTmpTableSql);
    		} catch(Exception e) {
    			throw new DstExecutionException("Failed to execute insert from temp table sql : " + insertFromTmpTableSql + " : " + e.getMessage(), e);
    		}
    		
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute delete insert oper : " + e.getMessage(), e);
    	}
    }


	@Override
	protected void prepareInsertSql(Insert oper) {
	}

	@Override
	protected void prepareUpdateSql(Update oper) {
	}

	@Override
	protected void prepareUpsertSql(Upsert oper) {
	}

	@Override
	protected void prepareDeleteSql(Delete oper) {
	}

	@Override
	protected void prepareDeleteInsertSql(DeleteInsert oper) {
	}

}
