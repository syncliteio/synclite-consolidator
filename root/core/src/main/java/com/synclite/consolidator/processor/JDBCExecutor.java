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

import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstDuplicateKeyException;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.log.CDCLogPosition;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CopyColumn;
import com.synclite.consolidator.oper.CopyTable;
import com.synclite.consolidator.oper.CreateDatabase;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.FinishBatch;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.UpdateIfPredicate;
import com.synclite.consolidator.oper.Upsert;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;

public abstract class JDBCExecutor extends SQLExecutor {

	protected SQLGenerator sqlGenerator;
	protected Connection conn;
	protected PreparedStatement insertPstmt;
	protected PreparedStatement upsertPstmt;
	protected PreparedStatement replacePstmt;
	protected PreparedStatement deletePstmt;
	protected PreparedStatement updatePstmt;
	protected long batchOperCount;
	protected long currentOperBatchSizeLimit;
	private OperType batchType;
	protected Oper prevBatchOper;
	protected Device device;
	protected Logger tracer;
	protected int dstIndex;
	protected String currentSql;
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss.SS");

	public JDBCExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		try {
			this.device = device;
			this.tracer = tracer;
			this.batchOperCount = 0;
			this.currentOperBatchSizeLimit = 1L;
			this.dstIndex = dstIndex;
			this.sqlGenerator = SQLGenerator.getInstance(dstIndex);
			this.conn = connectToDst();
		} catch (SyncLiteException e) {
			throw new DstExecutionException("Failed to create JDBCExecutor : ", e);
		}
	}

	/**
	 * Returns a schema-qualified table name for use in raw SQL strings that bypass
	 * the system table mapper. Uses destination-specific quoting so generated SQL is
	 * valid for MySQL/PostgreSQL and matches DeviceSyncProcessor behavior.
	 */
	protected String qualifiedDstTableName(String tableName) {
		String schema = ConfLoader.getInstance().getDstSchema(dstIndex);
		if (schema != null && !schema.trim().isEmpty()) {
			return SQLGenerator.getInstance(dstIndex).getSchemaQualifiedObjectName(schema, tableName);
		}
		return tableName;
	}


	protected void bindInsertArgs(Insert oper) throws SQLException {
		int i = 1;
		for (Object o : oper.afterValues) {
			try {
				bindPrepared(insertPstmt, i, o, oper.tbl.columns.get(i-1), false);
			} catch (Exception e) {
				this.tracer.error("Failed to bind insert arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for INSERT prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index " + i + " for INSERT prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);				
			}
			++i;
		}
	}

	protected void bindUpsertArgs(Upsert oper) throws SQLException {
		int i = 1;
		for (Object o : oper.afterValues) {
			try {
				bindPrepared(upsertPstmt, i, o, oper.tbl.columns.get(i-1), false);
			} catch (Exception e) {
				this.tracer.error("Failed to bind upsert arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for UPSERT prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index "+ i + " for UPSERT prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}
	}

	protected void bindReplaceArgs(Replace oper) throws SQLException {
		int i = 1;
		for (Object o : oper.afterValues) {
			try {
				bindPrepared(replacePstmt, i, o, oper.tbl.columns.get(i-1), false);
			} catch (Exception e) {
				this.tracer.error("Failed to bind replace arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for REPLACE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index "+ i + " for REPLACE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}
	}

	protected final String getRecordDump(List<Object> beforeValues, List<Object> afterValues) {
		StringBuilder sb = new StringBuilder();
		sb.append("Before Values : [");
		if (beforeValues != null) {
			boolean first = true;
			for (Object s : beforeValues) {
				if (!first) {
					sb.append(",");
				}
				if (s != null) {
					sb.append(s.toString());
				} else {
					sb.append("null");
				}
				first = false;
			}
		}
		sb.append("]");
		sb.append("After Values : [");
		if (afterValues != null) {
			boolean first = true;
			for (Object s : afterValues) {
				if (!first) {
					sb.append(",");
				}
				if (s != null) {
					sb.append(s.toString());
				} else {
					sb.append("null");
				}
				first = false;			
			}
		} 
		sb.append("]"); 

		return sb.toString();
	}


	@Override
	public void insert(Insert oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindInsertArgs(oper);
			addToInsertBatch();
			updatePrevBatchOper(oper);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute insert batch " , e);
		}
	}
	
	@Override
	public void deleteInsert(DeleteInsert oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindDeleteInsertArgs(oper);
			updatePrevBatchOper(oper);
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute deleteinsert batch " , e);
		}
	}

	protected void bindDeleteInsertArgs(DeleteInsert oper) throws DstExecutionException {
		try {
			bindDeleteArgs(oper.getDeleteOper());
			addToDeleteBatch();

			bindInsertArgs(oper.getInsertOper());
			addToInsertBatch();
		} catch (SQLException | DstExecutionException e) {
			throw new DstExecutionException("Failed to bind deleteinsert arguments" , e);
		}
	}

	@Override
	public void upsert(Upsert oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindUpsertArgs(oper);
			addToUpsertBatch();
			updatePrevBatchOper(oper);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute upsert batch " , e);
		}
	}

	@Override
	public void replace(Replace oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindReplaceArgs(oper);
			addToReplaceBatch();
			updatePrevBatchOper(oper);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute replace batch " , e);
		}
	}

	protected void doAddToPstmtBatch(PreparedStatement pstmt) throws SQLException {
		pstmt.addBatch();
	}

	protected void addToInsertBatch() throws DstExecutionException {
		try {
			doAddToPstmtBatch(insertPstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to add to an insert batch " , e);
		}
	}

	protected void addToUpsertBatch() throws DstExecutionException {
		try {
			doAddToPstmtBatch(upsertPstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to add to an upsert batch " , e);
		}
	}

	protected void addToReplaceBatch() throws DstExecutionException {
		try {
			doAddToPstmtBatch(replacePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to add to an replace batch " , e);
		}
	}

	protected void addToUpdateBatch() throws DstExecutionException {
		try {
			doAddToPstmtBatch(updatePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to add to an update batch " , e);
		}
	}

	protected void addToDeleteBatch() throws DstExecutionException {
		try {
			doAddToPstmtBatch(deletePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to add to a delete batch " , e);
		}
	}

	protected void bindUpdateArgs(Update oper) throws SQLException {
		int i = 1;

		if (oper.setColumns != null) {
			// Use explicitly specified SET columns
			int colIdx = 0;
			for (Object o : oper.afterValues) {
				Column c = oper.setColumns.get(colIdx);
				try {
					bindPrepared(updatePstmt, i, o, c, false);
				} catch (Exception e) {
					this.tracer.error("Failed to bind update SET clause arguments with exception : " + e.getMessage(), e);
					this.tracer.error("Failed to bind SET clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					throw new SQLException("Failed to bind SET clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				}
				++i;
				++colIdx;
			}
		} else {
		for (Object o : oper.afterValues) {
			try {
				bindPrepared(updatePstmt, i, o, oper.tbl.columns.get(i-1), false);
			} catch (Exception e) {
				this.tracer.error("Failed to bind update SET clause arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind SET clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);		
				throw new SQLException("Failed to bind SET clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);		
			}
			++i;
		}
		}

		if (oper.whereColumns != null) {
			// Use explicitly specified WHERE columns
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.whereColumns.get(colIdx);
				if (c.isNotNull > 0) {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				} else {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				}
				++colIdx;
			}
		} else {
		boolean includeAllColsInWhere = true;
		if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
			if (oper.tbl.hasPrimaryKey()) {
				includeAllColsInWhere = false;
			}
		}

		if (includeAllColsInWhere) {
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.tbl.columns.get(colIdx);
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					++colIdx;
					continue;
				}
				if (c.isNotNull > 0) {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);	
					}
					++i;
				} else {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				}
				++colIdx;
			}
		} else {
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.tbl.columns.get(colIdx);
				if (c.pkIndex == 0) {
					++colIdx;
					continue;
				}
				if (c.isNotNull > 0) {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				} else {
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(updatePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind update WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for UPDATE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				}
				++colIdx;
			}
		}
		}
	}

	@Override
	public void update(Update oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindUpdateArgs(oper);
			addToUpdateBatch();
			updatePrevBatchOper(oper);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute dst batch " , e);
		}
	}

	protected void bindDeleteArgs(Delete oper) throws SQLException {
		int i = 1;

		if (oper.whereColumns != null) {
			// Use explicitly specified WHERE columns
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.whereColumns.get(colIdx);
				if (c.isNotNull > 0) {
					try {
						bindPrepared(deletePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				} else {
					try {
						bindPrepared(deletePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(deletePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e);
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				}
				++colIdx;
			}
		} else {
		boolean includeAllColsInWhere = true;
		if (ConfLoader.getInstance().getDstOperPredicateOpt(dstIndex) == true) {
			if (oper.tbl.hasPrimaryKey()) {
				includeAllColsInWhere = false;
			}
		}

		if (includeAllColsInWhere) {
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.tbl.columns.get(colIdx);
				//Exclude system generated TS column from where list
				if (c.isSystemGeneratedTSColumn) {
					++colIdx;
					continue;
				}
				if (c.isNotNull > 0) {
					try {
						bindPrepared(deletePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				} else {
					try {
						bindPrepared(deletePstmt, i, o, oper.tbl.columns.get(colIdx), true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(deletePstmt, i, o, oper.tbl.columns.get(colIdx), true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					} 
					++i;
				}
				++colIdx;
			}
		} else {
			int colIdx = 0;
			for (Object o : oper.beforeValues) {
				Column c = oper.tbl.columns.get(colIdx);
				if (c.pkIndex == 0) {
					++colIdx;
					continue;
				}
				if (c.isNotNull > 0) {
					try {
						bindPrepared(deletePstmt, i, o, c, true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				} else {
					try {
						bindPrepared(deletePstmt, i, o, oper.tbl.columns.get(colIdx), true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
					try {
						bindPrepared(deletePstmt, i, o, oper.tbl.columns.get(colIdx), true);
					} catch (Exception e) {
						this.tracer.error("Failed to bind delete WHERE clause arguments with exception : " + e.getMessage(), e); 
						this.tracer.error("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
						throw new SQLException("Failed to bind WHERE clause argument with value " + o + " at index " + i + " for DELETE prepared statement : " + currentSql + " for table : " + oper.tbl + " for column : " + c.column + "(" + c.type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
					}
					++i;
				}
				++colIdx;
			}
		}
		}
	}

	@Override
	public void delete(Delete oper) throws DstExecutionException {
		try {
			checkBatchFlushAndRePrepare(oper);
			bindDeleteArgs(oper);
			addToDeleteBatch();
			updatePrevBatchOper(oper);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute dst batch " , e);
		}
	}

	@Override
	public void executeSQL(DeleteIfPredicate sqlStmt) throws DstExecutionException {
		executeUnbatchedOper(sqlStmt);
	}

	@Override
	public void executeSQL(UpdateIfPredicate sqlStmt) throws DstExecutionException {
		executeUnbatchedOper(sqlStmt);
	}

	@Override
	public void executeSQL(NativeOper sqlStmt) throws DstExecutionException {
		executeUnbatchedOper(sqlStmt);
	}

	@Override
	public void executeSQL(FinishBatch finishBatch) throws DstExecutionException{
		//No OP
	}

	@Override
	public void executeSQL(Minus sqlStmt) throws DstExecutionException {
		executeUnbatchedOper(sqlStmt);
	}

	protected final void updatePrevBatchOper(Oper oper) {
		prevBatchOper = oper;
		++batchOperCount;
	}

	/*
    private void bindPrepared(PreparedStatement pstmt, int i, Object o, DataType dt) throws SQLException {
        if (o == null) {
            pstmt.setNull(i, Types.VARCHAR);
        } else if (dt.storageClass == StorageClass.BLOB){
            pstmt.setBinaryStream(i, (InputStream) o);
        } else if (dt.storageClass== StorageClass.TEXT){
            pstmt.setString(i, o.toString());
        } else {
            pstmt.setObject(i, o, dt.javaSQLType);
        }

    }
	 */

	public int convertJDBCTypeToTypes(JDBCType jdbcType) {
		// Map JDBCType to Types based on ordinal values
		switch (jdbcType) {
		case BIT:
			return Types.BIT;
		case TINYINT:
			return Types.TINYINT;
		case SMALLINT:
			return Types.SMALLINT;
		case INTEGER:
			return Types.INTEGER;
		case BIGINT:
			return Types.BIGINT;
		case FLOAT:
			return Types.FLOAT;
		case REAL:
			return Types.REAL;
		case DOUBLE:
			return Types.DOUBLE;
		case NUMERIC:
			return Types.NUMERIC;
		case DECIMAL:
			return Types.DECIMAL;
		case CHAR:
			return Types.CHAR;
		case VARCHAR:
			return Types.VARCHAR;
		case LONGVARCHAR:
			return Types.LONGVARCHAR;
		case DATE:
			return Types.DATE;
		case TIME:
			return Types.TIME;
		case TIMESTAMP:
			return Types.TIMESTAMP;
		case BINARY:
			return Types.BINARY;
		case VARBINARY:
			return Types.VARBINARY;
		case LONGVARBINARY:
			return Types.LONGVARBINARY;
		case NULL:
			return Types.NULL;
		case OTHER:
			return Types.OTHER;
		case JAVA_OBJECT:
			return Types.JAVA_OBJECT;
		case DISTINCT:
			return Types.DISTINCT;
		case STRUCT:
			return Types.STRUCT;
		case ARRAY:
			return Types.ARRAY;
		case BLOB:
			return Types.BLOB;
		case CLOB:
			return Types.CLOB;
		case REF:
			return Types.REF;
		case DATALINK:
			return Types.DATALINK;
		case BOOLEAN:
			return Types.BOOLEAN;
		case ROWID:
			return Types.ROWID;
		case NCHAR:
			return Types.NCHAR;
		case NVARCHAR:
			return Types.NVARCHAR;
		case LONGNVARCHAR:
			return Types.LONGNVARCHAR;
		case NCLOB:
			return Types.NCLOB;
		case SQLXML:
			return Types.SQLXML;
		case REF_CURSOR:
			return Types.REF_CURSOR;
		case TIME_WITH_TIMEZONE:
			return Types.TIME_WITH_TIMEZONE;
		case TIMESTAMP_WITH_TIMEZONE:
			return Types.TIMESTAMP_WITH_TIMEZONE;
		default:
			return Types.OTHER; // Default to OTHER for unsupported types
		}
	}


	protected void bindPrepared(PreparedStatement pstmt, int i, Object o, Column c, boolean isConditionArg) throws SQLException {
		try {
			if (o == null) {
				pstmt.setNull(i, convertJDBCTypeToTypes(c.type.javaSQLType));
			} else {
				switch (c.type.javaSQLType) {
				case INTEGER:
					pstmt.setInt(i, Integer.parseInt(o.toString()));
					break;
				case VARCHAR:
					pstmt.setString(i, o.toString());
					break;
				case DECIMAL:
					pstmt.setBigDecimal(i, BigDecimal.valueOf(Double.parseDouble(o.toString())));
					break;
				case DOUBLE:
					Double doubleVal = Double.parseDouble(o.toString());
					pstmt.setDouble(i, doubleVal);
					break;
				case FLOAT:
					pstmt.setFloat(i, Float.parseFloat(o.toString()));
					break;
				case REAL:
					pstmt.setFloat(i, Float.parseFloat(o.toString()));
					break;
				case SMALLINT:
					pstmt.setShort(i, Short.parseShort(o.toString()));
					break;
				case TINYINT:
					pstmt.setByte(i, Byte.parseByte(o.toString()));
					break;
				case NUMERIC:
					pstmt.setBigDecimal(i, BigDecimal.valueOf(Double.parseDouble(o.toString())));
					break;
				case BIGINT:
					pstmt.setLong(i, Long.parseLong(o.toString()));
					break;
				case BINARY:
					pstmt.setBytes(i, coerceBinaryValue(o));
					break;
				case VARBINARY:
					pstmt.setBytes(i, coerceBinaryValue(o));
					break;
				case LONGVARBINARY:
					pstmt.setBytes(i, coerceBinaryValue(o));
					break;
				case BIT:
				case BOOLEAN:
					switch(o.toString().toLowerCase().strip()) {
					case "1":
					case "y":
					case "yes":
					case "t":
					case "true":
					case "on":
						pstmt.setBoolean(i, true);
						break;
					default:
						pstmt.setBoolean(i, false);
					}
					break;
				case CHAR:
					pstmt.setByte(i, Byte.parseByte(o.toString()));
					break;
				case BLOB:
					if (o instanceof InputStream) {
						pstmt.setBinaryStream(i, (InputStream) o);
					} else if (o instanceof Blob) {
						Blob blob = (Blob) o;
						pstmt.setBytes(i, blob.getBytes(1, (int) blob.length()));
					} else {
						pstmt.setBytes(i, coerceBinaryValue(o));
					}
					break;
				case CLOB:
					if (o instanceof InputStream) {
						pstmt.setBinaryStream(i, (InputStream) o);
					} else {
						pstmt.setObject(i, o);
					}
					break;
				case DATE:
					setDate(pstmt, i, o);
					break;
				case TIME:
				case TIME_WITH_TIMEZONE:
					setTime(pstmt, i , o);
					break;
				case TIMESTAMP:
				case TIMESTAMP_WITH_TIMEZONE:
					setTimestamp(pstmt, i, o);
					break;
				case ARRAY:
					try {
						String arrayString = o.toString().trim();
						if (arrayString.startsWith("{") && arrayString.endsWith("}")) {
							arrayString = arrayString.substring(1, arrayString.length() - 1);
						} else if (arrayString.startsWith("[") && arrayString.endsWith("]")) {
							arrayString = arrayString.substring(1, arrayString.length() - 1);
						}
						Object[] objectArray = Arrays.stream(arrayString.split(","))
								.map(String::trim)
								.toArray(); 
						switch(c.type.dbNativeDataType.toUpperCase()) {
						case "INTEGER[]":
							Array sqlArr = pstmt.getConnection().createArrayOf("INTEGER", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "BIGINT[]":
							sqlArr = pstmt.getConnection().createArrayOf("BIGINT", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "BOOLEAN[]":
							sqlArr = pstmt.getConnection().createArrayOf("BOOLEAN", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "FLOAT[]":
						case "VECTOR":
							sqlArr = pstmt.getConnection().createArrayOf("FLOAT", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "NUMERIC[]":
							sqlArr = pstmt.getConnection().createArrayOf("NUMERIC", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "TIMESTAMP[]":
							sqlArr = pstmt.getConnection().createArrayOf("TIMESTAMP", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "DATE[]":
							sqlArr = pstmt.getConnection().createArrayOf("DATE", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "TIME[]":
							sqlArr = pstmt.getConnection().createArrayOf("TIME", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "CHARACTER[]":
							sqlArr = pstmt.getConnection().createArrayOf("CHARACTER", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						case "TEXT[]":
						default:	
							sqlArr = pstmt.getConnection().createArrayOf("TEXT", objectArray);
							pstmt.setArray(i, sqlArr);		
							break;
						}
					} catch (Exception e) {
						pstmt.setObject(i, o);
					}
					break;
				case ROWID:
				case LONGNVARCHAR:
				case LONGVARCHAR:
				case DATALINK:     		
				case DISTINCT:
				case JAVA_OBJECT:
				case NCHAR:
				case NCLOB:
				case NVARCHAR:
				case REF:
				case REF_CURSOR:
				case SQLXML:
				case STRUCT:
				default:
					pstmt.setObject(i, o);
				}
			}
		} catch (Exception e) {
			if (ConfLoader.getInstance().getDstSetUnparsableValuesToNull(dstIndex)) {
				pstmt.setNull(i, convertJDBCTypeToTypes(c.type.javaSQLType));
			} else {
				throw e;
			}
		}

	}

	/**
	 * Coerces runtime values to a stable byte representation for binary destinations.
	 * Accepts native byte[]/Blob/InputStream and common textual encodings such as
	 * PostgreSQL bytea hex (\\x...), SQLite hex literals (X'ABCD'), or plain hex.
	 */
	protected byte[] coerceBinaryValue(Object o) throws SQLException {
		if (o == null) {
			return null;
		}
		if (o instanceof byte[]) {
			return (byte[]) o;
		}
		if (o instanceof Blob) {
			Blob blob = (Blob) o;
			return blob.getBytes(1, (int) blob.length());
		}
		if (o instanceof InputStream) {
			try {
				return ((InputStream) o).readAllBytes();
			} catch (Exception e) {
				throw new SQLException("Failed to read binary stream value", e);
			}
		}
		if (o instanceof String) {
			String s = ((String) o).trim();
			byte[] fromHex = parseHexBinaryLiteral(s);
			if (fromHex != null) {
				return fromHex;
			}
			return s.getBytes(StandardCharsets.UTF_8);
		}
		return o.toString().getBytes(StandardCharsets.UTF_8);
	}

	protected byte[] parseHexBinaryLiteral(String s) {
		if (s == null) {
			return null;
		}
		String hex = s;
		if ((hex.startsWith("\\\\x") || hex.startsWith("\\x")) && hex.length() > 2) {
			hex = hex.substring(2);
		} else if ((hex.startsWith("0x") || hex.startsWith("0X")) && hex.length() > 2) {
			hex = hex.substring(2);
		} else if ((hex.startsWith("X'") || hex.startsWith("x'")) && hex.endsWith("'") && hex.length() > 3) {
			hex = hex.substring(2, hex.length() - 1);
		}
		if (hex.isEmpty() || (hex.length() % 2 != 0)) {
			return null;
		}
		for (int i = 0; i < hex.length(); i++) {
			if (Character.digit(hex.charAt(i), 16) < 0) {
				return null;
			}
		}
		byte[] out = new byte[hex.length() / 2];
		for (int i = 0; i < hex.length(); i += 2) {
			int hi = Character.digit(hex.charAt(i), 16);
			int lo = Character.digit(hex.charAt(i + 1), 16);
			out[i / 2] = (byte) ((hi << 4) + lo);
		}
		return out;
	}

	protected void setDate(PreparedStatement pstmt, int i, Object o) throws SQLException {		
		try {
			pstmt.setDate(i, Date.valueOf(o.toString()));
		} catch(Exception e) {
			try {
				//Try parsing as a timestamp first.
				//
				Timestamp ts = Timestamp.valueOf(o.toString());
				java.sql.Date sqlDate = new java.sql.Date(ts.getTime());
				pstmt.setDate(i, sqlDate);
			} catch (Exception e1) {
				pstmt.setObject(i, o);
			}
		}
	}

	protected void setTime(PreparedStatement pstmt, int i, Object o) throws SQLException {
		try {
			pstmt.setTime(i, Time.valueOf(o.toString()));
		} catch(Exception e) {
			try {
				Timestamp ts = Timestamp.valueOf(o.toString());
				java.sql.Time sqlTime = new java.sql.Time(ts.getTime());
				pstmt.setTime(i, sqlTime);
			} catch (Exception e1) {	
				pstmt.setObject(i, o);
			}
		}
	}

	protected void setTimestamp(PreparedStatement pstmt, int i, Object o) throws SQLException {
		try {
			pstmt.setTimestamp(i, Timestamp.valueOf(o.toString()));			
		} catch (Exception e) {        
			try {
				// If parsing as a timestamp fails, try parsing as a date only
	            java.util.Date date = dateFormatter.parse(o.toString());
	
	            // Convert to java.sql.Timestamp using midnight as the time
	            pstmt.setTimestamp(i, new Timestamp(date.getTime()));
			} catch (Exception e1) {
				try {
					// If parsing as a date only fails, try parsing as a time with optional additional digits
	                java.util.Date time = timeFormatter.parse(o.toString());
	                // Convert to java.sql.Timestamp using the current date
		            pstmt.setTimestamp(i, new Timestamp(time.getTime()));
				} catch (Exception e2) {
					pstmt.setObject(i, o);					
				}
            }
		}
	}

	
	protected final void checkBatchFlushAndRePrepare(Oper oper) throws DstExecutionException {
		if (prevBatchOper == null) {
			if (oper.isBatchable()) {
				prepareBatch(oper);
			}
			return;
		}
		//If table or oper change then flush previous batch and reset prepared statement
		if ((oper.tbl == null) || (!oper.tbl.equals(prevBatchOper.tbl))) {
			flushAndResetBatch();
			prepareBatch(oper);
		} else if (oper.operType != prevBatchOper.operType) {
			flushAndResetBatch();
			prepareBatch(oper);
		} else if ((oper.operType == OperType.DELETE || oper.operType == OperType.UPDATE) && hasDMLWhereColumnsChanged((DML) oper, (DML) prevBatchOper)) {
			//WHERE/SET columns changed (e.g. different WHERE columns for same table/operType)
			flushAndResetBatch();
			prepareBatch(oper);
		} else if (batchOperCount == currentOperBatchSizeLimit) {
			//If batch full then flush previous batch and do not clear the prepared statement;
			flushBatch();
			//Prepare again if needed as we have an oper to process post flush
			if (!isOperPrepared(oper)) {
				prepareBatch(oper);
			}
		} else if (oper.isBatchable() && !isOperPrepared(oper)) {
			prepareBatch(oper);
		}
	}

	protected boolean isOperPrepared(Oper oper) throws DstExecutionException {
		switch (oper.operType) {
		case INSERT:
			return (insertPstmt != null);
		case UPDATE:
			return (updatePstmt != null);
		case DELETE:
			return (deletePstmt != null);
		case UPSERT:
			return (upsertPstmt != null);
		case REPLACE:
			return (replacePstmt != null);
		case DELETEINSERT:
			return ((deletePstmt != null) && (insertPstmt != null));			
		default:
			return false;
		}    	
	}

	private boolean hasDMLWhereColumnsChanged(DML current, DML previous) {
		int curWhereColCount = (current.whereColumns != null) ? current.whereColumns.size() : -1;
		int prevWhereColCount = (previous.whereColumns != null) ? previous.whereColumns.size() : -1;
		if (curWhereColCount != prevWhereColCount) {
			return true;
		}
		if (current.setColumns != null || previous.setColumns != null) {
			int curSetColCount = (current.setColumns != null) ? current.setColumns.size() : -1;
			int prevSetColCount = (previous.setColumns != null) ? previous.setColumns.size() : -1;
			if (curSetColCount != prevSetColCount) {
				return true;
			}
		}
		return false;
	}

	protected final void prepareBatch(Oper oper) throws DstExecutionException {
		if (!oper.isBatchable()) {
			return;
		}
		switch (oper.operType) {
		case INSERT:
			prepareInsertStatement((Insert)oper);
			this.batchType = OperType.INSERT;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstInsertBatchSize(dstIndex);
			break;
		case UPDATE:
			prepareUpdateStatement((Update)oper);
			this.batchType = OperType.UPDATE;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstUpdateBatchSize(dstIndex);
			break;
		case DELETE:
			prepareDeleteStatement((Delete)oper);
			this.batchType = OperType.DELETE;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstDeleteBatchSize(dstIndex);
			break;
		case UPSERT:
			prepareUpsertStatement((Upsert)oper);
			this.batchType = OperType.UPSERT;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstInsertBatchSize(dstIndex);
			break;
		case REPLACE:
			prepareReplaceStatement((Replace)oper);
			this.batchType = OperType.REPLACE;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstInsertBatchSize(dstIndex);
			break;
		case DELETEINSERT:
			prepareDeleteInsertStatement((DeleteInsert)oper);
			this.batchType = OperType.DELETEINSERT;
			this.currentOperBatchSizeLimit = ConfLoader.getInstance().getDstInsertBatchSize(dstIndex);
			break;			
		default:
			return;
		}    
	}

	private final void flushAndResetBatch() throws DstExecutionException {
		if ((prevBatchOper!= null) && (prevBatchOper.isBatchable())) {
			flushBatch();
		}
		resetBatch();
	}

	protected void resetBatch() throws DstExecutionException {
		this.insertPstmt = null;
		this.updatePstmt = null;
		this.deletePstmt = null;
		this.upsertPstmt = null;
		this.replacePstmt = null;
		this.batchOperCount = 0;
	}

	protected boolean isOpenInsertBatch() {
		return (insertPstmt != null);
	}

	protected boolean isOpenUpdateBatch() {
		return (updatePstmt != null);
	}

	protected boolean isOpenDeleteBatch() {
		return (deletePstmt != null);
	}

	protected boolean isOpenUpsertBatch() {
		return (upsertPstmt != null);
	}

	protected boolean isOpenReplaceBatch() {
		return (replacePstmt != null);
	}

	private final void flushBatch() throws DstExecutionException  {		
		try {
			if (this.batchType == null) {
				return;
			}
			switch (this.batchType) {
			case INSERT:
				try {
					if (isOpenInsertBatch()) {
						tracer.debug(prevBatchOper.tbl +  " : Flushing insert batch of size : " + batchOperCount);
						executeInsertBatch();
					}
				} catch (DstExecutionException e) {
					if (isDuplicateKeyException(e)) {
						throw new DstDuplicateKeyException(e);            	
					} else {
						throw e;
					}
				}
				break;
			case DELETE:
				if (isOpenDeleteBatch()) {
					tracer.debug(prevBatchOper.tbl +  " : Flushing delete batch of size : " + batchOperCount);
					executeDeleteBatch();
				}
				break;
			case UPDATE:
				if (isOpenUpdateBatch()) {
					tracer.debug(prevBatchOper.tbl +  " : Flushing update batch of size : " + batchOperCount);
					executeUpdateBatch();
				}
				break;
			case UPSERT:
				if (isOpenUpsertBatch()) {
					tracer.debug(prevBatchOper.tbl +  " : Flushing upsert batch of size : " + batchOperCount);
					executeUpsertBatch();
				}
				break;
			case REPLACE:
				if (isOpenReplaceBatch()) {
					tracer.debug(prevBatchOper.tbl +  " : Flushing replace batch of size : " + batchOperCount);
					executeReplaceBatch();
				}
				break;
			case DELETEINSERT:
				if (isOpenInsertBatch() && isOpenDeleteBatch()) {
					tracer.debug(prevBatchOper.tbl +  " : Flushing deleteinsert batch of size : " + batchOperCount);
					executeDeleteInsertBatch();
				}
				break;
			default:	
			}
			this.batchOperCount = 0;
			this.prevBatchOper = null;
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to flush a batch : ", e);
		}
	}

	protected void executeDeleteInsertBatch() throws DstExecutionException {
		tracer.debug(prevBatchOper.tbl +  " : Flushing delete subbatch of deleteinsert batch of size : " + batchOperCount);
		executeDeleteBatch();
		tracer.debug(prevBatchOper.tbl +  " : Flushing insert subbatch of deleteinsert batch of size : " + batchOperCount);
		try {
			executeInsertBatch();
		} catch (DstExecutionException e) {
			if (isDuplicateKeyException(e)) {
				throw new DstDuplicateKeyException(e);
			} else {
				throw e;
			}
		}
	}


	protected void doExcutePstmtBatch(PreparedStatement pstmt) throws SQLException {
		pstmt.executeBatch();
	}

	protected void executeDeleteBatch() throws DstExecutionException {
		try {
			doExcutePstmtBatch(deletePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute a delete batch : ", e);
		}		
	}

	protected void executeUpdateBatch() throws DstExecutionException {
		try {
			doExcutePstmtBatch(updatePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute an update batch : ", e);
		}		
	}

	protected void executeInsertBatch() throws DstExecutionException {
		try {
			doExcutePstmtBatch(insertPstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute an insert batch : ", e);
		}		
	}

	protected void executeUpsertBatch() throws DstExecutionException {
		try {
			doExcutePstmtBatch(upsertPstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute an upsert batch : ", e);
		}		
	}

	protected void executeReplaceBatch() throws DstExecutionException {
		try {
			doExcutePstmtBatch(replacePstmt);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to execute an replace batch : ", e);
		}		
	}

	protected void prepareInsertStatement(Insert oper) throws DstExecutionException {
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.insertPstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare insert statement : " + sql, e);
		}
	}

	protected void prepareUpdateStatement(Update oper) throws DstExecutionException {
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.updatePstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare update statement : " + sql, e);
		}
	}

	protected void prepareDeleteStatement(Delete oper) throws DstExecutionException {
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.deletePstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare delete statement : " + sql, e);
		}
	}

	protected void prepareUpsertStatement(Upsert oper) throws DstExecutionException {
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.upsertPstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare upsert statement : " + sql, e);
		}
	}

	protected void prepareReplaceStatement(Replace oper) throws DstExecutionException {
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.replacePstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare replace statement : " + sql, e);
		}
	}

	protected void prepareDeleteInsertStatement(DeleteInsert oper) throws DstExecutionException {
		String sql = oper.getDeleteOper().getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.deletePstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare delete statement of DeleteInsert operator: " + sql, e);
		}

		sql = oper.getInsertOper().getSQL(this.sqlGenerator);
		this.currentSql = sql;
		try {
			tracer.debug(oper.tbl + " SQL : " + sql);
			this.insertPstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to prepare insert statement of DeleteInsert operator: " + sql, e);
		}
	}

	
	@Override
	public void createTable(CreateTable oper) throws DstExecutionException {
		try {
			if (sqlGenerator.supportsIfClause() ) {
				executeUnbatchedOper(oper);
			}
			else if (!tableExists(oper.tbl)) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			//Even after checking table exits , if we got a table already exists error then skip and move on
			if (isTableAlreadyExistsException(e)) {
				this.tracer.info("Table : " + oper.tbl.id + " found already existing");
				return;
			}				
			throw new DstExecutionException("Failed to execute createTable dst oper : " + e.getMessage(), e);
		}
	}

	protected boolean isTableAlreadyExistsException(Exception e) {
		return false;
	}


	@Override
	public void dropTable(DropTable oper) throws DstExecutionException {
		try {
			if (sqlGenerator.supportsIfClause()) {
				executeUnbatchedOper(oper);
			} else if (tableExists(oper.tbl)) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute dropTable dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void renameTable(RenameTable oper) throws DstExecutionException {
		try {
			//If old table exists then rename
			//Idempotent way to execute rename        	
			if (tableExists(oper.tbl)) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute renameTable dst oper : " + e.getMessage(), e);
		}
	}


	@Override
	public void addColumn(AddColumn oper) throws DstExecutionException {
		try {
			if (sqlGenerator.supportsIfClauseInAlterColumn()) {
				executeUnbatchedOper(oper);
			} else if (!columnExists(oper.tbl, oper.columns.get(0))) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			//TODO
			//check if column already exists exception, if yes then ignore it and move on.
			if (! columnExistsException(e)) {
				throw new DstExecutionException("Failed to execute addColumn dst oper : " + e.getMessage(), e);
			}
		}
	}

	public boolean columnExistsException(DstExecutionException e) {
		return false;
	}


	@Override
	public void alterColumn(AlterColumn oper) throws DstExecutionException {
		try {
			executeUnbatchedOper(oper);
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute alterColumn dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void dropColumn(DropColumn oper) throws DstExecutionException {
		try {
			if (sqlGenerator.supportsIfClauseInAlterColumn()) {
				executeUnbatchedOper(oper);
			} else if (columnExists(oper.tbl, oper.columns.get(0))) {
				executeUnbatchedOper(oper);
			}        	
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute dropColumn dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void renameColumn(RenameColumn oper) throws DstExecutionException {
		try {
			//If old column exists then rename (check by old name, not new name)
			//This is an idempotent way to execute rename
			Column oldCol = new Column(oper.columns.get(0).cid, oper.oldName, oper.columns.get(0).type,
					oper.columns.get(0).isNotNull, oper.columns.get(0).defaultValue,
					oper.columns.get(0).pkIndex, oper.columns.get(0).isAutoIncrement);
			if (columnExists(oper.tbl, oldCol)) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute renameColumn dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void beginTran() throws DstExecutionException {
		try {
			if (conn == null) {        	  
				this.conn = connectToDst();
			}
			tracer.debug("SQL : BEGIN");
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to begin transaction on destination connection : " + e.getMessage(), e);
		}
	}

	protected Connection connectToDst() throws DstExecutionException {
		return JDBCConnector.getInstance(dstIndex).connect();
	}

	protected String getTelemetryFileURL() throws DstExecutionException {
		//By default consolidate into the specified destination database.
		return ConfLoader.getInstance().getDstConnStr(dstIndex);
	}

	protected final void executeUnbatchedOper(Oper oper) throws DstExecutionException {
		checkBatchFlushAndRePrepare(oper);
		executeUnbatchedSql(oper);
	}

	/**
	 * Whether this destination aborts the ENTIRE transaction when a single
	 * statement fails (PostgreSQL -> SQLSTATE 25P02 "current transaction is
	 * aborted, commands ignored until end of transaction block"). Such
	 * destinations need each unbatched (DDL / native) statement wrapped in a
	 * savepoint so that a benign, idempotently-tolerated failure (e.g. a
	 * concurrent CREATE TABLE losing the pg_type catalog race) does not poison
	 * the surrounding transaction and cause the following DML to fail. Engines
	 * that do statement-level rollback (SQLite/MySQL/MSSQL default) return
	 * false and skip the savepoint overhead.
	 */
	protected boolean abortsTxnOnStatementError() {
		return false;
	}

	protected void executeUnbatchedSql(Oper oper) throws DstExecutionException {
		String sql = oper.getSQL(sqlGenerator);
		this.currentSql = sql;
		tracer.debug(oper.tbl + " : SQL : " + sql);

		//For destinations that abort the whole transaction on a single failed
		//statement, take a savepoint so a caught-and-tolerated failure can be
		//rolled back to a clean point, keeping the outer transaction alive. The
		//upper layers (JDBCExecutor DDL idempotency checks and the
		//DeviceEventStreamer/DeviceConsolidator isDDLIdempotentError skip) then
		//continue with the following statements instead of hitting 25P02.
		boolean useSavepoint = false;
		Savepoint sp = null;
		try {
			useSavepoint = abortsTxnOnStatementError() && (conn != null) && !conn.getAutoCommit();
		} catch (SQLException e) {
			useSavepoint = false;
		}
		if (useSavepoint) {
			try {
				sp = conn.setSavepoint();
			} catch (SQLException e) {
				sp = null;
				useSavepoint = false;
			}
		}

		try (Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
			if (useSavepoint && (sp != null)) {
				try {
					conn.releaseSavepoint(sp);
				} catch (SQLException ignore) {
					//Non-fatal: savepoint is discarded at commit/rollback anyway.
				}
			}
		} catch (SQLException e) {
			if (useSavepoint && (sp != null)) {
				try {
					//Un-poison the transaction so a tolerated failure upstream can proceed.
					conn.rollback(sp);
				} catch (SQLException rbEx) {
					//If rollback-to-savepoint fails the txn is unrecoverable; surface the original error.
				}
			}
			throw new DstExecutionException("Failed to execute SQL : " + sql + " : " + e.getMessage(), e);
		}
	}

	protected void doCommit() throws DstExecutionException {
		try {			
			this.conn.commit();
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to commit a transaction : " + e.getMessage(), e);
		}
	}

	@Override
	public void commitTran() throws DstExecutionException {
		try {
			flushAndResetBatch();
			tracer.debug("SQL : COMMIT");
			doCommit();
		} catch (Exception e) {
			throw new DstExecutionException("Failed to commit a transaction : " + e.getMessage(), e);
		}
	}

	private final void closeConn() throws DstExecutionException {
		try {
			if (this.conn != null) {
				this.conn.close();
			}
			this.conn = null;
		} catch (SQLException e) {
			throw new DstExecutionException("Failled to close a connection : " + e.getMessage(), e);
		}

	}

	@Override
	public void rollbackTran() throws DstExecutionException {
		try {
			resetBatch();
			if (this.conn != null) {
				tracer.debug("SQL : ROLLBACK");
				this.conn.rollback();
			}
		} catch (SQLException e) {
			//Ignore
		}
		closeConn();
	}

	@Override
	public void createDatabase(CreateDatabase oper) throws DstExecutionException {
		try {
			if (!sqlGenerator.isDatabaseAllowed()) {
				//Skip
				return;
			}
			if (sqlGenerator.supportsIfClause() ) {
				executeUnbatchedOper(oper);
			} else if (!databaseExists(oper.tbl)) {
				executeUnbatchedOper(oper);
			}	
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to create database : " + e.getMessage(), e);
		}
	}

	@Override
	public void createSchema(CreateSchema oper) throws DstExecutionException {
		try {
			if (!sqlGenerator.isSchemaAllowed()) {
				//Skip
				return;
			}
			if (!schemaExists(oper.tbl)) {
				executeUnbatchedOper(oper);
			}
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to create schema : " + e.getMessage(), e);
		}
	}

	@Override
	protected CDCLogPosition readCDCLogPosition(String deviceUUID, String deviceName, ConsolidatorDstTable dstControlTable) throws DstExecutionException {
		String uuid = deviceUUID.replace("'", "''");
		String dname = deviceName.replace("'", "''");
		String sql = "SELECT " + sqlGenerator.getSelectTopClause(1)
				+ "commit_id, cdc_change_number, cdc_log_segment_sequence_number, txn_count FROM "
				+ qualifiedDstTableName("synclite_checkpoint")
				+ " WHERE synclite_device_id = '" + uuid + "' AND synclite_device_name = '" + dname + "'"
				+ " " + sqlGenerator.getRowLimitClause(1);
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
			if (rs.next()) {
				CDCLogPosition logPos = new CDCLogPosition(0, -1, 0, 0);
				logPos.commitId = rs.getLong(1);
				logPos.changeNumber = rs.getLong(2);
				logPos.logSegmentSequenceNumber = rs.getLong(3);
				logPos.txnCount = rs.getLong(4);
				return logPos;
			}
			throw new DstExecutionException("No checkpoint log position found in the destination");
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to read checkpoint log position from destination : " + e.getMessage(), e);
		}
	}

	@Override
	public List<String[]> readTableSchemas(String deviceUUID, String deviceName, int dstIdx) throws DstExecutionException {
		List<String[]> schemas = new ArrayList<>();
		String sql = "SELECT database_name, table_name, prop_value FROM " + qualifiedDstTableName("synclite_consolidator_table_metadata")
				+ " WHERE synclite_device_id = '"
				+ deviceUUID.replace("'", "''") + "' AND synclite_device_name = '"
				+ deviceName.replace("'", "''") + "' AND prop_key = 'create_sql'";
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql)) {
			while (rs.next()) {
				schemas.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3)});
			}
		} catch (SQLException e) {
			// Table may not exist yet on a completely fresh device — return empty
		}
		return schemas;
	}

	@Override
	public long readInitializationStatus(String deviceUUID, String deviceName, int dstIdx) throws DstExecutionException {
		String uuid = deviceUUID.replace("'", "''");
		String dname = deviceName.replace("'", "''");
		String sql = "SELECT " + sqlGenerator.getSelectTopClause(1)
				+ "initialization_status FROM " + qualifiedDstTableName("synclite_checkpoint")
				+ " WHERE synclite_device_id = '"
				+ uuid + "' AND synclite_device_name = '" + dname + "' ORDER BY commit_id DESC "
				+ sqlGenerator.getRowLimitClause(1);
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
			if (rs.next()) {
				return rs.getLong(1);
			}
			return -1;
		} catch (SQLException e) {
			// Table may not exist yet (first time this device is seen on a fresh destination).
			// Return -1 so caller treats it as "not initialized" and proceeds to initialize.
			String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
			if (msg.contains("does not exist") || msg.contains("no such table") || msg.contains("unknown table")) {
				return -1;
			}
			throw new DstExecutionException("Failed to read initialization status from destination : " + e.getMessage(), e);
		}
	}

	@Override
	public void close() throws DstExecutionException {
		closeConn();
	}

	@Override
	public void truncate(TruncateTable oper) throws DstExecutionException {
		try {
			executeUnbatchedOper(oper);
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute truncate: " + e.getMessage(), e);
		}
	}

	@Override
	public void copyTable(CopyTable oper) throws DstExecutionException {
		try {
			executeUnbatchedOper(oper);
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute copy table : " + e.getMessage(), e);
		}
	}

	@Override
	public void copyColumn(CopyColumn oper) throws DstExecutionException {
		try {
			executeUnbatchedOper(oper);
		} catch (DstExecutionException e) {
			throw new DstExecutionException("Failed to execute partial update : " + e.getMessage(), e);
		}
	}


	@Override
	public boolean databaseExists(Table tbl) throws DstExecutionException {
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sqlGenerator.getDatabaseExistsCheckSQL(tbl))) {
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to check database existence for  : " + tbl.id.database + " : " + e.getMessage(), e);
		}
		return false;
	}

	@Override
	public boolean schemaExists(Table tbl) throws DstExecutionException {
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sqlGenerator.getSchemaExistsCheckSQL(tbl))) {
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to check schema existence for  : " + tbl.id.schema , e);
		}
		return false;
	}


	@Override
	public boolean tableExists(Table tbl) throws DstExecutionException {
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sqlGenerator.getTableExistsCheckSQL(tbl))) {
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to check table existence for  : " + tbl , e);
		}

		return false;
	}

	@Override
	public boolean columnExists(Table tbl, Column c) throws DstExecutionException {
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery(sqlGenerator.getColumnExistsCheckSQL(tbl, c))) {
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to check column existence for  : " + c, e);
		}

		return false;
	}


	@Override
	@Deprecated
	public void loadFile(LoadFile oper) throws DstExecutionException {
		//Iterate on all records in the CSV file and perform batched INSERT.
		//You can optimize this implementation for individual targets, based on their abilities to directly load from csv files.
		String sql = oper.getSQL(this.sqlGenerator);
		this.currentSql = sql;
		long loadedRecCnt = 0;
		try {
			checkBatchFlushAndRePrepare(oper);
			PreparedStatement pstmt = conn.prepareStatement(sql);	        

			String[] csvHeader = new String[oper.tbl.columns.size() - oper.defaultValues.size()];
			Column[] colsInCsv = new Column[oper.tbl.columns.size() - oper.defaultValues.size()];			
			int idx=0;
			for (Column c : oper.tbl.columns) {
				if (!oper.defaultValues.containsKey(c.column)) {
					csvHeader[idx] = sqlGenerator.getColumnNameSQL(c);
					colsInCsv[idx] = c;
					++idx;
				}
			}

			try (Reader in = new FileReader(oper.dataFilePath.toString())) {
				Iterable<CSVRecord> records = CSVFormat.DEFAULT
						.withHeader(csvHeader)
						.withFirstRecordAsHeader()
						.withEscape(oper.escape)
						.withQuote(oper.quote)
						.parse(in);

				long batchRecCnt = 0;
				for (CSVRecord record : records) {
					for (idx=0; idx < colsInCsv.length; ++idx) {
						String val = record.get(colsInCsv[idx].column);
						try {
							bindPrepared(pstmt, idx+1, val, colsInCsv[idx], false);
						} catch (Exception e) {
							int argIdx = idx+1;
							this.tracer.error("Failed to bind argument at index " + argIdx + " for INSERT prepared statement ( for LOAD) : " + currentSql + " for table : " + oper.tbl + " for column : " + colsInCsv[idx].column + "(" + colsInCsv[idx].type + "). Failed CSV record dump : " + record.toString() , e);
							throw new SQLException("Failed to bind argument at index " + argIdx + " for INSERT prepared statement ( for LOAD) : " + currentSql + " for table : " + oper.tbl + " for column : " + colsInCsv[idx].column + "(" + colsInCsv[idx].type + "). Failed CSV record dump : " + record.toString() , e);
						}
					}
					pstmt.addBatch();
					++batchRecCnt;

					//TODO cache config values locally to optimize execution
					if (batchRecCnt == ConfLoader.getInstance().getDstInsertBatchSize(dstIndex)) {
						pstmt.executeBatch();
						pstmt.clearBatch();
						batchRecCnt = 0;
						loadedRecCnt += batchRecCnt;
					}
				}
				if (batchRecCnt > 0) {
					pstmt.executeBatch();
					loadedRecCnt += batchRecCnt;
				}
				oper.loadedRecordCount = loadedRecCnt;
			}
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute LoadFile operation : " + sql, e);
		}
	}

	
    public boolean canCreateDatabase() {
    	return false;
    }

}

