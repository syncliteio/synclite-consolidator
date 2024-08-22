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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.DstType;
import com.synclite.consolidator.log.CDCColumnValues;
import com.synclite.consolidator.log.CDCLogRecord;
import com.synclite.consolidator.log.CDCLogSchema;
import com.synclite.consolidator.log.CDCLogSegment;
import com.synclite.consolidator.log.CDCLogSegment.CDCLogSegmentWriter;
import com.synclite.consolidator.log.CommandLogRecord;
import com.synclite.consolidator.log.CommandLogRecord.DDLInfo;
import com.synclite.consolidator.log.CommandLogSegment;
import com.synclite.consolidator.log.CommandLogSegment.CommandLogSegmentReader;
import com.synclite.consolidator.nativedb.DB;
import com.synclite.consolidator.nativedb.DBCallback;
import com.synclite.consolidator.nativedb.PreparedStatement;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.schema.ReplicatorTable;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceReplicator extends DeviceProcessor {

	static {
		try {
			System.loadLibrary("synclite3");
		} catch (Exception e) {    		
			throw e;
		}
	}

	public class ReplayerCallback implements DBCallback{

		public Exception lastException ;

		public ReplayerCallback() {
			this.lastException = null;
		}

		@Override
		public int deliverChanges (
				String database,
				String table,
				String operation,
				long[] beforeImage,
				long[] afterImage
				) {
			return DeviceReplicator.this.getChanges(database, table, operation, beforeImage, afterImage);
		}

		@Override
		public void setException(Exception e) {
			this.lastException = e;
		}

		@Override
		public Exception getException() {
			return this.lastException;
		}

	}

	private class CDCLogger extends Logger{
		//private static final long MAX_LOG_BATCH_SIZE = 1000000;
		private static final long LOG_SEGMENT_SWITCH_LOGCOUNT_THRESHOLD = 1000000;
		private static final long LOG_SEGMENT_SWITCH_DURATION_THRESHOLD = 5000;
		//private CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder;
		private CDCLogSegmentWriter cdcLogSegmentWriter;
		private Device device;
		private long lastLogSegmentCreateTime;

		private CDCLogger(Device device) throws SyncLiteException {
			this.device = device;
			//          initializeLogger();
			//For now set it to current time
			this.lastLogSegmentCreateTime = System.currentTimeMillis();
			//this.cdcLogSegmentWriterHolder = null;
			this.cdcLogSegmentWriter = null;
		}

		/*private void setCDCLogSegmentWriterHolder(CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder) {
            this.cdcLogSegmentWriterHolder = cdcLogSegmentWriterHolder;
        }*/

		protected void setCDCLogSegmentWriter(CDCLogSegmentWriter cdcLogSegmentWriter) {
			this.cdcLogSegmentWriter = cdcLogSegmentWriter;
		}

		public final void log(CDCLogRecord record) throws SyncLiteException {
			try {
				//cdcLogSegmentWriterHolder.writeCDCLog(record);
				cdcLogSegmentWriter.writeCDCLog(record);
				//device.tracer.debug(record.toString());
			} catch(SyncLiteException e) {
				throw new SyncLiteException("Failed to log a CDC log record : ", e);
			}
		}

		public final void logCheckpointRecord(long commitId) throws SyncLiteException {
			try {
				CDCLogRecord checkpointRecord = new CDCLogRecord(commitId, null, null, null, null, OperType.CHECKPOINTTRAN, "CHECKPOINT", DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, null);
				log(checkpointRecord);
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to log a checkpoint log record in cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}
		}

		public final void logCommitAndFlush(long commitId) throws SyncLiteException {
			try {
				//Process the schema stage table if present and append to schema log table
				CDCLogRecord commitRecord = new CDCLogRecord(commitId, null, null, null, null, OperType.COMMITTRAN, "COMMIT", DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, null);
				//cdcLogSegmentWriterHolder.beginTran();
				cdcLogSegmentWriter.beginTran();
				log(commitRecord);
				flushLogSegment();
				//checkAndSwitchLogSegment();
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to log a commit and flush in the cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}
		}

		public final void logBeginRecord(long commitId) throws SyncLiteException {
			try {
				CDCLogRecord beginRecord = new CDCLogRecord(commitId, null, null, null, null, OperType.BEGINTRAN, "BEGIN", DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, null);
				log(beginRecord);
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to log a BEGIN record in cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}

		}
		public final void beginTran(long commitId) throws SyncLiteException {
			try {
				//cdcLogSegmentWriterHolder.beginTran();
				cdcLogSegmentWriter.beginTran();
				logBeginRecord(commitId);
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to begin a new txn in cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}
		}

		public final void flushLogSegment() throws SyncLiteException {
			try {
				//cdcLogSegmentWriterHolder.commitTran();
				cdcLogSegmentWriter.commitTran();
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to perform a commit on cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}
		}
		/*

        private final void initializeLogger() throws SyncLiteException {
            Screen.getInstance().incrTotalCDCLogSegmentCnt(SyncLiteReplicator.this.currentCDCLogSegment.sequenceNumber + 1);
            if (SyncLiteReplicator.this.currentCDCLogSegment.isClosed()) {
                createNewCDCLogSegment();
            }
            SyncLiteReplicator.this.currentCDCLogSegment.load(SyncLiteReplicator.this.commitID);
            //checkAndSwitchLogSegment();
        }

        private final void checkAndSwitchLogSegment() throws SyncLiteException {
            if (SyncLiteReplicator.this.currentCDCLogSegment.logSegmentLogCount > 0) {
                long currentTime = System.currentTimeMillis();
                if ((SyncLiteReplicator.this.currentCDCLogSegment.logSegmentLogCount > LOG_SEGMENT_SWITCH_LOGCOUNT_THRESHOLD) ||
                    ((currentTime - this.lastLogSegmentCreateTime) > LOG_SEGMENT_SWITCH_DURATION_THRESHOLD))
                {
                    doSwitchLogSegment();
                }
            }


       private final void doSwitchLogSegment() throws SyncLiteException {
            SyncLiteReplicator.this.currentCDCLogSegment.markClosed();
            createNewCDCLogSegment();
            SyncLiteReplicator.this.currentCDCLogSegment.load(SyncLiteReplicator.this.commitID);
            if (cdcLogSegmentWriterHolder != null) {
                this.cdcLogSegmentWriterHolder.replaceWriter(SyncLiteReplicator.this.currentCDCLogSegment.new CDCLogSegmentWriter());
            }
            Screen.getInstance().incrTotalCDCLogSegmentCnt(1L);
        }

        private void createNewCDCLogSegment() throws SyncLiteException{
            SyncLiteReplicator.this.currentCDCLogSegment = device.getNewCDCLogSegment(SyncLiteReplicator.this.currentCDCLogSegment.sequenceNumber + 1);
            this.lastLogSegmentCreateTime = System.currentTimeMillis();
        }

		 */
		protected final void closeCurrentCDCLogSegment() throws SyncLiteException {
			DeviceReplicator.this.currentCDCLogSegment.closeAndMarkReady();
		}

		protected final void createNewCDCLogSegment() throws SyncLiteException {
			DeviceReplicator.this.currentCDCLogSegment = device.getNewCDCLogSegment(DeviceReplicator.this.currentCommandLogSegment.sequenceNumber);
			DeviceReplicator.this.currentCDCLogSegment.load(DeviceReplicator.this.commitID);
			this.lastLogSegmentCreateTime = System.currentTimeMillis();
		}

		public final void logDDLRecord(long commitID, DDLInfo ddlInfo) throws SyncLiteException {
			try {
				ReplicatorTable tbl= ReplicatorTable.from(TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), 1, ddlInfo.databaseName, null, ddlInfo.tableName));
				HashMap<String, String> renamedColMap = null;
				if (ddlInfo.oldColumnName != null) {
					renamedColMap = new HashMap<String, String>();
					renamedColMap.put(ddlInfo.columnName, ddlInfo.oldColumnName);
				}
				CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, renamedColMap);
				switch (ddlInfo.ddlType) {
				case CREATETABLE:
					CDCLogRecord record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.CREATETABLE, null, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case DROPTABLE:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.DROPTABLE, null, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case RENAMETABLE:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, ddlInfo.oldTableName , OperType.RENAMETABLE, null, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case ADDCOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.ADDCOLUMN, null, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case DROPCOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.DROPCOLUMN, null, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case RENAMECOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.RENAMECOLUMN, null, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				}
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to log a DDL CDC log record : " + ddlInfo, e);
			}
		}
	}

	private abstract class Logger{
		/*private void setCDCLogSegmentWriterHolder(CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder) {
            this.cdcLogSegmentWriterHolder = cdcLogSegmentWriterHolder;
        }*/
		protected abstract void setCDCLogSegmentWriter(CDCLogSegmentWriter cdcLogSegmentWriter);

		public abstract void log(CDCLogRecord record) throws SyncLiteException;

		public abstract void logCheckpointRecord(long commitId) throws SyncLiteException;

		public abstract void logCommitAndFlush(long commitId) throws SyncLiteException;

		public abstract void logBeginRecord(long commitId) throws SyncLiteException;

		public abstract void beginTran(long commitId) throws SyncLiteException;

		public abstract void flushLogSegment() throws SyncLiteException;

		protected abstract void closeCurrentCDCLogSegment() throws SyncLiteException;

		protected abstract void createNewCDCLogSegment() throws SyncLiteException;

		public abstract void logDDLRecord(long commitID, DDLInfo ddlInfo) throws SyncLiteException;
	}

	private class NullLogger extends Logger{
		/*private void setCDCLogSegmentWriterHolder(CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder) {
            this.cdcLogSegmentWriterHolder = cdcLogSegmentWriterHolder;
        }*/
		protected void setCDCLogSegmentWriter(CDCLogSegmentWriter cdcLogSegmentWriter) {}

		public void log(CDCLogRecord record) throws SyncLiteException {}

		public void logCheckpointRecord(long commitId) throws SyncLiteException {}

		public void logCommitAndFlush(long commitId) throws SyncLiteException {}

		public void logBeginRecord(long commitId) throws SyncLiteException {}

		public void beginTran(long commitId) throws SyncLiteException {}

		public void flushLogSegment() throws SyncLiteException {}

		protected void closeCurrentCDCLogSegment() throws SyncLiteException {}

		protected void createNewCDCLogSegment() throws SyncLiteException {}

		public void logDDLRecord(long commitID, DDLInfo ddlInfo) throws SyncLiteException {}
	}

	private long commitID;
	private long changeNumber;
	private long txnChangeNumber;
	private long processedTxnCount;
	private boolean hasProcessedAllSegments = false;
	private Path replicaPath;
	//    private DB targetReplicaDB;
	private Logger logger;
	private CommandLogSegment currentCommandLogSegment;
	private CDCLogSegment currentCDCLogSegment;
	private static final String createTxnTableSql = "CREATE TABLE IF NOT EXISTS synclite_metadata(commit_id LONG NOT NULL PRIMARY KEY, change_number LONG NOT NULL, txn_change_number LONG NOT NULL, command_log_segment_sequence_number LONG NOT NULL, cdc_change_number LONG NOT NULL, cdc_txn_change_number LONG NOT NULL, cdc_log_segment_sequence_number LONG NOT NULL, txn_count LONG NOT NULL)";
	private static final String selectTxnTableSql = "SELECT commit_id, change_number, txn_change_number, command_log_segment_sequence_number, cdc_change_number, cdc_txn_change_number, cdc_log_segment_sequence_number, txn_count FROM synclite_metadata";
	private static final String updateTxnTableSql = "UPDATE synclite_metadata SET commit_id = ?, change_number = ?, txn_change_number = ?, command_log_segment_sequence_number = ?, cdc_change_number = ?, cdc_txn_change_number = ?, cdc_log_segment_sequence_number = ?, txn_count = ?";
	private static final String insertTxnTableSql = "INSERT INTO synclite_metadata VALUES($1, -1, -1, 0, -1, -1, 0, 0);";
	private static final String firstCommitIDSql = "SELECT commit_id FROM synclite_txn";
	//private PreparedStatement updateTxnTablePstmt;
	private DBCallback callback;
	private DeviceStatsCollector statsCollector;
	private DeviceLogCleaner logCleaner;

	protected DeviceReplicator(Device device, int dstIndex) throws SyncLiteException {
		super(device, dstIndex);
		if (isReplicationToSQLite()) {
			this.callback = null;
		} else {
			this.callback = new ReplayerCallback();
		}

		this.replicaPath = device.getReplica(this.dstIndex);
		/*        try {
            this.targetReplicaDB = new DB(this.replicaPath, this.callback);
        } catch (SQLException e) {
            throw new SyncLiteException("Failed to open replica at path : " + this.replicaPath + " with exception : ", e);
        } */
		initCheckpointTable();
		loadSchemas();
		if (isReplicationToSQLite()) {
			this.logger = new NullLogger();
			this.statsCollector = device.getDeviceStatsCollector(this.dstIndex);
		} else {
			this.logger = new CDCLogger(device);
			this.statsCollector = null;
		}

		//
		//Take this snapshot now so that the checkpoint table synclite_metadata is recorded in the snapshot.
		//
		device.dataBackupSnapshot();
	}

	private final void loadSchemas() throws SyncLiteException {
		device.tracer.info("Loading schemas");
		device.schemaReader.fetchReplicatorTables(this.replicaPath);
		device.tracer.info("Loaded schemas");
	}

	private final void initCheckpointTable() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.replicaPath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(createTxnTableSql);
				try (ResultSet rs = stmt.executeQuery(selectTxnTableSql)) {
					if (rs.next()) {
						this.commitID = rs.getLong("commit_id");
						this.changeNumber = rs.getLong("change_number");
						this.txnChangeNumber = rs.getLong("txn_change_number");
						long commandLogSegmentSequenceNumber = rs.getLong("command_log_segment_sequence_number");
						long cdcLogSegmentSequenceNumber = rs.getLong("cdc_log_segment_sequence_number");
						long cdcLogSegmentChangeNumber = rs.getLong("cdc_change_number");
						this.processedTxnCount = rs.getLong("txn_count");
						this.currentCommandLogSegment = device.getCommandLogSegment(commandLogSegmentSequenceNumber);
						//If we had started the replay then validate that the required command log segment is available
						if (this.changeNumber > -1) {
							if (this.currentCommandLogSegment == null) {
								throw new SyncLiteException("Restart recovery failed. Command log segment with sequence number :" + commandLogSegmentSequenceNumber + " missing from the device");
							}
						}
						this.currentCDCLogSegment = device.getCDCLogSegment(cdcLogSegmentSequenceNumber);
						this.currentCDCLogSegment.load(this.commitID);
						if (this.currentCDCLogSegment == null) {
							throw new SyncLiteException("Restart recovery failed. CDC segment with sequence number :" + cdcLogSegmentSequenceNumber + " missing from the device");
						}

						if (this.currentCommandLogSegment != null) {
							Monitor.getInstance().incrTotalCommandLogSegmentCnt(this.currentCommandLogSegment.sequenceNumber + 1);
						}
						Monitor.getInstance().incrTotalSyncLiteTxnCnt(this.processedTxnCount);
					} else {
						device.tracer.debug("Initializing checkpoint table");
						try (ResultSet rsFirstCommitID = stmt.executeQuery(firstCommitIDSql)) {
							this.commitID = rsFirstCommitID.getLong(1);
						} catch (SQLException e) {
							throw new SyncLiteException("Failed to read initial commit id from replica : " + replicaPath + " with exception : ", e);
						}
						this.changeNumber = -1;
						this.txnChangeNumber = -1;
						this.currentCommandLogSegment = device.getCommandLogSegment(0);
						this.currentCDCLogSegment = device.getNewCDCLogSegment(0);
						this.currentCDCLogSegment.load(DeviceReplicator.this.commitID);
						this.processedTxnCount = 0;
						String insertSql = insertTxnTableSql.replace("$1", String.valueOf(this.commitID));
						stmt.execute(insertSql);
						//Update statsCollector about replica size
						if (statsCollector != null) {
							statsCollector.updateInitialDeviceStatistics(0, 0, device.getReplicaSize());
						}
					}
				}
			}
			//             this.updateTxnTablePstmt = targetReplicaDB.prepare(updateTxnTableSql);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to initialize the replicate checkpoint table in replica : " + replicaPath, e);
		}
		device.updateLastReplicatedCommitID(this.commitID);
	}

	@Override
	public boolean hasMoreWork() {
		return !hasProcessedAllSegments;
	}

	@Override
	public long syncDevice() throws SyncLiteException {
		long replicatedOperCount = 0;
		if (!isInitialCommandLogSegmentAvailable()) {
			//Check if the very first command log segment is available
			this.currentCommandLogSegment = device.getCommandLogSegment(0);
			if (!isInitialCommandLogSegmentAvailable()) {
				this.hasProcessedAllSegments = true;
				return replicatedOperCount;
			}
		}

		boolean lookForNextLogSegment = false;
		if (!currentCommandLogSegment.isApplied()) {
			if (currentCommandLogSegment.isReadyToApply()) {
				device.tracer.info("Started sync for log segment : " + currentCommandLogSegment);
				replicatedOperCount = doSync();
				device.tracer.info("Finished sync for log segment : " + currentCommandLogSegment);
				lookForNextLogSegment = true;
			} else {
				//We should not come here at all ! But if we do then try to re-download the current log segment again.
				this.currentCommandLogSegment = device.getCommandLogSegment(this.currentCommandLogSegment.sequenceNumber);				
				lookForNextLogSegment = false;
				hasProcessedAllSegments = false;
			}
		} else {
			lookForNextLogSegment = true;
		}        

		if (lookForNextLogSegment) {
			CommandLogSegment nextCommandLogSegment = device.getNextCommandLogSegmentToProcess(currentCommandLogSegment);
			if (nextCommandLogSegment != null) {
				currentCommandLogSegment = nextCommandLogSegment;
				device.updateLogsToProcessSince(nextCommandLogSegment.getPublishTime());
				device.setLastHeartbeatTS(nextCommandLogSegment.getPublishTime());
				hasProcessedAllSegments = false;
				logger.createNewCDCLogSegment();
			} else {
				//TODO
				//check publish time for metadata file (an idle device keeps sending metadata file as a heartbeat message at a periodic interval).
				//device.setLastHeartbeatTS(nextCommandLogSegment.getPublishTime());
				hasProcessedAllSegments = true;
				device.updateLogsToProcessSince(Long.MAX_VALUE);
			}
		}
		/*else {
           //If there is no command log segment to apply next
           //then check if the current cdc log segment can be closed
           logger.checkAndSwitchLogSegment();
         }*/		
		return replicatedOperCount;
	}

	private final boolean isInitialCommandLogSegmentAvailable() {
		return (this.currentCommandLogSegment != null);
	}
	/*
    private final long doSync() throws SyncLiteException {
        try {
        currentCommandLogSegment.open(this.changeNumber);
        currentCommandLogSegment.logReaderStmt.bindLong(1, this.changeNumber);
         PreparedStatement logApplierStmt = null;
         int next = currentCommandLogSegment.logReaderStmt.stepQuery();
         if (next == -1) {
             throw new RuntimeException("no logs found");
         }
         boolean tranFinished = true;
         long prevCommitID = this.commitID;
         long commandLogOperCount= 0;
         long currentCommandLogTxnCount = 0;
         while (next != 0) {
             long nextChangeNumber = currentCommandLogSegment.logReaderStmt.getLong(0);
             long nextCommitID = currentCommandLogSegment.logReaderStmt.getLong(1);
             if (tranFinished) {
                 tranFinished = false;
                 beginTran(nextCommitID);
             } else {
                 //Rollback not seen for prevCommitID
                 //Log a ROLLBACK for prevCommitID
                 if (nextCommitID > prevCommitID) {
                     rollbackTran();
                     ++currentCommandLogTxnCount;
                     //Start a new tran as there is more log to replay
                     //keep tranFinished = false here as we are starting a new tran
                     beginTran(nextCommitID);
                 }
             }
             this.commitID = nextCommitID;
             this.changeNumber = nextChangeNumber;
             String sql = currentCommandLogSegment.logReaderStmt.getString(2);
             long argCnt = currentCommandLogSegment.logReaderStmt.getLong(3);

             if ((sql!= null) && (!sql.isEmpty())) {
                 device.tracer.debug("Command log to apply : " + sql + ", change number : " + this.changeNumber);
             }

             if (argCnt == 0) {
                 if (logApplierStmt != null) {
                     //Finalize the current prepared statement
                     logApplierStmt.finalizePrepared();
                     logApplierStmt = null;
                 }
                 if (isCommit(sql)) {
                     ++currentCommandLogTxnCount;
                     ++this.processedTxnCount;
                     commitTran();
                     tranFinished = true;
                 } else if (isRollback(sql)) {
                     //++currentCommandLogTxnCount;
                     //++this.processedTxnCount;
                     rollbackTran();
                     tranFinished = true;
                 } else {
                     //device.tracer.debug("Command log to apply : " + sql + " , change number : " + this.changeNumber);
                     targetReplicaDB.exec(sql);
                 }
             } else {
                 if (logApplierStmt == null) {
                     //new statement to be prepared
                     logApplierStmt = targetReplicaDB.prepare(sql);
                 }
                 if (argCnt <=currentCommandLogSegment.getLogTableArgCnt()) {
                     for (int i = 1; i <= argCnt; i++) {
                         //Bind all arguments
                         logApplierStmt.bindNativeValue(i, currentCommandLogSegment.logReaderStmt.getNativeValue(i+3));
                     }
                 } else {
                     long tableArgCnt = SyncLiteLog.nextPowerOf2(argCnt);
                     PreparedStatement argReaderPStmt = currentCommandLogSegment.argReaderPrepStmts.get(tableArgCnt);
                     argReaderPStmt.bindLong(1, this.changeNumber);
                     argReaderPStmt.stepQuery();
                     for (int i = 1; i <= argCnt; i++) {
                         //Bind all arguments
                         logApplierStmt.bindNativeValue(i, argReaderPStmt.getNativeValue(i-1));
                     }
                 }
                 logApplierStmt.step();
             }
             next = currentCommandLogSegment.logReaderStmt.stepQuery();
             prevCommitID = this.commitID;
             ++commandLogOperCount;
         }
         if (logApplierStmt != null) {
             logApplierStmt.finalizePrepared();
         }
         if (tranFinished == false) {
             ++currentCommandLogTxnCount;
             ++this.processedTxnCount;
             //Last tran in the log has no COMMIT/ROLLBACK.
             rollbackTran();
             tranFinished = true;
         }
         device.updateLastReplicatedCommitID(this.commitID);
         currentCommandLogSegment.close();
         currentCommandLogSegment.setApplied(true);
         this.changeNumber = -1;
         if (commandLogOperCount > 0) {
             device.tracer.debug("Replicated " + commandLogOperCount + " command logs from segment : " + currentCommandLogSegment.path);
             Screen.getInstance().incrTotalSyncLiteTxnCnt(currentCommandLogTxnCount);
         }
         Screen.getInstance().incrTotalCommandLogSegmentCnt(1L);
         return commandLogOperCount;
        } catch(SQLException e) {
            throw new SyncLiteException("Failed to sync command log segment : " + currentCommandLogSegment.path + ", with exception :", e);
        }
     }
	 */

	@Override
	public long consolidateDevice() throws SyncLiteException {
		//Nothing to do here
		return 0;
	}

	private final long doSync() throws SyncLiteException {
		try {
			long commandLogOperCount= 0;
			long currentCommandLogTxnCount = 0;
			try (DB targetReplicaDB = new DB(this.replicaPath, this.callback);
					PreparedStatement updateTxnTablePstmt = targetReplicaDB.prepare(updateTxnTableSql);
					CommandLogSegmentReader reader = currentCommandLogSegment.open(this.commitID);
					//CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder = new CDCLogSegmentWriterHolder(currentCDCLogSegment.getWriter());
					CDCLogSegmentWriter writer = currentCDCLogSegment.getWriter();
					) {
				PreparedStatement logApplierStmt = null;
				//logger.setCDCLogSegmentWriterHolder(cdcLogSegmentWriterHolder);
				logger.setCDCLogSegmentWriter(writer);
				CommandLogRecord log = reader.readNextRecord();
				while (log != null) {
					try {
						this.changeNumber = log.changeNumber;
						this.txnChangeNumber = log.txnChangeNumber;
						this.commitID = log.commitId;

						if ((log.sql!= null) && (!log.sql.isEmpty())) {
							device.tracer.debug("Command log to apply : CN : " + this.changeNumber + " TXN CN : " + this.txnChangeNumber + ", SQL : " + log.sql);
						} 
						
						if (log.argCnt == 0) {
							if (logApplierStmt != null) {
								//Finalize the current prepared statement
								logApplierStmt.finalizePrepared();
								logApplierStmt = null;
							}
							if (log.isBegin()) {
								beginTran(targetReplicaDB, log.commitId);
							} else if (log.isCommit()) {
								++currentCommandLogTxnCount;
								++this.processedTxnCount;
								commitTran(updateTxnTablePstmt, targetReplicaDB);
							} else if (log.isDDL()) {
								executeDDL(targetReplicaDB, updateTxnTablePstmt, log);
								++commandLogOperCount;
							} else if (log.isNoOp()) {
								//Do nothing
								++commandLogOperCount;
							} else {
								targetReplicaDB.exec(log.sql);
								++commandLogOperCount;
							}
						} else {
							if (logApplierStmt == null) {
								//new statement to be prepared
								logApplierStmt = targetReplicaDB.prepare(log.sql);
							} else {
								//If we have an ongoing logApplierStmt and we have got a new log with a non-null 
								//sql then we have to finalize the ongoing one and prepare a new one.  
								if (log.sql != null) {
									logApplierStmt.finalizePrepared();
									logApplierStmt = targetReplicaDB.prepare(log.sql);
								}
							}
							int argIdx = 1;
							for (Object arg : log.argValues) {
								logApplierStmt.bindNativeValue(argIdx, (long) arg);
								++argIdx;
							}
							logApplierStmt.step();
							++commandLogOperCount;
						}

					} catch(SQLException e) {
						//
						//Ignore failing SQLs
						//We are doing this to make replicator more robust.
						//The concern of doing this is we should not miss valid cases where we could not execute a valid SQL here ! 
						//We need to do this since SyncLite logger is now generalized to handle different kinds of device types! 
						//					
						if (e.getMessage().contains("Parse error") || e.getMessage().contains("syntax error")) {
							this.device.tracer.error("Replicator failed to apply SQL : " + log.sql + " on replica : " + this.replicaPath + " : " + e.getMessage(), e);
						} else {
							throw e;
						}
					}
					log = reader.readNextRecord();
				}
				//logger.setCDCLogSegmentWriterHolder(null);
				logger.setCDCLogSegmentWriter(null);
			}
			/*if (logApplierStmt != null) {
                logApplierStmt.finalizePrepared();
            }*/
			if (commandLogOperCount > 0) {
				Monitor.getInstance().incrTotalSyncLiteTxnCnt(currentCommandLogTxnCount);
				Monitor.getInstance().incrTotalCommandLogSegmentCnt(1L);
				//statsCollector will be non-null in a specific case of REPLICATION to SQLITE
				if (statsCollector != null) {
					statsCollector.updateLogStatsForLogSegment(currentCommandLogSegment.sequenceNumber, commandLogOperCount, currentCommandLogTxnCount, currentCommandLogSegment.getSize());
					Monitor.getInstance().registerChangedDevice(device);
				}
			} else {
				//statsCollector will be non-null in a specific case of REPLICATION to SQLITE
				if (statsCollector != null) {
					statsCollector.updateLogStatsForLogSegment(currentCommandLogSegment.sequenceNumber, 0, 0, 0);
				}
			}
			device.updateLastReplicatedCommitID(this.commitID);
			logger.closeCurrentCDCLogSegment();
			//logCleaner will be non-null in a specific case of REPLICATION to SQLITE
			if (logCleaner != null) {
				logCleaner.cleanUpCommandLogs(currentCommandLogSegment.sequenceNumber - 1);
			}
			currentCommandLogSegment.markApplied();
			this.changeNumber = -1;
			this.txnChangeNumber = -1;
			device.tracer.debug("Replicated " + commandLogOperCount + " command logs from segment : " + currentCommandLogSegment.path);
			return commandLogOperCount;
		} catch(SQLException e) {
			throw new SyncLiteException("Failed to sync command log segment : " + currentCommandLogSegment.path + ", with exception :", e);
		}
	}


	private final void executeDDL(DB targetReplicaDB, PreparedStatement updateTxnTablePstmt, CommandLogRecord log) throws SyncLiteException {
		try {
			checkpointTran(updateTxnTablePstmt);
			if (log.ddlInfo.ddlType == OperType.ALTERCOLUMN) {
				//
				//SQLite does not support ALTER COLUMN and does not need it to be applied also due it is dynamic type system.
				//Skip applying it on the replica.
				//But make sure to log it in CDC log, so that it can be replayed on final destination
				//
				logger.logDDLRecord(this.commitID, log.ddlInfo);
			} else {
				targetReplicaDB.exec(log.sql);
				reloadTableSchemas(targetReplicaDB, log.ddlInfo);
				logger.logDDLRecord(this.commitID, log.ddlInfo);
			}
			checkpointTran(updateTxnTablePstmt);
			logger.logBeginRecord(log.commitId);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to execute sql : " + log.sql + " on replica : " + this.replicaPath, e);
		}
	}

	private final void checkpointTran(PreparedStatement updateTxnTablePstmt) throws SyncLiteException {
		executeCheckpointOper(updateTxnTablePstmt, currentCommandLogSegment);
		logger.logCheckpointRecord(this.commitID);
	}

	private final void reloadTableSchemas(DB targetReplicaDB, DDLInfo ddlInfo) throws SyncLiteException {
		ReplicatorTable tbl= ReplicatorTable.from(TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), 1, ddlInfo.databaseName, null, ddlInfo.tableName));
		if (ddlInfo.ddlType == OperType.DROPTABLE) {
			tbl.clearColumns();
		} else {
			device.schemaReader.getReplicatorTableWithRefreshedSchema(targetReplicaDB, tbl.id);
			if (ddlInfo.oldTableName != null) {
				tbl= ReplicatorTable.from(TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), 1, ddlInfo.databaseName, null, ddlInfo.oldTableName));
				device.schemaReader.removeReplicatorTable(tbl.id);
			}
		}
	}

	private final void beginTran(DB targetReplicaDB, long commitId) throws SyncLiteException {
		logger.beginTran(commitId);
		try {
			targetReplicaDB.beginTran();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to begin a transaction on replica : " + this.replicaPath + " with exception : ", e);
		}
	}

	private final void commitTran(PreparedStatement updateTxnTablePstmt, DB targetReplicaDB) throws SyncLiteException {
		executeCheckpointOper(updateTxnTablePstmt, currentCommandLogSegment);
		if ((callback!= null) && (callback.getException() != null)) {
			throw new SyncLiteException("Exception in change processing : ", callback.getException());
		}
		//2PC : replicadb and logdb
		logger.flushLogSegment();
		try {
			//device.tracer.debug("Command log to apply : COMMIT");
			targetReplicaDB.commitTran();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to commit a transaction on replica : " + this.replicaPath + " with exception : ", e);
		}
		logger.logCommitAndFlush(this.commitID);
	}

	private final void executeCheckpointOper(PreparedStatement updateTxnTablePstmt, CommandLogSegment commandLogSegment) throws SyncLiteException {
		try {
			updateTxnTablePstmt.bindLong(1, this.commitID);
			updateTxnTablePstmt.bindLong(2, this.changeNumber);
			updateTxnTablePstmt.bindLong(3, this.txnChangeNumber);
			updateTxnTablePstmt.bindLong(4, commandLogSegment.sequenceNumber);
			updateTxnTablePstmt.bindLong(5, currentCDCLogSegment.logSegmentLogCount + 1);
			updateTxnTablePstmt.bindLong(6, -1);
			updateTxnTablePstmt.bindLong(7, currentCDCLogSegment.sequenceNumber);
			updateTxnTablePstmt.bindLong(8, this.processedTxnCount);
			updateTxnTablePstmt.step();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to bind arguments for a checkpoint statement in replica : " + this.replicaPath + " with exception : ", e);
		}
	}

	private int getChanges(
			String database,
			String table,
			String operation,
			//byte[][] beforeImage,
			//byte[][] afterImage
			long[] beforeImage,
			long[] afterImage
			){
		//long numFields = (beforeImage != null) ? beforeImage.length : afterImage.length;

		ReplicatorTable tbl = ReplicatorTable.from(TableID.from(device.getDeviceUUID(), device.getDeviceName(), 1, database, null, table));
		if (tbl.columns.isEmpty()) {
			String erroMsg = tbl.id + " : Table missing, log received for a missing table";
			device.tracer.error(erroMsg);
			//traceChange(database, table, operation, beforeImage, afterImage);
			this.callback.setException(new SyncLiteException(tbl.id + " : " + erroMsg));
		}
		/*
        ReplicatorTable tbl = null;

        try {

            tbl = device.schemaReader.getReplicatorTableNoSchemaFetch(TableID.from(device.getDeviceUUID(), device.getDeviceName(), database, null, table));

            if (tbl.columns.isEmpty()) {
                //We do not know about this table as of now.
                //This means, we have received the first operation for this table
                //Try getting the table with fetched schema.

                tbl = device.schemaReader.getReplicatorTable(TableID.from(device.getDeviceUUID(), device.getDeviceName(), database, null, table));

                if (!tbl.columns.isEmpty()) {
                    //We got the schema for this table
                    //Log CREATE TABLE for this table
                    CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, false, null);
                    CDCLogRecord record = new CDCLogRecord(this.commitID, database, null, table, null, "SCHEMA", null, logger.logSegmentLogCount, null, colSchemas);
                    logger.log(record);
                } else {
                    //Schema is empty.
                    //Table does not exist as of now in the replica
                    //It must have been created by the ongoing transaction
                    //Use a mocked table and log a schema operation.
                    tbl = device.schemaReader.getReplicatorTableWithMockedSchema(tbl.id, numFields);
                    CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, true, null);
                    CDCLogRecord record = new CDCLogRecord(this.commitID, database, null, table, null, "SCHEMA", null, logger.logSegmentLogCount, null, colSchemas);
                    logger.log(record);
                }
            }

            if (tbl.columns.size() < numFields) {
                //New column(s) added to the table recently.
                //Try to refresh the table schema
                tbl = device.schemaReader.getReplicatorTableWithRefreshedSchema(tbl.id);
                if (tbl.columns.size() == numFields) {
                    //Refreshed schema now matches numFields.
                    //Log a schema operation
                    CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, false, null);
                    CDCLogRecord record = new CDCLogRecord(this.commitID, database, null, table, null, "SCHEMA", null, logger.logSegmentLogCount, null, colSchemas);
                    logger.log(record);
                } else {
                    //Refreshed schema still does not match numFields.
                    //The table must have been altered as part of the ongoing transaction
                    //Add mock columns and log a schema operation to schema_stage
                    tbl = device.schemaReader.getReplicatorTableWithMockedSchema(tbl.id, numFields);
                    CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, true, null);
                    CDCLogRecord record = new CDCLogRecord(this.commitID, database, null, table, null, "SCHEMA", null, logger.logSegmentLogCount, null, colSchemas);
                    logger.log(record);
                }
            }
        } catch (SyncLiteException e) {
            this.callback.setException(e);
        }
		 */

		if (beforeImage != null) {
			if (beforeImage.length != tbl.columns.size()) {
				String erroMsg = tbl.id + " : Number of columns in received change before image  is : " + beforeImage.length + " is different than Table column count : " + tbl.columns.size();
				device.tracer.error(erroMsg);
				//traceChange(database, table, operation, beforeImage, afterImage);
				this.callback.setException(new SyncLiteException(tbl.id + " : " + erroMsg));
			}
		}

		if (afterImage != null) {
			if (afterImage.length != tbl.columns.size()) {
				String erroMsg = tbl.id + " : Number of columns in received change after image  is : " + afterImage.length + " is different than Table column count : " + tbl.columns.size();
				device.tracer.error(erroMsg);
				//traceChange(database, table, operation, beforeImage, afterImage);
				this.callback.setException(new SyncLiteException(erroMsg));
			}
		}

		if (operation.equals("INSERT")) {
			List<CDCColumnValues> colValuesList = new ArrayList<CDCColumnValues>();
			for (int i=0 ; i < afterImage.length; ++i) {
				CDCColumnValues colValues = new CDCColumnValues(i, 0, afterImage[i]);
				colValuesList.add(colValues);
			}
			try {
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.INSERT, null, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
				logger.log(logRecord);
			} catch (SyncLiteException e) {
				this.callback.setException(e);
			}
		} else if (operation.equals("UPDATE")) {
			List<CDCColumnValues> colValuesList = new ArrayList<CDCColumnValues>();
			for (int i=0 ; i < afterImage.length; ++i) {
				CDCColumnValues colValues = new CDCColumnValues(i, beforeImage[i], afterImage[i]);
				colValuesList.add(colValues);
			}
			try {
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.UPDATE, null, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
				logger.log(logRecord);
			} catch (SyncLiteException e) {
				this.callback.setException(e);
			}
		} else if (operation.equals("DELETE")) {
			List<CDCColumnValues> colValuesList = new ArrayList<CDCColumnValues>();
			for (int i=0 ; i < beforeImage.length; ++i) {
				CDCColumnValues colValues = new CDCColumnValues(i, beforeImage[i], 0);
				colValuesList.add(colValues);
			}
			try {
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.DELETE, null, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
				logger.log(logRecord);
			} catch (SyncLiteException e) {
				this.callback.setException(e);
			}
		}
		return 0;
	}

	private final void traceChange(String database, String table, String operation, long[] beforeImage, long[] afterImage) {

		/*         tracer.trace("Change Received for database : " + database + " and table : " + table + " and oper : " + operation);
        tracer.trace("Before image : ");
        if (beforeImage != null) {
            for (int i=0; i <beforeImage.length; i++) {
                String o = new String (beforeImage[i]);
                tracer.trace(o + ",");
            }
        }
        tracer.trace("");
        if (afterImage != null) {
            tracer.trace("After image : ");
            for (int i=0; i <afterImage.length; i++) {
                String o = new String (afterImage[i]);
                tracer.trace(o + ",");
            }
        }
		 */
		device.tracer.info("Change Received for database : " + database + " and table : " + table + " and oper : " + operation);
		device.tracer.info("Before image : ");
		if (beforeImage != null) {
			for (int i=0; i <beforeImage.length; i++) {
				device.tracer.info(beforeImage[i] + ",");
			}
		}
		device.tracer.info("");
		if (afterImage != null) {
			device.tracer.info("After image : ");
			for (int i=0; i <afterImage.length; i++) {
				device.tracer.info(afterImage[i] + ",");
			}
		}
	}

	private boolean isReplicationToSQLite() {
		if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.REPLICATION) {
			if (ConfLoader.getInstance().getDstType(dstIndex) == DstType.SQLITE) {			
				return true;
			}
		}
		return false;
	}
	/*
     // Test Driver
     public static void main(String[] args) {

        Replayer replayer = new Replayer("/home/ubuntu/sqlite/sqlitewrap_2edc3610-2fd3-47f8-9cb7-9a461dafd973/data.db.sqlitewrap.log.0", "/home/ubuntu/sqlite/sqlite/replay.db");
        replayer.replay();
        //LogApplier applier = LogApplier();
        //long db = replayer.open("/home/ubuntu/sqlite/sqlite/replay.db");
        /*replayer.exec(db, "create table if not exists t2 (a int, b text, c blob)");
        replayer.exec(db, "begin transaction;");
        long pstmt = replayer.prepare(db, "insert into t2 values(?, ?, ?)");
        for (int i = 0; i < 100; i++) {
            Integer a = new Integer(i);
            replayer.bindBlob(pstmt, 1, a.toString().getBytes());
            replayer.bindBlob(pstmt, 3, a.toString().getBytes());
            replayer.step(db, pstmt);
        }
        replayer.finalizePrepared(db, pstmt);
        replayer.exec(db, "commit transaction;");*/
	/*replayer.exec("create table if not exists t2 (a int, b text, c blob)");
        replayer.exec("insert into t2 values(1, 1 , 'asd')");
        replayer.exec("insert into t2 values(2, '2', 'asd')");
        replayer.exec("update t2 set a = 5");*/
	//replayer.replay("delete from t2");


	//replayer.close(db);
	//}

}


