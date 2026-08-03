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
import com.synclite.consolidator.device.DeviceStatus;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.DstType;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
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
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ReplicatorTable;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceReplicator extends DeviceProcessor {

	static {
		try {
	        String arch = System.getProperty("os.arch");
	        if (arch.equals("x86")) {
				System.loadLibrary("synclitecdc_x86");
	        } else if (arch.equals("amd64") || arch.equals("x86_64")) {
	        	System.loadLibrary("synclitecdc_x86_64");
	        } else {	        
	        	//throw new SyncLiteException("SyncLite native library not supported for current architecture : " + arch);
	        	//try to load the x86_64 one 
	        	System.loadLibrary("synclitecdc_x86_64");
	        }
		} catch (Exception e) {    		
			throw new RuntimeException("Failed to load SyncLite native library : " + e.getMessage(), e);
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

		public final void logCommitAndFlush(long commitId) throws SyncLiteException {
			try {
				//Process the schema stage table if present and append to schema log table
				CDCLogRecord commitRecord = new CDCLogRecord(commitId, null, null, null, null, OperType.COMMITTRAN, "COMMIT", DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, null);
				//cdcLogSegmentWriterHolder.beginTran();
				device.tracer.debug("CDCLOGGER: logCommitAndFlush commitId=" + commitId + " writer=" + (cdcLogSegmentWriter != null ? "SET" : "NULL"));
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
				device.tracer.debug("CDCLOGGER: beginTran commitId=" + commitId + " writer=" + (cdcLogSegmentWriter != null ? "SET" : "NULL"));
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

		public final void rollbackTran() throws SyncLiteException {
			try {
				cdcLogSegmentWriter.rollbackTran();
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to rollback CDC log transaction in cdc log segment : " + DeviceReplicator.this.currentCDCLogSegment.path + " with exception : ", e);
			}
		}

		protected final void closeCurrentCDCLogSegment() throws SyncLiteException {
			DeviceReplicator.this.currentCDCLogSegment.closeAndMarkReady();
		}

		protected final void createNewCDCLogSegment() throws SyncLiteException {
			DeviceReplicator.this.currentCDCLogSegment = device.getNewCDCLogSegment(DeviceReplicator.this.currentCommandLogSegment.sequenceNumber);
			DeviceReplicator.this.currentCDCLogSegment.load(DeviceReplicator.this.commitID);
			this.lastLogSegmentCreateTime = System.currentTimeMillis();
		}

		public final void logDDLRecord(long commitID, DDLInfo ddlInfo, String sql) throws SyncLiteException {
			try {
				ReplicatorTable tbl= ReplicatorTable.from(TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), 1, ddlInfo.databaseName, null, ddlInfo.tableName));
				HashMap<String, String> renamedColMap = null;
				if (ddlInfo.oldColumnName != null) {
					renamedColMap = new HashMap<String, String>();
					renamedColMap.put(ddlInfo.columnName, ddlInfo.oldColumnName);
				}
				CDCLogSchema colSchemas = new CDCLogSchema(tbl.columns, renamedColMap);
				String ddlSql = DeviceReplicator.this.buildDDLSql(ddlInfo, tbl, colSchemas, sql);
				switch (ddlInfo.ddlType) {
				case CREATETABLE:
					CDCLogRecord record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.CREATETABLE, ddlSql, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case DROPTABLE:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.DROPTABLE, ddlSql, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case RENAMETABLE:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, ddlInfo.oldTableName , OperType.RENAMETABLE, ddlSql, currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case ADDCOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.ADDCOLUMN, ddlSql, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case DROPCOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.DROPCOLUMN, ddlSql, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case RENAMECOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.RENAMECOLUMN, ddlSql, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
					log(record);
					break;
				case ALTERCOLUMN:
					record = new CDCLogRecord(commitID, tbl.id.database, null, tbl.id.table, null, OperType.ALTERCOLUMN, ddlSql, DeviceReplicator.this.currentCDCLogSegment.logSegmentLogCount, null, colSchemas);
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

		public abstract void logCommitAndFlush(long commitId) throws SyncLiteException;

		public abstract void logBeginRecord(long commitId) throws SyncLiteException;

		public abstract void beginTran(long commitId) throws SyncLiteException;

		public abstract void flushLogSegment() throws SyncLiteException;

		protected abstract void closeCurrentCDCLogSegment() throws SyncLiteException;

		protected abstract void createNewCDCLogSegment() throws SyncLiteException;

		public abstract void logDDLRecord(long commitID, DDLInfo ddlInfo, String sql) throws SyncLiteException;

		public abstract void rollbackTran() throws SyncLiteException;
	}

	private class NullLogger extends Logger{
		/*private void setCDCLogSegmentWriterHolder(CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder) {
            this.cdcLogSegmentWriterHolder = cdcLogSegmentWriterHolder;
        }*/
		protected void setCDCLogSegmentWriter(CDCLogSegmentWriter cdcLogSegmentWriter) {}

		public void log(CDCLogRecord record) throws SyncLiteException {}

		public void logCommitAndFlush(long commitId) throws SyncLiteException {}

		public void logBeginRecord(long commitId) throws SyncLiteException {}

		public void beginTran(long commitId) throws SyncLiteException {}

		public void flushLogSegment() throws SyncLiteException {}

		protected void closeCurrentCDCLogSegment() throws SyncLiteException {}

		protected void createNewCDCLogSegment() throws SyncLiteException {}

		public void logDDLRecord(long commitID, DDLInfo ddlInfo, String sql) throws SyncLiteException {}

		public void rollbackTran() throws SyncLiteException {}
	}

	private long commitID;
	private long changeNumber;
	private long txnChangeNumber;
	private long processedTxnCount;
	private boolean hasProcessedAllSegments = false;
	private Path replicaPath;
	private Logger logger;
	private CommandLogSegment currentCommandLogSegment;
	private CDCLogSegment currentCDCLogSegment;
	private String lastCallbackSql;
	private static final String createReplayCheckpointTableSql =
		"CREATE TABLE IF NOT EXISTS replay_checkpoint(commandlog_segment_sequence_number LONG NOT NULL, commit_id LONG NOT NULL, cdc_log_segment_sequence_number LONG NOT NULL)";
	private static final String selectReplayCheckpointSql =
		"SELECT commandlog_segment_sequence_number, commit_id, cdc_log_segment_sequence_number FROM replay_checkpoint ORDER BY commandlog_segment_sequence_number DESC, commit_id DESC LIMIT 1";
	private static final String updateReplayCheckpointSql =
		"UPDATE replay_checkpoint SET commandlog_segment_sequence_number = ?, commit_id = ?, cdc_log_segment_sequence_number = ?";
	private static final String insertReplayCheckpointSql =
		"INSERT INTO replay_checkpoint(commandlog_segment_sequence_number, commit_id, cdc_log_segment_sequence_number) VALUES($1, $2, $3)";
	private static final String firstCommitIDSql = "SELECT commit_id FROM synclite_txn";
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
		initCheckpointTable();
		loadSchemas();
		if (isReplicationToSQLite()) {
			this.logger = new NullLogger();
			this.statsCollector = device.getDeviceStatsCollector(this.dstIndex);
			this.logCleaner = DeviceLogCleaner.getInstance(device);
		} else {
			this.logger = new CDCLogger(device);
			this.statsCollector = null;
			this.logCleaner = null;
		}
		this.lastCallbackSql = null;

		//
		//Take this snapshot now so that replay checkpoint state is included in the snapshot.
		//
		device.dataBackupSnapshot();
		device.updateDeviceStatus(DeviceStatus.SYNCING, "");
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
				stmt.execute(createReplayCheckpointTableSql);
				try (ResultSet rs = stmt.executeQuery(selectReplayCheckpointSql)) {
					if (rs.next()) {
						long commandLogSegmentSequenceNumber = rs.getLong("commandlog_segment_sequence_number");
						this.commitID = rs.getLong("commit_id");
					long cdcLogSegmentSequenceNumber = rs.getLong("cdc_log_segment_sequence_number");
					this.changeNumber = -1;
					this.txnChangeNumber = -1;
					this.processedTxnCount = 0;
					this.currentCommandLogSegment = device.getCommandLogSegment(commandLogSegmentSequenceNumber);
					if (this.currentCommandLogSegment == null) {
						// The persisted checkpoint may still be at the initial first-init state
						// (seq=0, cdcSeq=0, commitID == replica's initial synclite_txn commit_id)
						// when the device was registered but no sqllog has been published yet.
						// In that case, tolerate the missing sqllog — first-init does the same
						// (it inserts (0, initCommitID, 0) without requiring sqllog 0 to exist)
						// and the replicator's normal loop waits for the first sqllog to arrive.
						long replicaInitialCommitID = -1L;
						try (ResultSet rsInit = stmt.executeQuery(firstCommitIDSql)) {
							if (rsInit.next()) {
								replicaInitialCommitID = rsInit.getLong(1);
							}
						} catch (SQLException ignored) {
							// Best-effort: if we can't read the initial commit id we'll fall
							// through to the strict failure below.
						}
						boolean atInitialState =
								commandLogSegmentSequenceNumber == 0
								&& cdcLogSegmentSequenceNumber == 0
								&& this.commitID == replicaInitialCommitID;
						if (!atInitialState) {
							throw new SyncLiteException("Restart recovery failed. Command log segment with sequence number :" + commandLogSegmentSequenceNumber + " missing from the device");
						}
						device.tracer.info("Replay checkpoint at initial first-init state (no sqllog published yet) for device : " + device + "; waiting for first sqllog to arrive");
						if (!isReplicationToSQLite()) {
							this.currentCDCLogSegment = device.getCDCLogSegment(0);
							if (this.currentCDCLogSegment == null) {
								this.currentCDCLogSegment = device.getNewCDCLogSegment(0);
							}
							this.currentCDCLogSegment.load(this.commitID);
						}
						Monitor.getInstance().incrTotalSyncLiteTxnCnt(this.processedTxnCount);
					} else {
					device.tracer.info("Replay checkpoint recovered : commandlogSeq=" + commandLogSegmentSequenceNumber + " commitID=" + this.commitID + " cdclogSeq=" + cdcLogSegmentSequenceNumber);
					if (!isReplicationToSQLite()) {
						this.currentCDCLogSegment = device.getCDCLogSegment(cdcLogSegmentSequenceNumber);
						if (this.currentCDCLogSegment == null) {
							this.currentCDCLogSegment = device.getNewCDCLogSegment(cdcLogSegmentSequenceNumber);
						}
						this.currentCDCLogSegment.load(this.commitID);
					}
						if (this.currentCommandLogSegment != null) {
							Monitor.getInstance().incrTotalCommandLogSegmentCnt(this.currentCommandLogSegment.sequenceNumber + 1);
						}
						Monitor.getInstance().incrTotalSyncLiteTxnCnt(this.processedTxnCount);
					}
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
					if (!isReplicationToSQLite()) {
						this.currentCDCLogSegment = device.getNewCDCLogSegment(0);
						this.currentCDCLogSegment.load(DeviceReplicator.this.commitID);
					}
						this.processedTxnCount = 0;
						String insertSql = insertReplayCheckpointSql
							.replace("$1", String.valueOf(0))
						.replace("$2", String.valueOf(this.commitID))
						.replace("$3", String.valueOf(0));
						stmt.execute(insertSql);
						//Update statsCollector about replica size
						if (statsCollector != null) {
							statsCollector.updateInitialDeviceStatistics(0, 0, device.getReplicaSize());
						}
					}
				}
			}
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

	@Override
	public long consolidateDevice() throws SyncLiteException {
		//Nothing to do here
		return 0;
	}

	private final long doSync() throws SyncLiteException {
		//
		//Idempotency guard (mirrors the Rust replicator's "cdclog already produced" skip,
		//and DeviceConsolidator/DeviceEventStreamer's isApplied() checks).
		//If the CDC log segment for this command log segment has already been fully
		//produced (populated + marked READY_TO_APPLY, or already consolidated+APPLIED),
		//we must NOT re-replay. Re-opening a populated cdclog and re-inserting records
		//collides on the change_number PRIMARY KEY (e.g. a duplicate BEGINTRAN). This
		//can happen when the same segment is re-processed (e.g. another consolidator
		//already produced/consumed the cdclog). Just mark the command log applied and
		//advance so the next segment is picked up on the following cycle.
		//
		if ((currentCDCLogSegment != null)
				&& (currentCDCLogSegment.isApplied() || currentCDCLogSegment.isReadyToApply())) {
			device.tracer.info("Skipping replication for command log segment : " + currentCommandLogSegment
					+ " : CDC log segment already produced : " + currentCDCLogSegment);
			currentCommandLogSegment.markApplied();
			return 0;
		}
		try {
			long commandLogOperCount= 0;
			long currentCommandLogTxnCount = 0;
			try (DB targetReplicaDB = new DB(this.replicaPath, this.callback);
					PreparedStatement updateTxnTablePstmt = targetReplicaDB.prepare(updateReplayCheckpointSql);
					CommandLogSegmentReader reader = currentCommandLogSegment.open(this.commitID);
						//CDCLogSegmentWriterHolder cdcLogSegmentWriterHolder = new CDCLogSegmentWriterHolder(currentCDCLogSegment.getWriter());
					CDCLogSegmentWriter writer = (currentCDCLogSegment != null) ? currentCDCLogSegment.getWriter() : null;
					) {
				PreparedStatement logApplierStmt = null;
				//logger.setCDCLogSegmentWriterHolder(cdcLogSegmentWriterHolder);
				if (currentCDCLogSegment != null) {
					device.tracer.debug("REPLICATOR: setCDCLogSegmentWriter on cdclog segment : " + currentCDCLogSegment.path);
				}
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
								device.tracer.debug("REPLICATOR: BEGIN commitId=" + log.commitId);
								beginTran(targetReplicaDB, log.commitId);
							} else if (log.isCommit()) {
								device.tracer.debug("REPLICATOR: COMMIT commitId=" + log.commitId);
								++currentCommandLogTxnCount;
								++this.processedTxnCount;
								commitTran(updateTxnTablePstmt, targetReplicaDB);
							} else if (log.isRollback()) {
								device.tracer.debug("REPLICATOR: ROLLBACK commitId=" + log.commitId);
								// Roll back both the CDC log transaction and the replica transaction.
								// This ensures no CDC records are written for a rolled-back replica transaction.
								++currentCommandLogTxnCount;
								rollbackTran(targetReplicaDB);
							} else if (log.isDDL()) {
								device.tracer.debug("REPLICATOR: DDL commitId=" + log.commitId + " type=" + log.ddlInfo.ddlType + " sql=" + log.sql);
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
				device.tracer.debug("REPLICATOR: clearing CDCLogSegmentWriter, total cmdlog opers=" + commandLogOperCount);
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
			if (currentCDCLogSegment != null) {
				device.tracer.debug("REPLICATOR: closeCurrentCDCLogSegment - cdclog segment : " + currentCDCLogSegment.path);
			}
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
			// DDL is replayed in-line and logged as a regular CDC DDL record.
			if (log.ddlInfo.ddlType == OperType.ALTERCOLUMN) {
				//
				//SQLite does not support ALTER COLUMN and does not need it to be applied also due it is dynamic type system.
				//Skip applying it on the replica.
				//But make sure to log it in CDC log, so that it can be replayed on final destination
				//
				logger.logDDLRecord(this.commitID, log.ddlInfo, log.sql);
			} else {
				device.tracer.debug("REPLICATOR: executeDDL - exec on replica: " + log.sql);
				targetReplicaDB.exec(log.sql);
				device.tracer.debug("REPLICATOR: executeDDL - reloadTableSchemas");
				reloadTableSchemas(targetReplicaDB, log.ddlInfo);
				device.tracer.debug("REPLICATOR: executeDDL - logger.logDDLRecord");
				logger.logDDLRecord(this.commitID, log.ddlInfo, log.sql);
			}
			device.tracer.debug("REPLICATOR: executeDDL complete");
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to execute sql : " + log.sql + " on replica : " + this.replicaPath, e);
		}
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
		device.tracer.debug("REPLICATOR: beginTran - logger.beginTran commitId=" + commitId);
		this.lastCallbackSql = null;
		logger.beginTran(commitId);
		device.tracer.debug("REPLICATOR: beginTran - targetReplicaDB.beginTran");
		try {
			targetReplicaDB.beginTran();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to begin a transaction on replica : " + this.replicaPath + " with exception : ", e);
		}
	}

	private final void commitTran(PreparedStatement updateTxnTablePstmt, DB targetReplicaDB) throws SyncLiteException {
		device.tracer.debug("REPLICATOR: commitTran - executeCheckpointOper");
		executeCheckpointOper(updateTxnTablePstmt, currentCommandLogSegment);
		if ((callback!= null) && (callback.getException() != null)) {
			throw new SyncLiteException("Exception in change processing : ", callback.getException());
		}
		//2PC : replicadb and logdb
		device.tracer.debug("REPLICATOR: commitTran - logger.flushLogSegment");
		logger.flushLogSegment();
		device.tracer.debug("REPLICATOR: commitTran - targetReplicaDB.commitTran");
		try {
			targetReplicaDB.commitTran();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to commit a transaction on replica : " + this.replicaPath + " with exception : ", e);
		}
		device.tracer.debug("REPLICATOR: commitTran - logger.logCommitAndFlush commitId=" + this.commitID);
		logger.logCommitAndFlush(this.commitID);
		this.lastCallbackSql = null;
		device.tracer.debug("REPLICATOR: commitTran complete");
	}

	private final void rollbackTran(DB targetReplicaDB) throws SyncLiteException {
		// First rollback the CDC log transaction to discard any DML records for this transaction.
		// This preserves the guarantee: if replica rolls back, no CDC records are written.
		logger.rollbackTran();
		try {
			targetReplicaDB.rollbackTran();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to rollback a transaction on replica : " + this.replicaPath + " with exception : ", e);
		}
		this.lastCallbackSql = null;
	}

	private final void executeCheckpointOper(PreparedStatement updateTxnTablePstmt, CommandLogSegment commandLogSegment) throws SyncLiteException {
		try {
			updateTxnTablePstmt.bindLong(1, commandLogSegment.sequenceNumber);
			updateTxnTablePstmt.bindLong(2, this.commitID);
			updateTxnTablePstmt.bindLong(3, currentCDCLogSegment != null ? currentCDCLogSegment.sequenceNumber : commandLogSegment.sequenceNumber);
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
		if (table != null && table.equalsIgnoreCase("replay_checkpoint")) {
			return 0;
		}
		//long numFields = (beforeImage != null) ? beforeImage.length : afterImage.length;

		ReplicatorTable tbl = ReplicatorTable.from(TableID.from(device.getDeviceUUID(), device.getDeviceName(), 1, database, null, table));
		if (tbl.columns.isEmpty()) {
			String erroMsg = tbl.id + " : Table missing, log received for a missing table";
			device.tracer.error(erroMsg);
			//traceChange(database, table, operation, beforeImage, afterImage);
			this.callback.setException(new SyncLiteException(tbl.id + " : " + erroMsg));
		}
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

		String synthesizedSql = buildCallbackSql(tbl, operation, beforeImage, afterImage);
		String sqlForRecord = synthesizedSql;
		if (synthesizedSql != null && synthesizedSql.equals(this.lastCallbackSql)) {
			sqlForRecord = null;
		} else {
			this.lastCallbackSql = synthesizedSql;
		}

		if (operation.equals("INSERT")) {
			List<CDCColumnValues> colValuesList = new ArrayList<CDCColumnValues>();
			for (int i=0 ; i < afterImage.length; ++i) {
				CDCColumnValues colValues = new CDCColumnValues(i, 0, afterImage[i]);
				colValuesList.add(colValues);
			}
			try {
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.INSERT, sqlForRecord, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
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
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.UPDATE, sqlForRecord, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
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
				CDCLogRecord logRecord = new CDCLogRecord(this.commitID, database, null, table, null, OperType.DELETE, sqlForRecord, currentCDCLogSegment.logSegmentLogCount, colValuesList, null);
				logger.log(logRecord);
			} catch (SyncLiteException e) {
				this.callback.setException(e);
			}
		}
		return 0;
	}

	private String buildCallbackSql(ReplicatorTable tbl, String operation, long[] beforeImage, long[] afterImage) {
		String tableName = tbl.id.table;
		if (operation.equals("INSERT") && afterImage != null) {
			int n = afterImage.length;
			String placeholders = buildPlaceholders(n);
			if (tbl.columns.size() == n) {
				return "INSERT INTO " + tableName + " (" + joinColumnNames(tbl, n) + ") VALUES (" + placeholders + ")";
			}
			return "INSERT INTO " + tableName + " VALUES (" + placeholders + ")";
		}
		if (operation.equals("UPDATE") && beforeImage != null && afterImage != null) {
			int n = afterImage.length;
			if (n > 0 && beforeImage.length == n && tbl.columns.size() >= n) {
				StringBuilder setClause = new StringBuilder();
				StringBuilder whereClause = new StringBuilder();
				for (int i = 0; i < n; ++i) {
					if (i > 0) {
						setClause.append(", ");
						whereClause.append(" AND ");
					}
					String colName = tbl.columns.get(i).column;
					setClause.append(colName).append(" = ?");
					whereClause.append(colName).append(" = ?");
				}
				return "UPDATE " + tableName + " SET " + setClause + " WHERE " + whereClause;
			}
		}
		if (operation.equals("DELETE") && beforeImage != null) {
			int n = beforeImage.length;
			if (n > 0 && tbl.columns.size() >= n) {
				StringBuilder whereClause = new StringBuilder();
				for (int i = 0; i < n; ++i) {
					if (i > 0) {
						whereClause.append(" AND ");
					}
					String colName = tbl.columns.get(i).column;
					whereClause.append(colName).append(" = ?");
				}
				return "DELETE FROM " + tableName + " WHERE " + whereClause;
			}
		}
		return operation;
	}

	private String joinColumnNames(ReplicatorTable tbl, int n) {
		StringBuilder out = new StringBuilder();
		for (int i = 0; i < n; ++i) {
			if (i > 0) {
				out.append(", ");
			}
			out.append(tbl.columns.get(i).column);
		}
		return out.toString();
	}

	private String buildPlaceholders(int n) {
		StringBuilder out = new StringBuilder();
		for (int i = 0; i < n; ++i) {
			if (i > 0) {
				out.append(", ");
			}
			out.append("?");
		}
		return out.toString();
	}

	private String buildDDLSql(DDLInfo ddlInfo, ReplicatorTable tbl, CDCLogSchema colSchemas, String fallbackSql) {
		switch (ddlInfo.ddlType) {
		case CREATETABLE:
			return buildCreateTableSql(tbl.id.table, colSchemas);
		case DROPTABLE:
			return "DROP TABLE " + tbl.id.table;
		case RENAMETABLE:
			if (ddlInfo.oldTableName != null) {
				return "ALTER TABLE " + ddlInfo.oldTableName + " RENAME TO " + tbl.id.table;
			}
			break;
		case ADDCOLUMN:
			if (ddlInfo.columnName != null) {
				String colDef = ddlInfo.colDef == null ? "" : ddlInfo.colDef;
				return "ALTER TABLE " + tbl.id.table + " ADD COLUMN " + ddlInfo.columnName + colDef;
			}
			break;
		case DROPCOLUMN:
			if (ddlInfo.columnName != null) {
				return "ALTER TABLE " + tbl.id.table + " DROP COLUMN " + ddlInfo.columnName;
			}
			break;
		case RENAMECOLUMN:
			if (ddlInfo.oldColumnName != null && ddlInfo.columnName != null) {
				return "ALTER TABLE " + tbl.id.table + " RENAME COLUMN " + ddlInfo.oldColumnName + " TO " + ddlInfo.columnName;
			}
			break;
		case ALTERCOLUMN:
			if (ddlInfo.columnName != null) {
				String colDef = ddlInfo.colDef == null ? "" : ddlInfo.colDef;
				return "ALTER TABLE " + tbl.id.table + " ALTER COLUMN " + ddlInfo.columnName + colDef;
			}
			break;
		default:
			break;
		}
		return fallbackSql;
	}

	private String buildCreateTableSql(String tableName, CDCLogSchema colSchemas) {
		if (colSchemas == null || colSchemas.columns == null || colSchemas.columns.isEmpty()) {
			return "CREATE TABLE " + tableName;
		}

		StringBuilder sql = new StringBuilder("CREATE TABLE ");
		sql.append(tableName).append(" (");
		StringBuilder pkCols = new StringBuilder();
		int pkCnt = 0;
		for (int i = 0; i < colSchemas.columns.size(); ++i) {
			Column col = colSchemas.columns.get(i);
			if (i > 0) {
				sql.append(", ");
			}
			sql.append(col.column).append(" ").append(col.type.dbNativeDataType);
			if (col.isNotNull != 0) {
				sql.append(" NOT NULL");
			}
			if (col.defaultValue != null && !col.defaultValue.isBlank()) {
				sql.append(" DEFAULT ").append(col.defaultValue);
			}
			if (col.pkIndex > 0) {
				if (pkCnt > 0) {
					pkCols.append(", ");
				}
				pkCols.append(col.column);
				pkCnt++;
			}
		}
		if (pkCnt > 0) {
			sql.append(", PRIMARY KEY (").append(pkCols).append(")");
		}
		sql.append(")");
		return sql.toString();
	}

	private boolean isReplicationToSQLite() {
		if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.REPLICATION) {
			if (ConfLoader.getInstance().getDstType(dstIndex) == DstType.SQLITE) {			
				return true;
			}
		}
		return false;
	}
}


