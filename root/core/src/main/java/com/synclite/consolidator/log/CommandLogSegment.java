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

package com.synclite.consolidator.log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.nativedb.DB;
import com.synclite.consolidator.nativedb.PreparedStatement;

public class CommandLogSegment extends LogSegment {

	public class CommandLogSegmentReader implements AutoCloseable {
		public DB dbHandle;
		public PreparedStatement logReaderStmt;
		public Map<Long, PreparedStatement> argReaderPrepStmts = new HashMap<Long, PreparedStatement>();
		private int hasNextLog;
		private static final String ARG_TABLE_NAME_PREFIX = "arg";
		private CommandLogRecord prevMainLogRecord;
		private CommandLogRecord prevLogRecord;
		private String commandLogSegmentReaderSql;
		private String txnLogSegmentReaderSql;

		public DB txnDBHandle;
		public PreparedStatement txnLogReaderStmt;

		public CommandLogSegmentReader(long afterCommitID) throws SQLException {
			dbHandle = new DB(CommandLogSegment.this.path);
			commandLogSegmentReaderSql = commandLogSegmentReaderSqlTemplate.replace("$1", SyncLiteLoggerInfo.prepareArgList(logTableArgCnt));
			txnLogSegmentReaderSql = txnLogSegmentReaderSqlTemplate.replace("$1", SyncLiteLoggerInfo.prepareArgList(logTableArgCnt));
			logReaderStmt = dbHandle.prepare(commandLogSegmentReaderSql);
			argReaderPrepStmts.clear();
			for (String argTblName : CommandLogSegment.this.argTableList){          
				prepareArgTablePstmt(argTblName);
			}
			logReaderStmt.bindLong(1, afterCommitID);
			this.hasNextLog = 0;
			this.prevMainLogRecord = null;
			this.prevLogRecord = null;
			this.txnDBHandle = null;
		}

		private void prepareArgTablePstmt(String argTblName) throws SQLException {
			Long argTableArgCnt = Long.valueOf(argTblName.substring(ARG_TABLE_NAME_PREFIX.length()));
			String argTableReaderSql = argTableReaderSqlTemplate.replace("$1", SyncLiteLoggerInfo.prepareArgList(argTableArgCnt));
			argTableReaderSql = argTableReaderSql.replace("$2", argTblName);
			PreparedStatement argReaderPstmt = dbHandle.prepare(argTableReaderSql);
			argReaderPrepStmts.put(argTableArgCnt, argReaderPstmt);
		}

		public CommandLogRecord readNextRecord() throws SyncLiteException {
			try {            	
				if (txnDBHandle != null) {
					//If txnDBHandle is open then return from it.
					this.hasNextLog = txnLogReaderStmt.stepQuery();                    
					if (this.hasNextLog != 0) {                    
						long changeNumber = prevMainLogRecord.changeNumber;
						long commitId = prevMainLogRecord.commitId;
						long txnChangeNumber = txnLogReaderStmt.getLong(0);
						String sql = txnLogReaderStmt.getString(2);
						long argCnt = txnLogReaderStmt.getLong(3);
						List<Object> args = new ArrayList<Object>();

						for (int i = 1; i <= argCnt; i++) {
							//Bind all arguments
							args.add(txnLogReaderStmt.getNativeValue(i+3));
						}

						CommandLogRecord commandLogRecord;
						if (sql == null) {
							commandLogRecord = new CommandLogRecord(changeNumber, txnChangeNumber, commitId, prevLogRecord, argCnt, args);
						} else {
							commandLogRecord = new CommandLogRecord(changeNumber, txnChangeNumber, commitId, sql, argCnt, args);
						}
						this.prevLogRecord = commandLogRecord;
						return commandLogRecord;
					} else {
						this.txnLogReaderStmt.close();
						this.txnDBHandle.close();
						this.txnLogReaderStmt = null;
						this.txnDBHandle = null;
					}
				}
				this.hasNextLog = logReaderStmt.stepQuery();
				if (this.hasNextLog == 0) {
					return null;
				} else {
					long changeNumber = logReaderStmt.getLong(0);
					long commitId = logReaderStmt.getLong(1);
					String sql = logReaderStmt.getString(2);
					long argCnt = logReaderStmt.getLong(3);
					List<Object> args = new ArrayList<Object>();

					if ((sql != null) && sql.equals("REPLAY_TXN")) {
						//Open the txn DB file and return all logs from the file one by one.
						//Return logs

						CommandLogRecord commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, sql, argCnt, args);
						this.prevMainLogRecord = commandLogRecord;
						this.prevLogRecord = commandLogRecord;

						//If txnDBHandle is open then return from it.
						Path txnFilePath = SyncLiteLoggerInfo.getCmdLogTxnFilePath(CommandLogSegment.this.path, commitId);
						if (! Files.exists(txnFilePath)) {
							if (ConfLoader.getInstance().getSkipBadTxnFiles()) {
								device.tracer.error("Failed to locate txn file, skipping it : " + txnFilePath);
								//Return an empty log record since nothing found here in this txn file.
								commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, "", 0, args);
								prevLogRecord = commandLogRecord;
								return commandLogRecord;                        	
							} else {
								device.tracer.error("Failed to locate txn file : " + txnFilePath);
								throw new SyncLiteException("Failed to locate txn file : " + txnFilePath + " for referred by command log segment : " + CommandLogSegment.this.path);
							}
						}

						try {
							txnDBHandle = new DB(txnFilePath);
							txnLogReaderStmt = txnDBHandle.prepare(txnLogSegmentReaderSql);
							this.hasNextLog = txnLogReaderStmt.stepQuery();                    
							if (this.hasNextLog != 0) {
								changeNumber = prevMainLogRecord.changeNumber;
								commitId = prevMainLogRecord.commitId;
								long txnChangeNumber = txnLogReaderStmt.getLong(0);
								sql = txnLogReaderStmt.getString(2);
								argCnt = txnLogReaderStmt.getLong(3);
								args.clear();
								for (int i = 1; i <= argCnt; i++) {
									//Bind all arguments
									args.add(txnLogReaderStmt.getNativeValue(i+3));
								}
								if (sql == null) {
									commandLogRecord = new CommandLogRecord(changeNumber, txnChangeNumber, commitId, prevLogRecord, argCnt, args);
								} else {
									commandLogRecord = new CommandLogRecord(changeNumber, txnChangeNumber, commitId, sql, argCnt, args);
								}
								prevLogRecord = commandLogRecord;
								return commandLogRecord;
							} else {
								this.txnLogReaderStmt.close();
								this.txnDBHandle.close();
								this.txnLogReaderStmt = null;
								this.txnDBHandle = null;

								//Return an empty log record since nothing found here in this txn file.
								commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, "", 0, args);
								prevLogRecord = commandLogRecord;
								return commandLogRecord;                        	
							}
						} catch (Exception e) {
							if (ConfLoader.getInstance().getSkipBadTxnFiles()) {
								device.tracer.error("Failed to read the txn log file, skipping it : " + txnFilePath + " : " + e.getMessage(), e);
								try {
									this.txnLogReaderStmt.close();
									this.txnDBHandle.close();
								} catch (Exception e1) {
									this.txnLogReaderStmt = null;
									this.txnDBHandle = null;
								}
								//Return an empty log record since nothing found here in this txn file.
								commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, "", 0, args);
								prevLogRecord = commandLogRecord;
								return commandLogRecord;                        	
							} else {
								device.tracer.error("Failed to read the txn log file : " + txnFilePath + " : " + e.getMessage(), e);
								throw new SyncLiteException("Failed to read txn log file : " + txnFilePath + " : " + e.getMessage(), e);
							}
						}
					}

					if (argCnt <= CommandLogSegment.this.logTableArgCnt) {
						for (int i = 1; i <= argCnt; i++) {
							//Bind all arguments
							args.add(logReaderStmt.getNativeValue(i+3));
						}
					} else {
						//
						/////DEPRECATED///
						//
						long tableArgCnt = SyncLiteLoggerInfo.nextPowerOf2(argCnt);
						PreparedStatement argReaderPStmt = argReaderPrepStmts.get(tableArgCnt);
						argReaderPStmt.bindLong(1, changeNumber);
						argReaderPStmt.stepQuery();
						for (int i = 0; i < argCnt; i++) {
							//Bind all arguments
							args.add(argReaderPStmt.getNativeValue(i));
						}
						//TODO
						argReaderPStmt.reset();
					}
					CommandLogRecord commandLogRecord;
					if (sql == null) {
						commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, prevLogRecord, argCnt, args);
					} else {
						commandLogRecord = new CommandLogRecord(changeNumber, -1, commitId, sql, argCnt, args);
					}
					this.prevMainLogRecord = commandLogRecord;
					this.prevLogRecord = commandLogRecord;
					return commandLogRecord;
				}
			} catch (SQLException e ) {
				throw new SyncLiteException("Failed to read records from command log segment : " + CommandLogSegment.this.path + " with exception : ", e);
			}
		}

		@Override
		public void close() throws SyncLiteException {
			try {
				for (Map.Entry<Long, PreparedStatement> entry : argReaderPrepStmts.entrySet()) {
					entry.getValue().finalizePrepared();
				}
				if (logReaderStmt != null) {
					logReaderStmt.finalizePrepared();
				}
				if (dbHandle != null) {
					dbHandle.close();
				}
				dbHandle = null;
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to close command log segment : " + CommandLogSegment.this.path + " with exception : ", e);
			}
		}
	}

	//private static final String commandLogSegmentReaderSqlTemplate = "SELECT change_number, commit_id, sql, arg_cnt, $1 FROM commandlog WHERE change_number > ? AND commit_id > ? AND commit_id IN (SELECT commit_id FROM commandlog WHERE sql = 'COMMIT') ORDER BY change_number";
	private static final String commandLogSegmentReaderSqlTemplate = "SELECT change_number, commit_id, sql, arg_cnt, $1 FROM commandlog WHERE commit_id > ? AND commit_id IN (SELECT commit_id FROM commandlog WHERE sql = 'COMMIT') ORDER BY change_number";
	private static final String txnLogSegmentReaderSqlTemplate = "SELECT change_number, commit_id, sql, arg_cnt, $1 FROM commandlog ORDER BY change_number";

	private static final String argTableInfoReaderSql = "SELECT tbl_name FROM sqlite_master WHERE type = 'table' AND tbl_name LIKE 'arg%'";
	private static final String argTableReaderSqlTemplate = "SELECT $1 FROM $2 WHERE change_number = ?";
	private long logSegmentLogCount;
	private long logTableArgCnt;
	private long publishTime;
	private List<String> argTableList = new ArrayList<String>();

	public CommandLogSegment(Device device, long sequenceNumber, Path path, long publishTime) {
		super(device, sequenceNumber, path);
		this.publishTime = publishTime;
	}

	public long getLogTableArgCnt() {
		return this.logTableArgCnt;
	}

	public long getPublishTime() {
		return this.publishTime;
	}

	public CommandLogSegmentReader open(long commitID) throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		for (long i = 0; i < ConfLoader.getInstance().getSyncLiteOperRetryCount(); ++i) {
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM commandlog;")) {
						if (rs.next()) {
							this.logSegmentLogCount = rs.getLong(1);
						} else {
							throw new SyncLiteException("Failed to read command log segment :" + this.path);
						}
					}
					long countCols = 0;
					try (ResultSet rs = stmt.executeQuery("pragma table_info(\"commandlog\")")) {
						while (rs.next()) {
							++countCols;
						}
					}
					this.logTableArgCnt = countCols - 4;
					argTableList.clear();
					try(ResultSet rs = stmt.executeQuery(argTableInfoReaderSql)) {
						while(rs.next()) {
							argTableList.add(rs.getString(1));
						}
					}
				}

				CommandLogSegmentReader reader = new CommandLogSegmentReader(commitID);
				return reader;
			} catch (Exception e) {
				if (i == (ConfLoader.getInstance().getSyncLiteOperRetryCount()-1)) {                    
					throw new SyncLiteException("Open of command log segment: "+ this.path + " failed after all retry attempts", e);
				} else {
					String errorMsg = "Open of command log segment: "+ this.path + " failed, will be retrid" + e;
					device.tracer.error(errorMsg);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getSyncLiteOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return null;
	}
}
