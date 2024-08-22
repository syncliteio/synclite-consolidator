package com.synclite.consolidator.log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

public class EventLogSegment extends LogSegment {

	public class EventLogSegmentReader implements AutoCloseable {
		public Connection dbHandle;
		public PreparedStatement logReaderStmt;
		public ResultSet logReaderStmtRS;

		public Connection txnDBHandle;
		public PreparedStatement txnLogReaderStmt;
		public ResultSet txnLogReaderStmtRS;

		public Map<Long, PreparedStatement> argReaderPrepStmts = new HashMap<Long, PreparedStatement>();
		private boolean hasNextLog;
		private EventLogRecord prevMainLogRecord;
		private EventLogRecord prevLogRecord;
		private static final String ARG_TABLE_NAME_PREFIX = "arg";
		private String commandLogSegmentReaderSql;

		public EventLogSegmentReader() throws SQLException {
			dbHandle = DriverManager.getConnection("jdbc:sqlite:" + EventLogSegment.this.path);
			commandLogSegmentReaderSql = commandLogSegmentReaderSqlTemplate.replace("$1", SyncLiteLoggerInfo.prepareArgList(logTableArgCnt));
			logReaderStmt = dbHandle.prepareStatement(commandLogSegmentReaderSql);
			argReaderPrepStmts.clear();
			for (String argTblName : EventLogSegment.this.argTableList){
				Long argTableArgCnt = Long.valueOf(argTblName.substring(ARG_TABLE_NAME_PREFIX.length()));
				String argTableReaderSql = argTableReaderSqlTemplate.replace("$1", SyncLiteLoggerInfo.prepareArgList(argTableArgCnt));
				argTableReaderSql = argTableReaderSql.replace("$2", argTblName);
				PreparedStatement argReaderPstmt = dbHandle.prepareStatement(argTableReaderSql);
				argReaderPrepStmts.put(argTableArgCnt, argReaderPstmt);
			}
			this.hasNextLog = false;
			this.logReaderStmtRS = logReaderStmt.executeQuery();
			this.prevMainLogRecord = null;
			this.prevLogRecord = null;
		}

		public EventLogRecord readNextRecord() throws SyncLiteException {
			try {            	
				if (txnDBHandle != null) {
					//If txnDBHandle is open then return from it.
					this.hasNextLog = txnLogReaderStmtRS.next();                
					if (this.hasNextLog == true) {
						long changeNumber = prevMainLogRecord.changeNumber;
						long commitId = prevMainLogRecord.commitId;
						long txnChangeNumber = txnLogReaderStmtRS.getLong("change_number");
						String sql = txnLogReaderStmtRS.getString("sql");
						long argCnt = txnLogReaderStmtRS.getLong("arg_cnt");
						List<Object> args = new ArrayList<Object>();

						for (int i = 1; i <= argCnt; i++) {
							//Bind all arguments
							args.add(txnLogReaderStmtRS.getObject(i+4));
						}
						EventLogRecord eventLogRecord;
						if (sql == null) {
							eventLogRecord = new EventLogRecord(changeNumber, txnChangeNumber, commitId, prevLogRecord, argCnt, args);
						} else {
							eventLogRecord = new EventLogRecord(changeNumber, txnChangeNumber, commitId, sql, argCnt, args);
						}
						this.prevLogRecord = eventLogRecord;
						return eventLogRecord;
					} else {
						this.txnLogReaderStmtRS.close();                    	
						this.txnLogReaderStmtRS = null;
						this.txnLogReaderStmt.close();
						this.txnLogReaderStmt = null;
						this.txnDBHandle.close();
						this.txnDBHandle = null;
					}
				}

				this.hasNextLog = logReaderStmtRS.next();
				if (this.hasNextLog == false) {
					return null;
				} else {
					long changeNumber = logReaderStmtRS.getLong(1);
					long commitId = logReaderStmtRS.getLong(2);
					String sql = logReaderStmtRS.getString(3);
					long argCnt = logReaderStmtRS.getLong(4);
					List<Object> args = new ArrayList<Object>();

					if ((sql != null) && sql.equals("REPLAY_TXN")) {
						//Open the txn DB file and return all logs from the file one by one.
						//Return logs

						EventLogRecord eventLogRecord = new EventLogRecord(changeNumber, -1, commitId, sql, argCnt, args);
						this.prevMainLogRecord = eventLogRecord;
						this.prevLogRecord = eventLogRecord;

						//If txnDBHandle is open then return from it.
						Path txnFilePath = SyncLiteLoggerInfo.getEventLogTxnFilePath(EventLogSegment.this.path, commitId);

						if (! Files.exists(txnFilePath)) {
							if (ConfLoader.getInstance().getSkipBadTxnFiles()) {
								device.tracer.error("Failed to locate txn file, skipping it : " + txnFilePath);
								return eventLogRecord;
							} else {
								device.tracer.error("Failed to locate txn file : " + txnFilePath);
								throw new SyncLiteException("Failed to locate txn file : " + txnFilePath + " for referred by event log segment : " + EventLogSegment.this.path);
							}
						}

						try {
							txnDBHandle = DriverManager.getConnection("jdbc:sqlite:" + txnFilePath);
							txnLogReaderStmt = txnDBHandle.prepareStatement(commandLogSegmentReaderSql);
							txnLogReaderStmtRS = txnLogReaderStmt.executeQuery();                        

							this.hasNextLog = txnLogReaderStmtRS.next();
							if (this.hasNextLog == true) {
								changeNumber = prevMainLogRecord.changeNumber;
								commitId = prevMainLogRecord.commitId;
								long txnChangeNumber = txnLogReaderStmtRS.getLong("change_number");
								sql = txnLogReaderStmtRS.getString("sql");
								argCnt = txnLogReaderStmtRS.getLong("arg_cnt");
								args.clear();
								for (int i = 1; i <= argCnt; i++) {
									//Bind all arguments
									args.add(txnLogReaderStmtRS.getObject(i+4));
								}
								if (sql == null) {
									eventLogRecord = new EventLogRecord(changeNumber, txnChangeNumber, commitId, prevLogRecord, argCnt, args);
								} else {
									eventLogRecord = new EventLogRecord(changeNumber, txnChangeNumber, commitId, sql, argCnt, args);
								}
								this.prevLogRecord = eventLogRecord;
								return eventLogRecord;
							} else {
								this.txnLogReaderStmt.close();
								this.txnDBHandle.close();
								this.txnLogReaderStmt = null;
								this.txnDBHandle = null;
							}
						} catch (Exception e) {
							if (ConfLoader.getInstance().getSkipBadTxnFiles()) {
								try {
									this.txnLogReaderStmt.close();
									this.txnDBHandle.close();
								} catch (Exception e1) {
									this.txnLogReaderStmt = null;
									this.txnDBHandle = null;
								}
								device.tracer.error("Failed to read the txn log file, skipping it : " + txnFilePath + " : " + e.getMessage(), e);
								return eventLogRecord;
							} else {
								device.tracer.error("Failed to read the txn log file : " + txnFilePath + " : " + e.getMessage(), e);
								throw new SyncLiteException("Failed to read txn log file : " + txnFilePath + " : " + e.getMessage(), e);
							}
						}
					}

					if (argCnt <= EventLogSegment.this.logTableArgCnt) {
						for (int i = 1; i <= argCnt; i++) {
							//Bind all arguments
							args.add(logReaderStmtRS.getObject(i+4));
						}
					} else {
						long tableArgCnt = SyncLiteLoggerInfo.nextPowerOf2(argCnt);
						PreparedStatement argReaderPStmt = argReaderPrepStmts.get(tableArgCnt);
						argReaderPStmt.setLong(1, changeNumber);
						ResultSet argReaderPstmtRS = argReaderPStmt.executeQuery();
						for (int i = 1; i <= argCnt; i++) {
							args.add(argReaderPstmtRS.getObject(i));
						}
					}
					EventLogRecord eventLogRecord;
					if (sql == null) {
						//
						//Optimization to reuse all parsed info 
						//If sql is null then this log record is for arguments of a previous prepared statement
						//We don't have to parse it all over again, just carry forward the parsed info from previous log record
						//                    	 
						eventLogRecord = new EventLogRecord(changeNumber, -1, commitId, prevLogRecord, argCnt, args);
					} else {
						eventLogRecord = new EventLogRecord(changeNumber, -1, commitId, sql, argCnt, args);
					}
					this.prevMainLogRecord = eventLogRecord;
					this.prevLogRecord = eventLogRecord;
					return eventLogRecord;
				}
			} catch (SQLException e ) {
				throw new SyncLiteException("Failed to read records from event log segment : " + EventLogSegment.this.path + " with exception : ", e);
			}
		}


		@Override
		public void close() throws SyncLiteException {
			try {
				for (Map.Entry<Long, PreparedStatement> entry : argReaderPrepStmts.entrySet()) {
					entry.getValue().close();
				}
				argReaderPrepStmts.clear();
				if (logReaderStmtRS != null) {
					logReaderStmtRS.close();
				}
				if (logReaderStmt != null) {
					logReaderStmt.close();
				}
				if (dbHandle != null) {
					dbHandle.close();
				}
				dbHandle = null;
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to close command log segment : " + EventLogSegment.this.path + " with exception : ", e);
			}
		}
	}

	private static final String commandLogSegmentReaderSqlTemplate = "SELECT change_number, commit_id, sql, arg_cnt, $1 FROM commandlog ORDER BY change_number";
	private static final String argTableInfoReaderSql = "SELECT tbl_name FROM sqlite_master WHERE type = 'table' AND tbl_name LIKE 'arg%'";
	private static final String argTableReaderSqlTemplate = "SELECT $1 FROM $2 WHERE change_number = ?";
	private long logSegmentLogCount;
	private long logTableArgCnt;
	private List<String> argTableList = new ArrayList<String>();
	private long publishTime;

	public EventLogSegment(Device device, long sequenceNumber, Path path, long publishTime) {
		super(device, sequenceNumber, path);
		this.publishTime = publishTime;
	}

	public long getLogTableArgCnt() {
		return this.logTableArgCnt;
	}

	public long getPublishTime() {
		return this.publishTime;
	}

	public EventLogSegmentReader open() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		for (long i = 0; i < ConfLoader.getInstance().getSyncLiteOperRetryCount(); ++i) {
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM commandlog;")) {
						if (rs.next()) {
							this.logSegmentLogCount = rs.getLong(1);
						} else {
							throw new SyncLiteException("Failed to read event log segment :" + this.path);
						}
					}
					long countCols = 0;
					try (ResultSet rs = stmt.executeQuery("pragma table_info(\"commandlog\")")) {
						while (rs.next()) {
							++countCols;
						}
					}
					this.logTableArgCnt = countCols - 4;

					this.argTableList.clear();
					try(ResultSet rs = stmt.executeQuery(argTableInfoReaderSql)) {
						while(rs.next()) {
							argTableList.add(rs.getString(1));
						}
					}
				}

				EventLogSegmentReader reader = new EventLogSegmentReader();
				return reader;
			} catch (Exception e) {
				if (i == (ConfLoader.getInstance().getSyncLiteOperRetryCount()-1)) {
					throw new SyncLiteException("Open of event log segment: "+ this.path + " failed after all retry attempts", e);
				} else {
					String error = "Open of event log segment : "+ this.path + " failed, will be retrying again : " + e;
					device.tracer.error(error);
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
