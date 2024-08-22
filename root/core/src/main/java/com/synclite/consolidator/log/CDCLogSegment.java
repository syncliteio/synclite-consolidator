package com.synclite.consolidator.log;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.nativedb.DB;
import com.synclite.consolidator.nativedb.PreparedStatement;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.schema.Column;

public class CDCLogSegment extends LogSegment {

	public class CDCLogSegmentWriter implements AutoCloseable{
		private DB logTableConn;
		private PreparedStatement insertLogTablePstmt;
		private PreparedStatement insertLogValuesTablePstmt;
		private PreparedStatement insertLogSchemasTablePstmt;
		private PreparedStatement insertLogStatsTablePstmt;

		public CDCLogSegmentWriter() throws SyncLiteException {
			try {
				this.logTableConn = new DB(CDCLogSegment.this.path);
				String argList = prepareArgList(logSegmentArgCnt);
				String fillerList = preparePStmtFillerList(logSegmentArgCnt + 8);
				String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
				insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
				this.insertLogTablePstmt = logTableConn.prepare(insertLogTableSql);
				this.insertLogSchemasTablePstmt = logTableConn.prepare(insertLogSchemasTableSql);
			} catch(SQLException e) {
				throw new SyncLiteException("Failed to create a writer for cdc log segment : " + CDCLogSegment.this, e);
			}
		}

		public void beginTran() throws SyncLiteException {
			try {
				logTableConn.beginTran();
				CDCLogSegment.this.currentBatchLogCount = 0;
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to begin a transaction on cdc log segment : " + CDCLogSegment.this);
			}
		}

		public void commitTran() throws SyncLiteException {
			try {
				logTableConn.commitTran();
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to commit a transaction on cdc log segment : " + CDCLogSegment.this);
			}
		}

		public void undoLogsAfterCommitID(long commitID) {

		}

		public void writeCDCLog(CDCLogRecord record) throws SyncLiteException {
			try {

				long argCnt = 0;
				if (record.values != null) {
					argCnt = record.values.size();
					if (record.opType == OperType.UPDATE) {
						//Update stores all before values and after values.
						argCnt = 2 * argCnt;
					}
					//Check if log has enough room else add additional arg columns and reprepare the statement 
					if (argCnt > logSegmentArgCnt) {
						//Flush existing pstmt 
						if (CDCLogSegment.this.currentBatchLogCount > 0) {
							insertLogTablePstmt.finalizePrepared();
						}
						//Adjust cdclog table to accomodate additional arg cols
						addNewInlinedArgCols(logSegmentArgCnt + 1, argCnt);

						//Reprepare the new pstmt
						rePrepareLogTablePstmt();
					}					
				}
				insertLogTablePstmt.bindLong(1, record.commitId);
				insertLogTablePstmt.bindText(2, record.database, '1');
				insertLogTablePstmt.bindText(3, record.schema, '1');
				insertLogTablePstmt.bindText(4, record.table, '1');
				insertLogTablePstmt.bindText(5, record.opType.toString(), '1');
				insertLogTablePstmt.bindText(6, record.sql, '1');
				insertLogTablePstmt.bindLong(7, record.changeNumber);
				insertLogTablePstmt.bindLong(8, argCnt);

				if (record.values != null) {
					int idx = 9;
					switch (record.opType) {
					case INSERT:
						for (CDCColumnValues value:  record.values) {
							insertLogTablePstmt.bindNativeValue(idx, value.afterValue);
							++idx;
						}					
						break;
					case UPDATE:
						for (CDCColumnValues value:  record.values) {
							insertLogTablePstmt.bindNativeValue(idx, value.beforeValue);
							++idx;
						}
						for (CDCColumnValues value:  record.values) {
							insertLogTablePstmt.bindNativeValue(idx, value.afterValue);
							++idx;
						}
						break;
					case DELETE:
						for (CDCColumnValues value:  record.values) {
							insertLogTablePstmt.bindNativeValue(idx, value.beforeValue);
							++idx;
						}
						break;
					default:
						
					}
				}

				insertLogTablePstmt.step();

				if (record.colSchemas != null) {
					for (Column c : record.colSchemas.columns) {
						insertLogSchemasTablePstmt.bindLong(1, record.changeNumber);
						insertLogSchemasTablePstmt.bindText(2, record.database, '1');
						insertLogSchemasTablePstmt.bindText(3, record.schema, '1');
						insertLogSchemasTablePstmt.bindText(4, record.table, '1');
						insertLogSchemasTablePstmt.bindLong(5, c.cid);
						insertLogSchemasTablePstmt.bindText(6, c.column, '1');
						insertLogSchemasTablePstmt.bindText(7, c.type.dbNativeDataType, '1');
						insertLogSchemasTablePstmt.bindInt(8, c.isNotNull);
						insertLogSchemasTablePstmt.bindText(9, c.defaultValue, '1');
						insertLogSchemasTablePstmt.bindInt(10, c.pkIndex);
						insertLogSchemasTablePstmt.bindInt(11, c.isAutoIncrement);
						insertLogSchemasTablePstmt.bindText(12, record.oldTable, '1');
						String oldColName = null;
						if (record.colSchemas.newToOldColumnNames != null) {
							oldColName = record.colSchemas.newToOldColumnNames.get(c.column);
						}
						insertLogSchemasTablePstmt.bindText(13, oldColName, '1');
						insertLogSchemasTablePstmt.step();
					}
				}
				++CDCLogSegment.this.currentBatchLogCount;
				++CDCLogSegment.this.logSegmentLogCount;
			} catch(SQLException e) {
				throw new SyncLiteException("Failed to write a CDC log record in cdc log segment : " + CDCLogSegment.this.path, e);
			}
		}

		private final void addNewInlinedArgCols(long startIndex, long endIndex) throws SQLException {
			for (long i=startIndex; i <=endIndex; ++i) {    			
				String sql = alterLogTableSqlTemplate.replace("$1", getArgColPrefix() +i);
				logTableConn.exec(sql);
			}    		
			logSegmentArgCnt = endIndex;
		}

		private final void rePrepareLogTablePstmt() throws SQLException {
			String argList = prepareArgList(logSegmentArgCnt);
			String fillerList = preparePStmtFillerList(logSegmentArgCnt + 8);
			String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
			insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
			insertLogTablePstmt = logTableConn.prepare(insertLogTableSql);		
		}

		@Override
		public void close() throws SyncLiteException {
			try {
				if (insertLogTablePstmt != null) {
					insertLogTablePstmt.finalizePrepared();
					insertLogTablePstmt = null;
				}

				if (insertLogValuesTablePstmt != null) {
					insertLogValuesTablePstmt.finalizePrepared();
					insertLogValuesTablePstmt = null;
				}

				if (insertLogSchemasTablePstmt != null) {
					insertLogSchemasTablePstmt.finalizePrepared();
					insertLogSchemasTablePstmt = null;
				}

				if (insertLogStatsTablePstmt != null) {
					insertLogStatsTablePstmt.finalizePrepared();
					insertLogStatsTablePstmt = null;
				}

				if (logTableConn != null) {
					logTableConn.close();
					logTableConn = null;
				}
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to close CDC log segment : " + CDCLogSegment.this.path + " with exception : ", e);
			}
		}
	}

	public static final String createLogTableSqlTemplate = "CREATE TABLE IF NOT EXISTS cdclog(commit_id LONG, database_name TEXT, schema_name TEXT, table_name TEXT, op_type TEXT, sql TEXT, change_number INTEGER PRIMARY KEY, arg_cnt INTEGER, $1);";
	public static final String createLogSchemasTableSql  = "CREATE TABLE IF NOT EXISTS cdclog_schemas(change_number INTEGER, database_name TEXT, schema_name TEXT, table_name TEXT, column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER, column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER, old_table_name TEXT, old_column_name TEXT)";
	public static final String createMetadataTableSql = "CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT)";
	protected static final String alterLogTableSqlTemplate = "ALTER TABLE cdclog ADD COLUMN $1";

	public static final String logTableIndexSql = "CREATE INDEX IF NOT EXISTS commit_id_cdclog_index ON cdclog(commit_id)";
	public static final String logSchemasTableIndexSql = "CREATE INDEX IF NOT EXISTS change_number_cdclog_schemas_index ON cdclog_schemas(change_number)";

	public static final String undoCDCLogSql = "DELETE FROM cdclog WHERE commit_id = $1";
	public static final String undoCDCLogSchemasSql = "DELETE FROM cdclog_schemas WHERE change_number IN (SELECT change_number FROM cdclog WHERE commit_id = $1)";

	public static final String insertLogTableSqlTemplate = "INSERT INTO cdclog(commit_id, database_name, schema_name, table_name, op_type, sql, change_number, arg_cnt, $1) VALUES ($2)";
	public static final String insertLogTableCommitSql = "INSERT INTO cdclog(commit_id, database_name, schema_name, table_name, op_type, sql, change_number, arg_cnt) VALUES ($1, null, null, null, 'COMMIT', 'COMMIT', $2, 0)";
	public static final String insertLogSchemasTableSql = "INSERT INTO cdclog_schemas(change_number, database_name, schema_name, table_name, column_index, column_name, column_type, column_not_null, column_default_value, column_primary_key, column_auto_increment, old_table_name, old_column_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	public static final String cdcLogReaderSqlTemplate = "SELECT commit_id, database_name, schema_name, table_name, op_type, change_number, arg_cnt, $1 FROM cdclog";
	public static final String cdcLogSchemaReaderSql = "SELECT column_index, column_name, column_type, column_not_null, column_default_value, column_primary_key, column_auto_increment, old_table_name, old_column_name FROM cdclog_schemas WHERE change_number = ? ORDER BY column_index";
	public static final String cdcLogStatsReaderSql = "SELECT database_name, schema_name, table_name, op_type, count(*) FROM cdclog GROUP BY database_name, schema_name, table_name, op_type";
	private static final int DEFAULT_MAX_INLINED_ARGS = 16;

	public long logSegmentArgCnt;
	public long logSegmentLogCount;
	public long lastLoggedCommitID;
	public String lastLoggedTxnFate;
	public long currentBatchLogCount;

	public CDCLogSegment(Device device, long sequenceNumber, Path path) {
		super(device, sequenceNumber, path);
	}


	public CDCLogSegmentWriter getWriter() throws SyncLiteException {
		return new CDCLogSegmentWriter();
	}

	public void load(long lastCommitId) throws SyncLiteException {
		initialize();
		loadMetadata();
		resolveInDoubtTxn(lastCommitId);
		device.tracer.debug("Loaded CDC log segment : " + this);
	}

	public final void undoUncommittedTran(long commitId) throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		try (Connection conn = DriverManager.getConnection(url)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(CDCLogSegment.undoCDCLogSchemasSql.replace("$1", String.valueOf(commitId)));
				stmt.execute(CDCLogSegment.undoCDCLogSql.replace("$1", String.valueOf(commitId)));
			}
			conn.commit();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to log a rollback and flush in the cdc log segment : " + CDCLogSegment.this.path + " with exception : ", e);
		}
	}

	private final void resolveInDoubtTxn(long lastCommitId) throws SyncLiteException {
		if (lastCommitId > 0) {
			//2PC in doubt resolution
			//Check the end of current log segment
			//- if a commit/rollback is recorded then nothing to do
			//- Else, record a commit if commitid matches else record a rollback
			if (lastLoggedCommitID == 0) {
				//There is nothing to resolve
				return;
			}
			if (lastLoggedCommitID < lastCommitId) {
				throw new SyncLiteException("Restart recovery failed. Replica commit with id : " + lastCommitId + " cannot be larger than last logged commit id : " + lastLoggedCommitID + " in the current log segment : " + CDCLogSegment.this);
			} else if (lastLoggedCommitID > lastCommitId) {
				if (lastLoggedTxnFate.equalsIgnoreCase("COMMIT")) {
					throw new SyncLiteException("Restart recovery failed. The last logged commit id : " + lastLoggedCommitID + " has a COMMIT fate even when it is not committed in the replica. The last committed txn commit id in this replica is :" + lastCommitId);
				} else if (lastLoggedTxnFate.equalsIgnoreCase("UNKNOWN")) {
					//logRollbackAndFlush(this.lastLoggedCommitID);
					undoUncommittedTran(lastLoggedCommitID);
					loadMetadata();
				}
				//Nothing to do if fate is already ROLLBACK
			} else {
				if (lastLoggedTxnFate.equalsIgnoreCase("ROLLBACK")) {
					throw new SyncLiteException("Restart recovery failed. The last logged commit id : " + lastLoggedCommitID + " has a ROLLBACK fate whereas the txn with the same commit id is COMMITTed in the replica.");
				} else if (lastLoggedTxnFate.equalsIgnoreCase("UNKNOWN")) {
					markLastLoggedTranAsCommitted(lastLoggedCommitID);
					loadMetadata();
				}
				//Nothing to do if fate is already COMMIT
			}
		}
	}


	private final void markLastLoggedTranAsCommitted(long lastLoggedCommitId) throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				String sql = insertLogTableCommitSql.replace("$1", String.valueOf(lastLoggedCommitId));
				sql = sql.replace("$2", String.valueOf(this.logSegmentLogCount));
				stmt.execute(sql);
			}
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to log a commit entry in cdc log segment : " + CDCLogSegment.this.path + " with exception : ", e);
		}
	}

	public void closeAndMarkReady() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		try (Connection conn = DriverManager.getConnection(url)) {
			conn.setAutoCommit(false);
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(CDCLogSegment.logTableIndexSql);
				stmt.execute(CDCLogSegment.logSchemasTableIndexSql);
			}
			conn.commit();
			markReadyToApply();
		} catch (SQLException e ) {
			throw new SyncLiteException("Failed to close and mark CDC log segment : " + CDCLogSegment.this.path + " closed : ", e);
		}
	}


	private final void initialize() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.path;
		String argList = prepareArgList(DEFAULT_MAX_INLINED_ARGS);
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("pragma journal_mode = normal;");
				stmt.execute("pragma synchronous = normal;");
				stmt.execute("pragma temp_store = memory;");
				stmt.execute("pragma mmap_size = 30000000000;");
				//stmt.execute("pragma page_size = 32768;");
				stmt.execute("pragma page_size = 512;");
				stmt.execute(createLogTableSqlTemplate.replace("$1", argList));
				stmt.execute(createLogSchemasTableSql);
				stmt.execute(createMetadataTableSql);
			}
		} catch(SQLException e) {
			throw new SyncLiteException("Failed to initialize log segment : " + this + " with exception : ", e);
		}
	}

	public final static String prepareArgList(long argCnt) {
		StringBuilder argListBuilder = new StringBuilder();
		for (long i = 1; i <= argCnt; ++i) {
			if (i > 1) {
				argListBuilder.append(", ");
			}
			argListBuilder.append(getArgColPrefix() + i);
		}
		return argListBuilder.toString();
	}

	final static String preparePStmtFillerList(long argCnt) {
		StringBuilder argListBuilder = new StringBuilder();
		for (long i = 1; i <= argCnt; ++i) {
			if (i > 1) {
				argListBuilder.append(", ");
			}
			argListBuilder.append("?");
		}
		return argListBuilder.toString();
	}

	public long loadMetadata() throws SyncLiteException {
		String url = "jdbc:sqlite:" + path;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT max(change_number) + 1 FROM cdclog;")) {
					if (rs.next()) {
						this.logSegmentLogCount = rs.getLong(1);
						if (this.logSegmentLogCount > 0) {
							try (ResultSet rs1 = stmt.executeQuery("SELECT commit_id FROM cdclog WHERE change_number = (SELECT max(change_number) FROM cdclog);")) {
								if (rs1.next()) {
									this.lastLoggedCommitID = rs1.getLong(1);
									try (ResultSet rs2 = stmt.executeQuery("SELECT op_type FROM cdclog WHERE commit_id = " + this.lastLoggedCommitID + " AND  (sql = 'COMMIT' OR sql = 'ROLLBACK')")) {
										if (rs2.next()) {
											this.lastLoggedTxnFate = rs2.getString(1);
										} else {
											this.lastLoggedTxnFate = "UNKNOWN";
										}
									}
								} else {
									throw new SyncLiteException("Invalid metadata state. Unable to find max commit id from a log segmment having non zero log records : " + this);
								}
							}
						} else {
							this.currentBatchLogCount = 0;
							this.logSegmentLogCount = 0;
							this.lastLoggedCommitID = 0;
							this.lastLoggedTxnFate = "UNKNOWN";
						}
					} else {
						this.logSegmentLogCount = 0;
						this.currentBatchLogCount = 0;
						this.lastLoggedCommitID = 0;
						this.lastLoggedTxnFate = "UNKNOWN";
					}                   

				}
				long colCnt = 0;
				try (ResultSet rs = stmt.executeQuery("pragma table_info(cdclog)")) {
					while (rs.next()) {
						++colCnt;
					}
				}
				logSegmentArgCnt = colCnt - 8;
			}
		}catch (SQLException e) {
			throw new SyncLiteException("Failed to read log metadata from log segment : " + this);
		}
		return 0;
	}

	public String getLogReaderSql(Connection conn) throws SQLException {
		long colCnt = 0;
		try (Statement stmt = conn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery("pragma table_info(cdclog)")) {
				while (rs.next()) {
					++colCnt;
				}
			}
		}
		String cdcLogReaderSql = CDCLogSegment.cdcLogReaderSqlTemplate.replace("$1", CDCLogSegment.prepareArgList(colCnt - 8));
		return cdcLogReaderSql;
	}

	@Override
	public String toString() {
		return "Path : " + path;
	}


	public final static String getArgColPrefix() {
		return "arg";
	}

}
