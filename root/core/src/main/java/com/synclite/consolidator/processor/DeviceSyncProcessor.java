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

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.MetadataRetry;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.schema.TableMapper;

/**
 * Abstract base for processors that apply logs to a destination and maintain
 * checkpoint / schema metadata — either locally (LOCAL) or on the destination
 * database itself (DESTINATION).  Both DeviceEventStreamer and
 * DeviceConsolidator extend this class and share all infrastructure below.
 */
public abstract class DeviceSyncProcessor extends DeviceProcessor {

	// ── Shared SQL constants ───────────────────────────────────────────────────
	public static final String createTxnTableSql =
		"CREATE TABLE IF NOT EXISTS synclite_checkpoint(commit_id LONG NOT NULL PRIMARY KEY, " +
		"cdc_change_number LONG NOT NULL, " +
		"cdc_log_segment_sequence_number LONG NOT NULL, " +
		"initialization_status LONG NOT NULL DEFAULT 0, " +
		"txn_count LONG NOT NULL)";
	public static final String createDstTxnTableSql =
		"CREATE TABLE IF NOT EXISTS synclite_checkpoint(" +
		"synclite_device_id TEXT NOT NULL, " +
		"synclite_device_name TEXT NOT NULL, " +
		"synclite_update_timestamp TEXT, " +
		"commit_id LONG NOT NULL, " +
		"cdc_change_number LONG NOT NULL, " +
		"cdc_log_segment_sequence_number LONG NOT NULL, " +
		"initialization_status LONG NOT NULL DEFAULT 0, " +
		"txn_count LONG NOT NULL, " +
		"PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id))";
	public static final String selectTxnTableSql =
		"SELECT commit_id, cdc_change_number, cdc_log_segment_sequence_number, txn_count " +
		"FROM synclite_checkpoint";
	public static final String insertTxnTableSql =
		"INSERT INTO synclite_checkpoint(" +
		"commit_id, cdc_change_number, cdc_log_segment_sequence_number, txn_count" +
		") VALUES($1, -1, 0, 0);";
	protected static final String updateTxnTableSql =
		"UPDATE synclite_checkpoint SET commit_id = ?, cdc_change_number = ?, " +
		"cdc_log_segment_sequence_number = ?, txn_count = ?";

	// ── Where metadata is stored ───────────────────────────────────────────────
	protected enum MetadataStore { DESTINATION, LOCAL }
	protected MetadataStore metadataStore;

	// ── Shared fields ──────────────────────────────────────────────────────────
	protected Connection inMemoryReplicaConn;
	protected ConsolidatorSrcTable checkpointTable;
	protected boolean dstSchemaTableInitialized = false;
	protected boolean dstInitStatusColumnInitialized = false;
	protected boolean hasProcessedAllSegments = false;

	protected TableMapper userTableMapper;
	protected TableMapper systemTableMapper;
	protected final ConsolidatorMetadataManager consolidatorMetadataMgr;
	protected DeviceStatsCollector statsCollector;
	protected DeviceDstInitializer dstInitializer;

	protected long consolidatedTxnCount = 0;
	protected long lastConsolidatedCommitId = 0;
	protected long lastConsolidatedChangeNumber = -1;
	protected boolean applyInsertIdempotently;

	protected HashSet<TableID> currentTxnDstTables = new HashSet<TableID>();
	protected HashMap<TableID, HashMap<OperType, Long>> tableStats = new HashMap<TableID, HashMap<OperType, Long>>();

	// ── Constructor ────────────────────────────────────────────────────────────
	protected DeviceSyncProcessor(Device device, int dstIndex) throws SyncLiteException {
		super(device, dstIndex);
		this.userTableMapper = TableMapper.getUserTableMapperInstance(dstIndex);
		this.systemTableMapper = TableMapper.getSystemTableMapperInstance(dstIndex);
		this.consolidatorMetadataMgr = device.getConsolidatorMetadataMgr(dstIndex);
		this.statsCollector = device.getDeviceStatsCollector(dstIndex);
		this.dstInitializer = new DeviceDstInitializer(device, userTableMapper, systemTableMapper, statsCollector, dstIndex);
		this.dstInitializer.setSyncProcessor(this);
		this.applyInsertIdempotently = ConfLoader.getInstance().getDstIdempotentDataIngestion(dstIndex);
		String modeStr = ConfLoader.getInstance().getMetadataStore(dstIndex);
		this.metadataStore = "LOCAL".equalsIgnoreCase(modeStr) ? MetadataStore.LOCAL : MetadataStore.DESTINATION;
		ensureWorkDirExists();
		initInMemoryReplica();
	}

	// ── Abstract hook: return sequence number of current log segment ───────────
	protected abstract long getCurrentLogSegmentSequenceNumber();

	// ── hasMoreWork default: both subclasses just check hasProcessedAllSegments ─
	@Override
	public boolean hasMoreWork() throws SyncLiteException {
		return !hasProcessedAllSegments;
	}

	// ── In-memory replica ──────────────────────────────────────────────────────
	protected final void initInMemoryReplica() throws SyncLiteException {
		try {
			this.inMemoryReplicaConn = DriverManager.getConnection("jdbc:sqlite::memory:");
			this.inMemoryReplicaConn.setAutoCommit(true);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to initialize in-memory replica connection", e);
		}
	}

	protected void ensureWorkDirExists() throws SyncLiteException {
		try {
			java.nio.file.Path root = device.getDeviceDataRoot();
			if (!Files.exists(root)) {
				Files.createDirectories(root);
				device.tracer.info("Created fresh workDir at : " + root);
			}
		} catch (IOException e) {
			throw new SyncLiteException("Failed to create workDir : " + device.getDeviceDataRoot(), e);
		}
	}

	protected final void seedInMemoryReplicaFromSchemas() throws SyncLiteException {
		try (Statement stmt = inMemoryReplicaConn.createStatement()) {
			for (ConsolidatorSrcTable srcTable : consolidatorMetadataMgr.getConsolidatorSrcTables()) {
				if (srcTable.sql != null && !srcTable.sql.isEmpty()) {
					try {
						stmt.execute(srcTable.sql);
					} catch (SQLException e) {
						if (!e.getMessage().contains("already exists")) throw e;
					}
				} else {
					StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
					sb.append(srcTable.id.table).append(" (");
					boolean first = true;
					List<Column> pkCols = new java.util.ArrayList<>();
					for (Column col : srcTable.columns) {
						if (!first) sb.append(", ");
						sb.append(col.column).append(" ").append(col.type.dbNativeDataType);
						if (col.isNotNull != 0) sb.append(" NOT NULL");
						if (col.pkIndex != 0) pkCols.add(col);
						first = false;
					}
					if (pkCols.size() == 1) {
						sb.append(", PRIMARY KEY(").append(pkCols.get(0).column).append(")");
					} else if (pkCols.size() > 1) {
						pkCols.sort((a, b) -> Integer.compare(a.pkIndex, b.pkIndex));
						sb.append(", PRIMARY KEY(");
						for (int i = 0; i < pkCols.size(); ++i) {
							if (i > 0) sb.append(", ");
							sb.append(pkCols.get(i).column);
						}
						sb.append(")");
					}
					sb.append(")");
					try {
						stmt.execute(sb.toString());
					} catch (SQLException e) {
						if (!e.getMessage().contains("already exists")) throw e;
					}
				}
			}
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to seed in-memory replica from stored schemas", e);
		}
	}

	/** Populates checkpointTable.columns from the in-memory replica DDL when empty. */
	protected final void reloadCheckpointTableColumns() throws SyncLiteException {
		if (this.checkpointTable.columns.isEmpty()) {
			try (Statement _stmt = inMemoryReplicaConn.createStatement()) {
				_stmt.execute(createTxnTableSql);
			} catch (SQLException _e) {
				// Already exists — fine
			}
			try {
				List<Column> _cols = device.schemaReader.fetchColumns(inMemoryReplicaConn, this.checkpointTable.id);
				this.checkpointTable.clearColumns();
				for (Column _c : _cols) {
					this.checkpointTable.addColumn(_c);
				}
			} catch (SyncLiteException _e) {
				throw new SyncLiteException("Failed to populate checkpoint table columns from in-memory replica", _e);
			}
		}
	}

	// ── Initialization status on destination metadata ───────────────────────────

	protected void ensureDstMetadataInitStatusColumn(SQLExecutor dstExecutor) throws DstExecutionException {
		if (dstInitStatusColumnInitialized) return;
		dstExecutor.execute(new NativeOper(null, createDstTxnTableSql));
		boolean recreate = false;
		try {
			dstExecutor.execute(new NativeOper(null,
					"SELECT synclite_device_id, synclite_device_name, initialization_status FROM synclite_checkpoint WHERE 1 = 0"));
		} catch (DstExecutionException e) {
			String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
			if (msg.contains("no such column") || msg.contains("unknown column") || msg.contains("invalid identifier")) {
				recreate = true;
			} else {
				throw e;
			}
		}
		if (!recreate) {
			try {
				dstExecutor.execute(new NativeOper(null,
						"SELECT command_log_change_number FROM synclite_checkpoint WHERE 1 = 0"));
				recreate = true;
			} catch (DstExecutionException e) {
				String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
				if (!(msg.contains("no such column") || msg.contains("unknown column") || msg.contains("invalid identifier"))) {
					throw e;
				}
			}
		}
		if (recreate) {
			// Recreate incompatible or legacy checkpoint table schema in one shot (no ALTER path).
			dstExecutor.execute(new NativeOper(null, "DROP TABLE IF EXISTS synclite_checkpoint"));
			dstExecutor.execute(new NativeOper(null, createDstTxnTableSql));
		}
		dstExecutor.commitTran();
		dstExecutor.beginTran();
		dstInitStatusColumnInitialized = true;
	}

	protected void persistInitializationStatusToDst(SQLExecutor dstExecutor, long status) throws DstExecutionException {
		ensureDstMetadataInitStatusColumn(dstExecutor);
		String uuid  = device.getDeviceUUID().replace("'", "''");
		String dname = device.getDeviceName().replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"UPDATE synclite_checkpoint SET initialization_status = " + status
				+ " WHERE synclite_device_id = '" + uuid
				+ "' AND synclite_device_name = '" + dname + "'"));
	}

	/**
	 * Reads initialization status from the destination and, if it is 1, syncs it
	 * back into the local metadata manager so processing can proceed to doSync().
	 * Called when local status is 0 and metadataStore == DESTINATION (fresh-host recovery).
	 */
	protected void loadInitializationStatusFromDestination(SQLExecutor dstExecutor) throws SyncLiteException, DstExecutionException {
		ensureDstMetadataInitStatusColumn(dstExecutor);
		long dstStatus = dstExecutor.readInitializationStatus(
				device.getDeviceUUID(), device.getDeviceName(), this.dstIndex);
		if (dstStatus == 1) {
			try {
				MetadataRetry.retry(dstIndex, device.tracer, "updateInitializedSnapshotName",
						() -> consolidatorMetadataMgr.updateInitializedSnapshotName("restored"));
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to restore initialization status from destination : ", e);
			}
		}
	}

	// ── Schema-on-destination helpers ─────────────────────────────────────────

	/**
	 * Ensures the unified per-table metadata table exists on the destination.
	 * Per-table source DDL is stored as {@code prop_key='create_sql'} rows in
	 * {@code synclite_consolidator_table_metadata}.
	 */
	protected void ensureDstSchemaTableExists(SQLExecutor dstExecutor) throws DstExecutionException {
		if (dstSchemaTableInitialized) return;
		dstExecutor.execute(new NativeOper(null, SyncLiteConsolidatorInfo.getCreateConsolidatorTableMetadataTableSql()));
		dstExecutor.commitTran();
		dstExecutor.beginTran();
		dstSchemaTableInitialized = true;
	}

	protected void loadSchemasFromDestination(SQLExecutor dstExecutor) throws SyncLiteException, DstExecutionException {
		ensureDstSchemaTableExists(dstExecutor);
		List<String[]> rows = dstExecutor.readTableSchemas(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex);
		for (String[] row : rows) {
			// row = [database_name, table_name, create_sql]
			String dbName    = (row.length > 2) ? row[0] : "main";
			String tableName = (row.length > 2) ? row[1] : row[0];
			String createSql = (row.length > 2) ? row[2] : row[1];
			if (createSql == null || createSql.isBlank()) continue;
			TableID id = TableID.from(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex,
					(dbName == null || dbName.isEmpty()) ? "main" : dbName, null, tableName);
			ConsolidatorSrcTable srcTable = ConsolidatorSrcTable.from(id);
			srcTable.sql = createSql;
			// Execute CREATE SQL on in-memory replica so PRAGMA table_info can resolve columns
			boolean replicaTableReady = true;
			try (Statement stmt = inMemoryReplicaConn.createStatement()) {
				stmt.execute(createSql);
			} catch (SQLException e) {
				String msg = e.getMessage().toLowerCase();
				// Ignore benign conditions: table already exists, or composite PK syntax
				// that the in-memory SQLite rejects (e.g. "more than one primary key").
				// These do not prevent column resolution via PRAGMA table_info.
				if (!msg.contains("already exists") && !msg.contains("more than one primary key")) {
					throw new SyncLiteException("Failed to seed in-memory replica for table : " + tableName, e);
				}
				// If the CREATE failed (e.g. composite PK rejection), the table was not created
				// in the replica so columns cannot be resolved — skip this table.
				if (msg.contains("more than one primary key")) {
					replicaTableReady = false;
				}
			}
			if (!replicaTableReady) continue;
			// Populate srcTable.columns via PRAGMA table_info on in-memory replica
			List<Column> cols = device.schemaReader.fetchColumns(inMemoryReplicaConn, id);
			srcTable.clearColumns();
			for (Column c : cols) srcTable.addColumn(c);
			try {
				consolidatorMetadataMgr.upsertSchema(srcTable);
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to cache schema from destination for table : " + tableName, e);
			}
		}
	}

	protected void persistSchemaToDst(SQLExecutor dstExecutor, ConsolidatorSrcTable srcTable) throws DstExecutionException {
		ensureDstSchemaTableExists(dstExecutor);
		String uuid  = device.getDeviceUUID().replace("'", "''");
		String dname = device.getDeviceName().replace("'", "''");
		String dbname = (srcTable.id.database == null || srcTable.id.database.isEmpty() ? "main" : srcTable.id.database).replace("'", "''");
		String tname = srcTable.id.table.replace("'", "''");
		// Always derive the DDL from the current column model — srcTable.sql is set once
		// from the original CREATE on restart and never refreshed by apply*Column, so it
		// would drift after any ADDCOLUMN / DROPCOLUMN / ALTERCOLUMN / RENAMECOLUMN.
		String csql  = buildCreateSqlFromColumns(srcTable).replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"DELETE FROM synclite_consolidator_table_metadata WHERE device_uuid = '" + uuid
				+ "' AND device_name = '" + dname
				+ "' AND database_name = '" + dbname
				+ "' AND table_name = '" + tname
				+ "' AND prop_key = 'create_sql'"));
		dstExecutor.execute(new NativeOper(null,
				"INSERT INTO synclite_consolidator_table_metadata(device_uuid, device_name, database_name, table_name, prop_key, prop_value) VALUES('"
				+ uuid + "', '" + dname + "', '" + dbname + "', '" + tname + "', 'create_sql', '" + csql + "')"));
	}

	protected void deleteSchemaFromDst(SQLExecutor dstExecutor, ConsolidatorSrcTable srcTable) throws DstExecutionException {
		if (!dstSchemaTableInitialized) return;
		String uuid  = device.getDeviceUUID().replace("'", "''");
		String dname = device.getDeviceName().replace("'", "''");
		String dbname = (srcTable.id.database == null || srcTable.id.database.isEmpty() ? "main" : srcTable.id.database).replace("'", "''");
		String tname = srcTable.id.table.replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"DELETE FROM synclite_consolidator_table_metadata WHERE device_uuid = '" + uuid
				+ "' AND device_name = '" + dname
				+ "' AND database_name = '" + dbname
				+ "' AND table_name = '" + tname
				+ "' AND prop_key = 'create_sql'"));
	}

	protected String buildCreateSqlFromColumns(ConsolidatorSrcTable srcTable) {
		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(srcTable.id.table).append(" (");
		List<Column> pkCols = new ArrayList<>();
		boolean first = true;
		for (Column col : srcTable.columns) {
			if (!first) sb.append(", ");
			sb.append(col.column).append(" ").append(col.type.dbNativeDataType);
			if (col.isNotNull != 0) sb.append(" NOT NULL");
			if (col.pkIndex != 0) {
				pkCols.add(col);
			}
			first = false;
		}

		if (pkCols.size() == 1) {
			sb.append(", PRIMARY KEY(").append(pkCols.get(0).column).append(")");
		} else if (pkCols.size() > 1) {
			pkCols.sort((a, b) -> Integer.compare(a.pkIndex, b.pkIndex));
			sb.append(", PRIMARY KEY(");
			for (int i = 0; i < pkCols.size(); ++i) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(pkCols.get(i).column);
			}
			sb.append(")");
		}

		sb.append(")");
		return sb.toString();
	}

	/**
	 * Persists updated schema to destination after a DDL.
	 * No-op in LOCAL mode — local metadata manager is already updated by caller.
	 */
	protected void persistSchemaAfterDDL(SQLExecutor dstExecutor, ConsolidatorSrcTable srcTable) throws SyncLiteException {
		if (metadataStore == MetadataStore.DESTINATION) {
			try {
				persistSchemaToDst(dstExecutor, srcTable);
			} catch (DstExecutionException e) {
				throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " on destination : ", e);
			}
		}
	}

	/** Deletes schema from destination after DROPTABLE (no-op in LOCAL mode). */
	protected void deleteSchemaAfterDrop(SQLExecutor dstExecutor, ConsolidatorSrcTable srcTable) throws SyncLiteException {
		if (metadataStore == MetadataStore.DESTINATION) {
			try {
				deleteSchemaFromDst(dstExecutor, srcTable);
			} catch (DstExecutionException e) {
				throw new SyncLiteException("Failed to delete schema for table : " + srcTable.id + " from destination : ", e);
			}
		}
	}

	// ── Checkpoint helpers ─────────────────────────────────────────────────────

	protected final List<Oper> prepareCheckpointUpdate(long commitIDToCheckpoint,
			long changeNumberToCheckpoint,
			List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException {
		List<Object> checkpointBeforeValues = new ArrayList<Object>(1);
		checkpointBeforeValues.add(this.lastConsolidatedCommitId);

		List<Object> checkpointAfterValues = new ArrayList<Object>(4);
		checkpointAfterValues.add(commitIDToCheckpoint);
		checkpointAfterValues.add(changeNumberToCheckpoint);
		checkpointAfterValues.add(getCurrentLogSegmentSequenceNumber());
		checkpointAfterValues.add(this.consolidatedTxnCount + 1);

		Update checkpointUpdate = new Update(checkpointTable, checkpointBeforeValues, checkpointAfterValues);
		checkpointUpdate.whereColumns = new ArrayList<Column>();
		checkpointUpdate.whereColumns.add(getCheckpointColumn("commit_id"));
		checkpointUpdate.setColumns = new ArrayList<Column>();
		checkpointUpdate.setColumns.add(getCheckpointColumn("commit_id"));
		checkpointUpdate.setColumns.add(getCheckpointColumn("cdc_change_number"));
		checkpointUpdate.setColumns.add(getCheckpointColumn("cdc_log_segment_sequence_number"));
		checkpointUpdate.setColumns.add(getCheckpointColumn("txn_count"));
		return systemTableMapper.mapOper(checkpointUpdate);
	}

	private Column getCheckpointColumn(String columnName) throws SyncLiteException {
		Column col = checkpointTable.colMap.get(columnName);
		if (col == null) {
			throw new SyncLiteException("Missing checkpoint column in synclite_checkpoint: " + columnName);
		}
		return col;
	}

	protected final List<Oper> prepareCheckpointInsert() throws SyncLiteException {
		List<Object> afterValues = new ArrayList<Object>();
		afterValues.add(this.lastConsolidatedCommitId);
		afterValues.add(this.lastConsolidatedChangeNumber);
		afterValues.add(getCurrentLogSegmentSequenceNumber());
		afterValues.add(consolidatorMetadataMgr.getInitializationStatus());
		afterValues.add(this.consolidatedTxnCount);
		return systemTableMapper.mapOper(new Insert(checkpointTable, afterValues, false));
	}

	protected void updateDstCheckpointIfNeeded(SQLExecutor dstExecutor, long commitId,
			long changeNumber,
			List<Object> beforeValues, List<Object> afterValues) throws DstExecutionException, SyncLiteException {
		if (!ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			dstExecutor.execute(prepareCheckpointUpdate(commitId, changeNumber, beforeValues, afterValues));
		}
	}

	protected void updateLocalCheckpointIfNeeded(long commitId, long changeNumber)
			throws DstExecutionException, SyncLiteException {
		if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			ArrayList<Object> args = new ArrayList<Object>();
			args.add(commitId);
			args.add(changeNumber);
			args.add(getCurrentLogSegmentSequenceNumber());
			args.add(this.consolidatedTxnCount + 1);
			consolidatorMetadataMgr.executeCheckpointTablePreparedStmt(updateTxnTableSql, args);
		}
	}

	// ── Commit / reset helpers ─────────────────────────────────────────────────

	protected void commitDstTran(SQLExecutor dstExecutor) throws DstExecutionException {
		if (ConfLoader.getInstance().triggersEnabled(dstIndex)) {
			for (TableID dstTableID : currentTxnDstTables) {
				if (ConfLoader.getInstance().tableHasTriggers(dstIndex, dstTableID.table)) {
					List<String> triggers = ConfLoader.getInstance().getTriggers(dstIndex, dstTableID.table);
					if (triggers != null) {
						for (String triggerStmt : triggers) {
							dstExecutor.execute(new NativeOper(null, triggerStmt));
						}
					}
				}
			}
		}
		dstExecutor.commitTran();
		currentTxnDstTables.clear();
		resetApplyInsertIdempotently();
	}

	protected void resetApplyInsertIdempotently() {
		if (!ConfLoader.getInstance().getDstIdempotentDataIngestion(dstIndex)) {
			this.applyInsertIdempotently = false;
		}
	}
}
