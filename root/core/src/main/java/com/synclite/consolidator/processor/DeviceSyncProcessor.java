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
		"CREATE TABLE IF NOT EXISTS synclite_metadata(commit_id LONG NOT NULL PRIMARY KEY, " +
		"command_log_change_number LONG NOT NULL, command_log_txn_change_number LONG NOT NULL, " +
		"command_log_segment_sequence_number LONG NOT NULL, cdc_change_number LONG NOT NULL, " +
		"cdc_txn_change_number LONG NOT NULL, cdc_log_segment_sequence_number LONG NOT NULL, " +
		"txn_count LONG NOT NULL)";
	public static final String selectTxnTableSql =
		"SELECT commit_id, command_log_change_number, command_log_txn_change_number, command_log_segment_sequence_number, " +
		"cdc_change_number, cdc_txn_change_number, cdc_log_segment_sequence_number, txn_count " +
		"FROM synclite_metadata";
	public static final String insertTxnTableSql =
		"INSERT INTO synclite_metadata VALUES($1, -1, -1, 0, -1, -1, 0, 0);";
	protected static final String updateTxnTableSql =
		"UPDATE synclite_metadata SET commit_id = ?, cdc_change_number = ?, " +
		"cdc_txn_change_number = ?, cdc_log_segment_Sequence_number = ?, txn_count = ?";

	// ── Where metadata is stored ───────────────────────────────────────────────
	protected enum MetadataStore { DESTINATION, LOCAL }
	protected MetadataStore metadataStore;

	// ── Shared fields ──────────────────────────────────────────────────────────
	protected Connection inMemoryReplicaConn;
	protected ConsolidatorSrcTable checkpointTable;
	protected boolean dstSchemaTableInitialized = false;
	protected boolean dstDeviceStatusTableInitialized = false;
	protected boolean hasProcessedAllSegments = false;

	protected TableMapper userTableMapper;
	protected TableMapper systemTableMapper;
	protected final ConsolidatorMetadataManager consolidatorMetadataMgr;
	protected DeviceStatsCollector statsCollector;
	protected DeviceDstInitializer dstInitializer;

	protected long consolidatedTxnCount = 0;
	protected long lastConsolidatedCommitId = 0;
	protected long lastConsolidatedChangeNumber = -1;
	protected long lastConsolidatedTxnChangeNumber = -1;
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

	// ── Device-status-on-destination helpers ───────────────────────────────────

	protected void ensureDstDeviceStatusTableExists(SQLExecutor dstExecutor) throws DstExecutionException {
		if (dstDeviceStatusTableInitialized) return;
		dstExecutor.execute(new NativeOper(null, SyncLiteConsolidatorInfo.getCreateDeviceStatusTableSql()));
		dstExecutor.commitTran();
		dstExecutor.beginTran();
		dstDeviceStatusTableInitialized = true;
	}

	protected void persistInitializationStatusToDst(SQLExecutor dstExecutor, long status) throws DstExecutionException {
		ensureDstDeviceStatusTableExists(dstExecutor);
		String uuid  = device.getDeviceUUID().replace("'", "''");
		String dname = device.getDeviceName().replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"DELETE FROM synclite_device_status WHERE device_uuid = '" + uuid
				+ "' AND device_name = '" + dname
				+ "' AND dst_index = " + dstIndex));
		dstExecutor.execute(new NativeOper(null,
				"INSERT INTO synclite_device_status(device_uuid, device_name, dst_index, initialization_status) VALUES('"
				+ uuid + "', '" + dname + "', " + dstIndex + ", " + status + ")"));
	}

	/**
	 * Reads initialization status from the destination and, if it is 1, syncs it
	 * back into the local metadata manager so processing can proceed to doSync().
	 * Called when local status is 0 and metadataStore == DESTINATION (fresh-host recovery).
	 */
	protected void loadInitializationStatusFromDestination(SQLExecutor dstExecutor) throws SyncLiteException, DstExecutionException {
		ensureDstDeviceStatusTableExists(dstExecutor);
		long dstStatus = dstExecutor.readInitializationStatus(
				device.getDeviceUUID(), device.getDeviceName(), this.dstIndex);
		if (dstStatus == 1) {
			try {
				consolidatorMetadataMgr.updateInitializedSnapshotName("restored");
			} catch (SyncLiteException e) {
				throw new SyncLiteException("Failed to restore initialization status from destination : ", e);
			}
		}
	}

	// ── Schema-on-destination helpers ─────────────────────────────────────────

	protected void ensureDstSchemaTableExists(SQLExecutor dstExecutor) throws DstExecutionException {
		if (dstSchemaTableInitialized) return;
		dstExecutor.execute(new NativeOper(null, SyncLiteConsolidatorInfo.getCreateTableSchemaTableSql()));
		dstExecutor.commitTran();
		dstExecutor.beginTran();
		dstSchemaTableInitialized = true;
	}

	protected void loadSchemasFromDestination(SQLExecutor dstExecutor) throws SyncLiteException, DstExecutionException {
		ensureDstSchemaTableExists(dstExecutor);
		List<String[]> rows = dstExecutor.readTableSchemas(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex);
		for (String[] row : rows) {
			String tableName = row[0];
			String createSql = row[1];
			if (createSql == null || createSql.isBlank()) continue;
			TableID id = TableID.from(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex, "main", null, tableName);
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
		String tname = srcTable.id.table.replace("'", "''");
		String sql   = srcTable.sql != null ? srcTable.sql : buildCreateSqlFromColumns(srcTable);
		String csql  = (sql != null ? sql : "").replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"DELETE FROM synclite_table_schema WHERE device_uuid = '" + uuid
				+ "' AND device_name = '" + dname
				+ "' AND dst_index = " + dstIndex
				+ " AND table_name = '" + tname + "'"));
		dstExecutor.execute(new NativeOper(null,
				"INSERT INTO synclite_table_schema(device_uuid, device_name, dst_index, table_name, create_sql) VALUES('"
				+ uuid + "', '" + dname + "', " + dstIndex + ", '" + tname + "', '" + csql + "')"));
	}

	protected void deleteSchemaFromDst(SQLExecutor dstExecutor, ConsolidatorSrcTable srcTable) throws DstExecutionException {
		if (!dstSchemaTableInitialized) return;
		String uuid  = device.getDeviceUUID().replace("'", "''");
		String dname = device.getDeviceName().replace("'", "''");
		String tname = srcTable.id.table.replace("'", "''");
		dstExecutor.execute(new NativeOper(null,
				"DELETE FROM synclite_table_schema WHERE device_uuid = '" + uuid
				+ "' AND device_name = '" + dname
				+ "' AND dst_index = " + dstIndex
				+ " AND table_name = '" + tname + "'"));
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
			long changeNumberToCheckpoint, long txnChangeNumberToCheckpoint,
			List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException {
		beforeValues.add(this.lastConsolidatedCommitId);
		beforeValues.add(0);
		beforeValues.add(0);
		beforeValues.add(0);
		beforeValues.add(this.lastConsolidatedChangeNumber);
		beforeValues.add(this.lastConsolidatedTxnChangeNumber);
		beforeValues.add(getCurrentLogSegmentSequenceNumber());
		beforeValues.add(this.consolidatedTxnCount);

		afterValues.add(commitIDToCheckpoint);
		afterValues.add(0);
		afterValues.add(0);
		afterValues.add(0);
		afterValues.add(changeNumberToCheckpoint);
		afterValues.add(txnChangeNumberToCheckpoint);
		afterValues.add(getCurrentLogSegmentSequenceNumber());
		afterValues.add(this.consolidatedTxnCount + 1);

		return systemTableMapper.mapOper(new Update(checkpointTable, beforeValues, afterValues));
	}

	protected final List<Oper> prepareCheckpointInsert() throws SyncLiteException {
		List<Object> afterValues = new ArrayList<Object>();
		afterValues.add(this.lastConsolidatedCommitId);
		afterValues.add(0);
		afterValues.add(0);
		afterValues.add(0);
		afterValues.add(this.lastConsolidatedChangeNumber);
		afterValues.add(this.consolidatedTxnCount);
		return systemTableMapper.mapOper(new Insert(checkpointTable, afterValues, false));
	}

	protected void updateDstCheckpointIfNeeded(SQLExecutor dstExecutor, long commitId,
			long changeNumber, long txnChangeNumber,
			List<Object> beforeValues, List<Object> afterValues) throws DstExecutionException, SyncLiteException {
		if (!ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			dstExecutor.execute(prepareCheckpointUpdate(commitId, changeNumber, txnChangeNumber, beforeValues, afterValues));
		}
	}

	protected void updateLocalCheckpointIfNeeded(long commitId, long changeNumber, long txnChangeNumber)
			throws DstExecutionException, SyncLiteException {
		if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			ArrayList<Object> args = new ArrayList<Object>();
			args.add(commitId);
			args.add(changeNumber);
			args.add(txnChangeNumber);
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
