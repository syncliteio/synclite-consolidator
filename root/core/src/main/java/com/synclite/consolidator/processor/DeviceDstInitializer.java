package com.synclite.consolidator.processor;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.DstObjectInitMode;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;
import com.synclite.consolidator.schema.ValueMapper;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceDstInitializer {

	private Device device;
	private final TableMapper userTableMapper;
	private final TableMapper systemTableMapper;
	
	private final ConsolidatorMetadataManager consolidatorMetadataMgr;
	private final DeviceStatsCollector statsCollector;
	private int dstIndex;
	public DeviceDstInitializer(Device device, TableMapper userTableMapper, TableMapper systemTableMapper, DeviceStatsCollector statsCollector, int dstIndex) {
		this.dstIndex = dstIndex;
		this.device = device;
		this.consolidatorMetadataMgr = device.getConsolidatorMetadataMgr(this.dstIndex);
		this.userTableMapper = userTableMapper;
		this.systemTableMapper = systemTableMapper;
		this.statsCollector = statsCollector;
	}

	protected final void trySnapshotConsolidation(Path snapshot) throws SyncLiteException {
		if (snapshot != null) {
			device.tracer.info("Starting to consolidate device snapshot : " + snapshot + " on dst : " + dstIndex);
			consolidate(snapshot);
			consolidatorMetadataMgr.updateInitializedSnapshotName(snapshot.getFileName().toString());
			//Update restartCDCLogPosition
			//reloadCheckpointInfo();
			Monitor.getInstance().incrInitializedDeviceCnt(1L);
			Monitor.getInstance().incrInitializationCnt(1L);
			statsCollector.updateInitialTableAndDeviceStatistics();
			device.tracer.info("Finished consolidating device snapshot : " + snapshot + " on dst : " + dstIndex);
		}
	}
	
	private final void consolidate(Path snapshot) throws SyncLiteException {
		List<ConsolidatorSrcTable> srcTables = device.schemaReader.fetchConsolidatorSrcTables(snapshot, this.dstIndex);
		Table checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		checkpointTable.setIsSystemTable();
		initializeSchema(srcTables);
		initializeTables(snapshot, srcTables);
	}

	protected void initializeDataLakeObject() throws SyncLiteException {
		//Snapshot is not for telemetry file initialization as no one else is operating on replica file for Telemetry device.
		//We can directly initialize from replica file.
		List<ConsolidatorSrcTable> srcTables = device.schemaReader.fetchConsolidatorSrcTables(device.getReplica(this.dstIndex), this.dstIndex);
		Table checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		checkpointTable.setIsSystemTable();
		initializeDataLakeSchema(srcTables);
		initializeDataLakeTables(device.getReplica(this.dstIndex), srcTables);
	}

	private final void initializeSchema(List<ConsolidatorSrcTable> srcTables) throws DstExecutionException, SyncLiteException {    	 
		//For REPLICATION sync mode, we create one schema per device.
		//Also need to initialize schema if destination is DATALAKE to create appropriate schema on the data lake object
		if ((ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.REPLICATION)) {
			for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
					try {
						try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
							//dstExecutor.initSetup();
							dstExecutor.beginTran();
							dstExecutor.execute(userTableMapper.mapOper(new CreateSchema(srcTables.get(0))));
							dstExecutor.commitTran();
						}
						break;
					} catch (DstExecutionException e) {
						if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
							throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
						}
						try {
							Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
						} catch (InterruptedException e1) {
							Thread.interrupted();
						}
						device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
					}
				}
		}
	}

	private final void initializeDataLakeSchema(List<ConsolidatorSrcTable> srcTables) throws DstExecutionException, SyncLiteException {    	 
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
			try {
				try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
					dstExecutor.beginTran();
					dstExecutor.execute(userTableMapper.mapOper(new CreateSchema(srcTables.get(0))));
					dstExecutor.commitTran();
				}
				break;
			} catch (DstExecutionException e) {
				if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
					throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
			}
		}
	}

	private final void markTableInitializing(ConsolidatorSrcTable srcTable) throws SyncLiteException {
		try {
			consolidatorMetadataMgr.upsertTableMetadataEntry(srcTable, "initialization_status", "INITIALIZING");
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to persist initialization status for table : " + srcTable.id + " in consolidator metadata file : " , e);
		}
	}

	private final void markTableInitialized(ConsolidatorSrcTable srcTable, long rowCount) throws SyncLiteException {
		try {
			consolidatorMetadataMgr.upsertTableMetadataEntry(srcTable, "initialization_status", "INITIALIZED");
			consolidatorMetadataMgr.upsertTableMetadataEntry(srcTable, "initial_rows", rowCount);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to persist initialization status for table : " + srcTable.id + " in consolidator metadata file : " , e);
		}
	}


	private final void initializeTables(Path snapshot, List<ConsolidatorSrcTable> srcTables) throws SyncLiteException {
		ConsolidatorSrcTable loggerCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLoggerCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		ConsolidatorSrcTable dbReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getDBReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		ConsolidatorSrcTable logReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLogReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));

		ConsolidatorSrcTable consolidatorCheckpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		for(ConsolidatorSrcTable srcTable : srcTables) {			
			//
			//Skip the logger checkpoint table synclite_txn , dbreader checkpoint table synclite_dbreader_checkpoint, synclite_logreader_checkpoint 
			//as these are not needed on destination
			//
			if (srcTable == loggerCheckpointTable) {
				continue;
			}
			if (srcTable == dbReaderCheckpointTable) {
				continue;
			}
			if (srcTable == logReaderCheckpointTable) {
				continue;
			}
			
			if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
				if (srcTable == consolidatorCheckpointTable) {
					continue;
				}
			}

			if (! ConfLoader.getInstance().isAllowedTable(dstIndex, srcTable.id.table)) {
				continue;
			}
			TableMapper tableMapper = srcTable.getIsSystemTable() ? systemTableMapper : userTableMapper;
			String status = getTableInitializationStatus(srcTable);
			if ((status == null) || (! status.equalsIgnoreCase("INITIALIZED"))) {
				device.tracer.info("Initializing table : " + srcTable.id);
				//No status found
				markTableInitializing(srcTable);
				device.tracer.info("Initializing Table schema : " + srcTable.id);
				initTableSchema(srcTable, tableMapper);
				device.tracer.info("Initialized Table schema : " + srcTable.id);
				device.tracer.info("Copying initial data : " + srcTable.id);
				long rowCount = copyTable(snapshot, srcTable, tableMapper);
				device.tracer.info("Copied initial "  + rowCount + " records : " + srcTable.id);
				markTableInitialized(srcTable, rowCount);
				device.tracer.info("Initialized table : " + srcTable.id);
			}            
		}
	}

	public final void cleanupTables() throws SyncLiteException {
		List<ConsolidatorSrcTable> srcTables = device.schemaReader.fetchConsolidatorSrcTables(device.getReplica(this.dstIndex), this.dstIndex);
		ConsolidatorSrcTable loggerCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLoggerCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		ConsolidatorSrcTable dbReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getDBReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		ConsolidatorSrcTable logReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLogReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		ConsolidatorSrcTable consolidatorCheckpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));

		for (ConsolidatorSrcTable srcTable : srcTables) {
			//
			//Skip the logger checkpoint table synclite_txn , dbreader checkpoint table synclite_dbreader_checkpoint 
			//as these are not needed on destination
			//
			if (srcTable == loggerCheckpointTable) {
				continue;
			}
			if (srcTable == dbReaderCheckpointTable) {
				continue;
			}
			if (srcTable == logReaderCheckpointTable) {
				continue;
			}

			if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
				if (srcTable == consolidatorCheckpointTable) {
					continue;
				}
			}

			if (! ConfLoader.getInstance().isAllowedTable(dstIndex, srcTable.id.table)) {
				continue;
			}
			TableMapper tableMapper = srcTable.getIsSystemTable() ? systemTableMapper : userTableMapper;
			undoTableData(srcTable, tableMapper);
		}		
	}

	private final void initializeDataLakeTables(Path snapshot, List<ConsolidatorSrcTable> srcTables) throws SyncLiteException {
		try {
			ConsolidatorSrcTable loggerCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLoggerCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
			ConsolidatorSrcTable dbReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getDBReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
			ConsolidatorSrcTable logReaderCheckpointTable = ConsolidatorSrcTable.from(SyncLiteLoggerInfo.getLogReaderCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));

			ConsolidatorSrcTable consolidatorCheckpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));

			for(ConsolidatorSrcTable srcTable : srcTables) {			
				//
				//Skip the logger checkpoint table synclite_txn , dbreader checkpoint table synclite_dbreader_checkpoint 
				//as these are not needed on destination
				//
				if (srcTable == loggerCheckpointTable) {
					continue;
				}
				if (srcTable == dbReaderCheckpointTable) {
					continue;
				}
				if (srcTable == logReaderCheckpointTable) {
					continue;
				}

				if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
					if (srcTable == consolidatorCheckpointTable) {
						continue;
					}
				}

				if (! ConfLoader.getInstance().isAllowedTable(dstIndex, srcTable.id.table)) {
					continue;
				}
				TableMapper tableMapper = srcTable.getIsSystemTable() ? systemTableMapper : userTableMapper;
				initTableSchema(srcTable, tableMapper);
			}
		} catch (SyncLiteException e) {
			throw new SyncLiteException("Failed to initialize new telemetry file", e);
		}
	}

	private final void initTableSchema(ConsolidatorSrcTable srcTable, TableMapper tableMapper) throws SyncLiteException {
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
			try {
				try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
					//
					DstObjectInitMode objectInitMode = ConfLoader.getInstance().getDstObjectInitMode(dstIndex);
					Table checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));					
					if (srcTable == checkpointTable) {
						objectInitMode = DstObjectInitMode.TRY_CREATE_DELETE_DATA;
					}
					dstExecutor.beginTran();
					dstExecutor.execute(tableMapper.mapOper(new CreateTable(srcTable)));
					dstExecutor.commitTran();
				}
				try {
					consolidatorMetadataMgr.upsertSchema(srcTable);
				} catch (SQLException e) {
					throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
				}
				break;
			} catch (DstExecutionException e) {
				if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
					throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
			}
		}
	}

	private final long copyTable(Path snapshot, ConsolidatorSrcTable srcTable, TableMapper tableMapper) throws SyncLiteException {
		long rowCount = 0;
		ConsolidatorDstTable dstTable = tableMapper.mapTable(srcTable);
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
			rowCount = 0;
			try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
				rowCount = copyTableData(snapshot, srcTable, dstTable, dstExecutor, tableMapper);
				break;
			} catch (DstExecutionException e) {
				if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
					throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
			}
		}
		return rowCount;
	}


	private final long copyTableData(Path snapshot, ConsolidatorSrcTable srcTable, ConsolidatorDstTable dstTable, SQLExecutor dstExecutor, TableMapper tableMapper) throws SyncLiteException {
		long rowCount = 0;
		boolean applyInsertIdempotently = ConfLoader.getInstance().getDstIdempotentDataIngestion(dstIndex);
		dstExecutor.beginTran();
		String url = "jdbc:sqlite:" + snapshot;
		ValueMapper valueMapper = tableMapper.getValueMapper();
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery(device.schemaReader.getSelectSql(snapshot, srcTable))) {
					List<Object> values = new ArrayList<Object>(srcTable.columns.size());
					//Get a mapped insert operation
					//In the while loop below, only bind the values and avoid creating and mapping Insert oper again and again.
					//
					Insert mappedInsert = (Insert) (tableMapper.mapOper(new Insert(srcTable, values, applyInsertIdempotently)).get(0));
					dstExecutor.beginTran();
					while (rs.next()) {
						for (int i = 1; i <= srcTable.columns.size(); ++i) {
							Column col = srcTable.columns.get(i-1);
							if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, srcTable.id.table, col.column)) {
								continue;
							}
							Object colVal = rs.getObject(i);
							colVal = valueMapper.mapValue(srcTable.id, col, colVal);
							values.add(colVal);
						}
						
						tableMapper.bindMappedInsert(mappedInsert, values);
						dstExecutor.execute(mappedInsert);
						values.clear();
						++rowCount;
					}
					//Check if there are triggers set for this table then exeucte them.
					
					//
					//Check if trigger statements are set on tables involved in this transaction
					//If yes then execute
					//		
					if (ConfLoader.getInstance().triggersEnabled(dstIndex)) {
						if (ConfLoader.getInstance().tableHasTriggers(dstIndex, dstTable.id.table)) {
							List<String> triggers = ConfLoader.getInstance().getTriggers(dstIndex, dstTable.id.table);
							if (triggers != null) {
								for (String triggerStmt : triggers) {
									dstExecutor.execute(new NativeOper(null, triggerStmt));
								}
							}					
						}
					}					
					dstExecutor.commitTran();					
				}
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed copying data for table " + srcTable.id.table + " from snapshot : " + snapshot, e);
		}
		return rowCount;
	}

	private final void undoTableData(ConsolidatorSrcTable srcTable, TableMapper tableMapper) throws SyncLiteException {
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
			try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
				dstExecutor.beginTran();
				
				if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.REPLICATION) {
					switch (ConfLoader.getInstance().getDstObjectInitMode(dstIndex)) {
					case OVERWRITE_OBJECT:
						device.tracer.info("Dropping table : " + srcTable.id);
						dstExecutor.execute(tableMapper.mapOper(new DropTable(srcTable)));
						break;
					case TRY_CREATE_DELETE_DATA:
					case DELETE_DATA:
						device.tracer.info("Truncating table : " + srcTable.id);
						dstExecutor.execute(tableMapper.mapOper(new TruncateTable(srcTable)));
						break;
					case TRY_CREATE_APPEND_DATA:
					case APPEND_DATA:
						//Do nothing
						device.tracer.info("Skipping Undo as Dst Object Initialization Mode is : " + DstObjectInitMode.APPEND_DATA);
						break;
					}
				} else if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.CONSOLIDATION) {
					switch (ConfLoader.getInstance().getDstObjectInitMode(dstIndex)) {
					case OVERWRITE_OBJECT:
						device.tracer.info("Deleting data for device : " + device + " from table : " + srcTable.id);
						dstExecutor.execute(tableMapper.mapOper(new TruncateTable(srcTable)));
						break;
					case TRY_CREATE_DELETE_DATA:
					case DELETE_DATA:
						device.tracer.info("Deleting data for device : " + device + " from table : " + srcTable.id);
						dstExecutor.execute(tableMapper.mapOper(new TruncateTable(srcTable)));
						break;
					case TRY_CREATE_APPEND_DATA:
					case APPEND_DATA:
						//Do nothing
						break;
					}
				}
				dstExecutor.commitTran();
				break;
			} catch (DstExecutionException e) {
				if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
					throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
			}
		}
	}


	private final String getTableInitializationStatus(ConsolidatorSrcTable srcTable) throws SyncLiteException {
		String status = null;
		try {
			Object val = consolidatorMetadataMgr.getTableMetadataEntry(srcTable, "initialization_status");
			if (val != null) {
				return val.toString();
			}
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to read initialization status for table : " + srcTable.id + " from consolidator metadata file : ", e);
		}
		return status;
	}

	protected final void removeTables() throws SyncLiteException {
		for(ConsolidatorSrcTable srcTable : consolidatorMetadataMgr.getConsolidatorSrcTables()) {
			TableMapper tableMapper = srcTable.getIsSystemTable() ? systemTableMapper : userTableMapper;
			removeTable(srcTable, tableMapper);
		}
	}


	private final void removeTable(ConsolidatorSrcTable srcTable, TableMapper tableMapper) throws SyncLiteException {
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
			try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)){
				dstExecutor.beginTran();
				dstExecutor.execute(tableMapper.mapOper(new DropTable(srcTable)));
				dstExecutor.commitTran();
				break;
			} catch (DstExecutionException e) {
				if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
					throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
			}
		}
	}



}
