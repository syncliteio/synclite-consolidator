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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.synclite.consolidator.SyncDriver;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.device.DeviceStatus;
import com.synclite.consolidator.exception.DstDuplicateKeyException;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.log.CDCLogPosition;
import com.synclite.consolidator.log.CommandLogRecord.DDLInfo;
import com.synclite.consolidator.log.EventLogRecord;
import com.synclite.consolidator.log.EventLogSegment;
import com.synclite.consolidator.log.EventLogSegment.EventLogSegmentReader;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.FinishBatch;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.UpdateIfPredicate;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.schema.TableMapper;
import com.synclite.consolidator.schema.ValueMapper;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceEventStreamer extends DeviceSyncProcessor {

	private static final String firstCommitIDSql = "SELECT commit_id FROM synclite_txn";

	private Path replicaPath;
	private EventLogSegment currentEventLogSegment;
	private long lastConsolidatedCommitIdOnReplica;
	private long lastConsolidatedChangeNumberOnReplica;
	private long lastConsolidatedTxnChangeNumberOnReplica;
	private CDCLogPosition restartEventLogPosition;
	private boolean replicaAppenderEnabled;
	private boolean isStoreDevice;

	protected DeviceEventStreamer(Device device, int dstIndex) throws SyncLiteException {
		super(device, dstIndex); // DeviceSyncProcessor sets up shared infrastructure + ensureWorkDirExists + initInMemoryReplica
		this.restartEventLogPosition = null;
		this.replicaPath = device.getReplica(this.dstIndex);
		this.replicaAppenderEnabled = false;
		this.isStoreDevice = SyncLiteLoggerInfo.isStoreDevice(device.getDeviceType());
		if (SyncLiteLoggerInfo.isAppenderOrStoreDevice(device.getDeviceType()) || SyncLiteLoggerInfo.isDBLoggerOrStreamingDevice(device.getDeviceType())) {
			replicaAppenderEnabled = ConfLoader.getInstance().getEnableReplicasForStoreAndStreamingDevices();
		}
		initCheckpointTable();
		device.updateDeviceStatus(DeviceStatus.SYNCING, "");
	}

	@Override
	protected long getCurrentLogSegmentSequenceNumber() {
		return this.currentEventLogSegment != null ? this.currentEventLogSegment.sequenceNumber : 0;
	}

	private final void initCheckpointTable() throws SyncLiteException {
		if (Files.exists(this.replicaPath)) {
			String url = "jdbc:sqlite:" + this.replicaPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTxnTableSql);
					try (ResultSet rs = stmt.executeQuery(selectTxnTableSql)) {
						if (!rs.next()) {
							device.tracer.debug("Initializing checkpoint table");
							try (ResultSet rsFirstCommitID = stmt.executeQuery(firstCommitIDSql)) {
								this.lastConsolidatedCommitId = rsFirstCommitID.getLong("commit_id");
								this.lastConsolidatedCommitIdOnReplica = rsFirstCommitID.getLong("commit_id");
							} catch (SQLException e) {
								throw new SyncLiteException("Failed to read initial commit id from replica : " + replicaPath + " with exception : ", e);
							}
							//
							//Set change numbers to Long.MAX_VALUE as all the changes corresponding to 
							//transaction with firstCommitID commit id are reflected in replica and destination
							//as part of snapshot application.
							//Hence we need to skip all changes recorded in the first log file for firstCommitID 
							//Setting changeNumber to MAX_VALUE will make it skip all the logs for this first txn.
							//
							this.lastConsolidatedChangeNumber = Long.MAX_VALUE;
							this.lastConsolidatedTxnChangeNumber = Long.MAX_VALUE;
							this.lastConsolidatedChangeNumberOnReplica = Long.MAX_VALUE;
							this.lastConsolidatedTxnChangeNumberOnReplica = Long.MAX_VALUE;
							
							this.currentEventLogSegment = device.getEventLogSegment(0);
							this.consolidatedTxnCount = 0;

							String insertSql = insertTxnTableSql.replace("$1", String.valueOf(this.lastConsolidatedCommitId));
							stmt.execute(insertSql);
						} else {
							this.lastConsolidatedCommitIdOnReplica = rs.getLong("commit_id");
							this.lastConsolidatedChangeNumberOnReplica = rs.getLong("cdc_change_number");
							this.lastConsolidatedTxnChangeNumberOnReplica = rs.getLong("cdc_txn_change_number");
						}
					}
				}
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to initialize the synclite dblogger checkpoint table in replica : " + replicaPath, e);
			}
		} else {
			//Replica file does not exist (recovery on a new machine).
			//Checkpoint position will be read from destination in reloadCheckpointInfo().
			//In-memory replica will be seeded from stored schemas in reloadCheckpointInfo().
			device.tracer.info("Replica file not found at : " + replicaPath + ". Will recover checkpoint and schema from destination and metadata store.");
			this.lastConsolidatedCommitIdOnReplica = 0;
			this.lastConsolidatedChangeNumberOnReplica = -1;
			this.lastConsolidatedTxnChangeNumberOnReplica = -1;
		}
		
		if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			//Create checkpoint table in metadata file.
			try {
				consolidatorMetadataMgr.executeCheckpointTableSql(createTxnTableSql);				
				HashMap<String, Object> checkpoint = consolidatorMetadataMgr.readCheckpointRecord(selectTxnTableSql);
				if (checkpoint.isEmpty()) {
					String insertSql = insertTxnTableSql.replace("$1", String.valueOf(this.lastConsolidatedCommitId));
					consolidatorMetadataMgr.executeCheckpointTableSql(insertSql);
				}
			} catch(SyncLiteException e) {
				throw new SyncLiteException("Failed to initialize the checkpoint table in SyncLite consolidator metadata file : ", e);			
			}
		}
		device.updateLastReplicatedCommitID(this.lastConsolidatedCommitId);
	}

	
	@Override
	public boolean hasMoreWork() throws SyncLiteException {
		return !hasProcessedAllSegments;
	}

	@Override
	public long syncDevice() throws SyncLiteException {
		long consolidatedOperCount = 0;
		// On a fresh host (workDir was re-created), local initializationStatus is 0.
		// In DESTINATION mode, recover from what was already persisted on the destination.
		if (consolidatorMetadataMgr.getInitializationStatus() != 1 && metadataStore == MetadataStore.DESTINATION) {
			try (SQLExecutor dstExecutorForStatus = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)) {
				loadInitializationStatusFromDestination(dstExecutorForStatus);
			} catch (Exception e) {
				throw new SyncLiteException("Failed to load initialization status from destination : ", e);
			}
		}
		if (consolidatorMetadataMgr.getInitializationStatus() == 1) {
			//Already initialized
			return doSync();
		} else if (device.getStatus() == DeviceStatus.SYNCING){
			this.dstInitializer.trySnapshotConsolidation(this.replicaPath);
			// If initialization just completed, persist status to destination so it survives host failure
			if (consolidatorMetadataMgr.getInitializationStatus() == 1 && metadataStore == MetadataStore.DESTINATION) {
				try (SQLExecutor dstExecutorForStatus = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)) {
					persistInitializationStatusToDst(dstExecutorForStatus, 1);
					dstExecutorForStatus.commitTran();
				} catch (Exception e) {
					throw new SyncLiteException("Failed to persist initialization status to destination : ", e);
				}
			}
		}
		return consolidatedOperCount;
	}

	private final void reloadCheckpointInfo() throws SyncLiteException {
		if (metadataStore == MetadataStore.DESTINATION) {
			try (SQLExecutor dstExecutorForSchema = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)) {
				ensureDstSchemaTableExists(dstExecutorForSchema);
				loadSchemasFromDestination(dstExecutorForSchema);
			} catch (Exception e) {
				throw new SyncLiteException("Failed to load schemas from destination : ", e);
			}
		} else {
			try {
				consolidatorMetadataMgr.loadSchemas(device);			
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to load consolidation src tables from metadata file : ", e);
			}
		}
		//Seed the in-memory replica with table schemas loaded from metadata so that DDL operations
		//can use PRAGMA table_info against it instead of the on-disk replica file.
		seedInMemoryReplicaFromSchemas();
		this.checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		this.checkpointTable.setIsSystemTable();
		// Populate checkpointTable.columns from in-memory replica so that mapTable()
		// can build dstTable.columns for bindUpdateArgs/bindInsertArgs on fresh-host path.
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
		if (ConfLoader.getInstance().getDstDisableMetadataTable(dstIndex)) {
			//Read from consolidator metadata file.
			HashMap<String, Object> checkpointInfo = consolidatorMetadataMgr.readCheckpointRecord(selectTxnTableSql);
			if (checkpointInfo.isEmpty()) {
				throw new SyncLiteException("Failed to read checkpoint info from consolidator metadata file : ");
			} else {
				this.restartEventLogPosition = new CDCLogPosition(Long.valueOf(checkpointInfo.get("commit_id").toString()), Long.valueOf(checkpointInfo.get("cdc_change_number").toString()), Long.valueOf(checkpointInfo.get("cdc_txn_change_number").toString()), Long.valueOf(checkpointInfo.get("cdc_log_segment_sequence_number").toString()), Long.valueOf(checkpointInfo.get("txn_count").toString()));
			}
		} else {
			for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
				try {			
					try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer) ) {
						this.restartEventLogPosition = dstExecutor.readCDCLogPosition(device.getDeviceUUID(), device.getDeviceName(), systemTableMapper.mapTable(checkpointTable));
						break;
					}
				} catch (DstExecutionException e) {
					if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
						throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex) );
					} catch (InterruptedException e1) {
						Thread.currentThread().interrupt();
					}
					device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
				}
			}
		}

		if (this.restartEventLogPosition != null) {
			//If we had recorded a higher log segment sequence number in our metadata 
			//(possibly due to skipped/all logs filtered out/empty log segments) then 
			//bump up restartCDCLogPosition
			//
			if (consolidatorMetadataMgr.getLastConsolidatedCDCLogSegmentSeqNum() > restartEventLogPosition.logSegmentSequenceNumber) {
				this.restartEventLogPosition.logSegmentSequenceNumber = consolidatorMetadataMgr.getLastConsolidatedCDCLogSegmentSeqNum();
			}
			this.currentEventLogSegment = device.getEventLogSegment(this.restartEventLogPosition.logSegmentSequenceNumber);
			this.lastConsolidatedCommitId = this.restartEventLogPosition.commitId;
			this.lastConsolidatedChangeNumber = this.restartEventLogPosition.changeNumber;
			this.lastConsolidatedTxnChangeNumber = this.restartEventLogPosition.txnChangeNumber;
			this.consolidatedTxnCount = this.restartEventLogPosition.txnCount;
			device.updateLastConsolidatedCommitID(this.lastConsolidatedCommitId);
			//Monitor.getInstance().incrTotalDstTxnCnt(this.consolidatedTxnCount);
			/*
			if (this.currentEventLogSegment != null) {
				if (this.currentEventLogSegment.isApplied()) {
					Monitor.getInstance().incrTotalCDCLogSegmentCnt(this.currentEventLogSegment.sequenceNumber + 1);
					device.updateLastConsolidatedLogSegment(this.currentEventLogSegment.sequenceNumber);				
				} else {
					Monitor.getInstance().incrTotalCDCLogSegmentCnt(this.currentEventLogSegment.sequenceNumber);
					device.updateLastConsolidatedLogSegment(currentEventLogSegment.sequenceNumber - 1);
				}
				Monitor.getInstance().registerChangedDevice(device);
			}
			*/
		}
	}

	private long doSync() throws SyncLiteException {
		long consolidatedOperCount = 0;
		if (this.restartEventLogPosition == null) {
			reloadCheckpointInfo();
			if (this.restartEventLogPosition == null) {
				return 0;
			}
		}
		if (this.currentEventLogSegment == null) {
			this.currentEventLogSegment = device.getEventLogSegment(this.restartEventLogPosition.logSegmentSequenceNumber);
			if (this.currentEventLogSegment == null) {
				return 0;
			}
		}

		boolean lookForNextLogSegment = false;
		if (!currentEventLogSegment.isApplied()) {
			if (currentEventLogSegment.isReadyToApply()) {
				device.tracer.info("Started consolidating event log segment : " + currentEventLogSegment + " on dst : " + this.dstIndex);
				for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
					try {
						consolidatedOperCount = doApply();
						if (metadataStore == MetadataStore.LOCAL) {
							device.uploadConsolidatorMetadata(this.dstIndex);
						}
						device.updateLastConsolidatedCommitID(this.lastConsolidatedCommitId);
						break;					
					} catch (SyncLiteException e) {
						if (e instanceof DstDuplicateKeyException) {
							this.applyInsertIdempotently = true;
							device.tracer.error("Insert failed with duplicate key exception, retrying transaction with insert converted to delete+insert");
						}
						if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
							device.tracer.error("Dst txn failed after all retry attempts : ", e);
							Boolean skipFailedLogSegments = ConfLoader.getInstance().getDstSkipFailedLogFiles(dstIndex);
							if (!skipFailedLogSegments) {
								throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
							} else {
								skipAndMarkApplied(currentEventLogSegment);
								break;	
							}
						}
						try {
							Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex) );
						} catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
						}
						device.tracer.error("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
					}
				}
				device.tracer.info("Finished consolidating event log segment : " + currentEventLogSegment + " on dst : " + this.dstIndex);
				lookForNextLogSegment = true;
			} else {
				//current segment is not ready to apply yet
				//We should not come here at all ! But if we do then try to re-download the current log segment again.
				this.currentEventLogSegment = device.getEventLogSegment(this.currentEventLogSegment.sequenceNumber);				
				lookForNextLogSegment = false;
				hasProcessedAllSegments = false;
				device.updateLogsToProcessSince(currentEventLogSegment.getPublishTime());
			}
		} else {
			lookForNextLogSegment = true;
		}

		if (lookForNextLogSegment) {
			EventLogSegment nextEventLogSegment = device.getNextEventLogSegmentToProcess(currentEventLogSegment);
			if (nextEventLogSegment != null) {
				currentEventLogSegment = nextEventLogSegment;
				device.updateLogsToProcessSince(nextEventLogSegment.getPublishTime());
				device.setLastHeartbeatTS(currentEventLogSegment.getPublishTime());
				hasProcessedAllSegments = false;
			} else {
				//TODO
				//check for metadata file publish time. Device keeps sending metadata file at regular interval as a heartbeat message.
				//device.setLastHeartbeatTS(currentEventLogSegment.getPublishTime());
				hasProcessedAllSegments = true;
				device.updateLogsToProcessSince(Long.MAX_VALUE);        	
			}
		}
		return consolidatedOperCount;
	}

	private final void skipAndMarkApplied(EventLogSegment currentEventLogSegment) {
		try {
			Path failedLogDir = this.device.getFailedLogSegmentsDirectoryPath(this.dstIndex);
			Path targetPath = failedLogDir.resolve(currentEventLogSegment.path.getFileName());
			Files.copy(currentEventLogSegment.path, targetPath, StandardCopyOption.REPLACE_EXISTING);

			consolidatorMetadataMgr.updateLastConsolidatedCDCLogSegmentSeqNum(currentEventLogSegment.sequenceNumber);

			this.lastConsolidatedChangeNumber = -1;
			this.lastConsolidatedTxnChangeNumber = -1;
			this.lastConsolidatedChangeNumberOnReplica = -1;
			this.lastConsolidatedTxnChangeNumberOnReplica = -1;
			
			device.tracer.info("Skipped failed log segment : " + currentEventLogSegment);
		} catch(Exception e) {
			device.tracer.error("Failed to skip and mark failed log segment : " + currentEventLogSegment, e);
		}
	}

	private long doApply() throws SyncLiteException {
		try {
			long currentEventLogSegmentOperCnt = 0;
			long currentEventLogSegmentTxnCnt = 0;
			boolean emptyTxn = true;
			long currentInsertBatchCount = 0;
			long currentUpdateBatchCount = 0;
			long currentDeleteBatchCount = 0;
			long currentInsertBatchCountOnReplica = 0;
			long insertBatchSize = ConfLoader.getInstance().getDstInsertBatchSize(dstIndex);			
			long updateBatchSize = ConfLoader.getInstance().getDstUpdateBatchSize(dstIndex);			
			long deleteBatchSize = ConfLoader.getInstance().getDstDeleteBatchSize(dstIndex);			

			tableStats.clear();
			Connection replicaConn = null;
			PreparedStatement replicaCheckpointUpdatePrepStmt = null;
			try (EventLogSegmentReader reader = currentEventLogSegment.open();
					SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)) {
				if (replicaAppenderEnabled || isStoreDevice) {
					replicaConn = DriverManager.getConnection("jdbc:sqlite:" + this.replicaPath);
					replicaConn.setAutoCommit(false);
					try (Statement replicaInitStmt = replicaConn.createStatement()) {
						replicaInitStmt.execute(createTxnTableSql);
						try (ResultSet rsChk = replicaInitStmt.executeQuery(selectTxnTableSql)) {
							if (!rsChk.next()) {
								replicaInitStmt.execute(insertTxnTableSql.replace("$1", String.valueOf(lastConsolidatedCommitIdOnReplica)));
							}
						}
					}
					replicaCheckpointUpdatePrepStmt = replicaConn.prepareStatement(updateTxnTableSql);
				}
				if (metadataStore == MetadataStore.DESTINATION) {
					ensureDstSchemaTableExists(dstExecutor);
				}
				dstExecutor.beginTran();
				EventLogRecord log = reader.readNextRecord();
				List<Object> afterValues = new ArrayList<Object>();
				List<Object> beforeValues = new ArrayList<Object>();
				HashMap<TableID, Insert> tblMappedInsertOpers = new HashMap<TableID, Insert>();
				HashMap<TableID, DML> tblMappedUpdateOpers = new HashMap<TableID, DML>();
				HashMap<TableID, Delete> tblMappedDeleteOpers = new HashMap<TableID, Delete>();
				HashMap<TableID, Integer> tblDeleteWhereColCount = new HashMap<TableID, Integer>();
				HashMap<TableID, Integer> tblUpdateWhereColCount = new HashMap<TableID, Integer>();
				HashMap<TableID, Integer> tblUpdateSetColCount = new HashMap<TableID, Integer>();

				ConsolidatorSrcTable srcTable = null;
				EventLogRecord lastLog = log;
				TableID prevTableId = null;
				PreparedStatement replicaInsertPrepStmt = null;
				while (log != null) {
					emptyTxn = false;
					if (log.tableName == null) {
						//Ignore invalid logs
						lastLog = log;
						log = reader.readNextRecord();
						continue;
					}
					//Avoid TableID lookup if same table logs		
					TableID tableId = null;
					if ((prevTableId != null) && (prevTableId.table.equals(log.tableName)) &&(prevTableId.database.equals(log.databaseName)))  {
						tableId = prevTableId;
					} else {
						tableId =  TableID.from(device.getDeviceUUID(), device.getDeviceName(),  this.dstIndex, log.databaseName, null, log.tableName);						
					}
					srcTable = ConsolidatorSrcTable.from(tableId);
					TableMapper tableMapper = srcTable.getIsSystemTable() ? this.systemTableMapper : this.userTableMapper;
					ValueMapper valueMapper = tableMapper.getValueMapper();
					HashMap<OperType, Long> opStats = tableStats.get(tableId);
					if (opStats == null) {
						opStats = new HashMap<OperType, Long>();
						tableStats.put(tableId, opStats);
					}
					// In consolidation mode, DROP COLUMN and DROP TABLE are intentionally ignored
					// (other devices may still use the column/table). Skip stats and log INFO.
					boolean ddlIgnoredInConsolidation = false;
					if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.CONSOLIDATION
							&& (log.opType == OperType.DROPCOLUMN || log.opType == OperType.DROPTABLE)) {
						ddlIgnoredInConsolidation = true;
						device.tracer.info("Ignoring " + log.opType + " on table " + log.tableName + " in consolidation mode - other devices may still use this column/table");
					} else {
						Long opCnt = opStats.get(log.opType);
						if (opCnt == null) {
							opStats.put(log.opType, 1L);
						} else {
							opStats.put(log.opType, opCnt + 1L);
						}
					}
					//
					//Replicate the log on replica first if populateReplica option is ON
					//Regardless of this option, we always excecute the DDL on replica file of dblogger device
					//This option is only to decide if we need to execute INSERTs and LOADs on replica file.
					//System.out.println("Log record : " + log);
					//
					//Do this only when dstIndex is set to 1. We should apply changes to replica only once.
					//We may have multiple DeviceEventStreamers one per destination.
					//
					try {
						if ((log.commitId > this.lastConsolidatedCommitIdOnReplica) || ((log.commitId == this.lastConsolidatedCommitIdOnReplica) && (log.changeNumber > this.lastConsolidatedChangeNumberOnReplica)) || ((log.commitId == this.lastConsolidatedCommitIdOnReplica) && (log.changeNumber == this.lastConsolidatedChangeNumberOnReplica) && (log.txnChangeNumber > this.lastConsolidatedTxnChangeNumberOnReplica))) {
							if (log.ddlInfo == null) {
								if (this.replicaAppenderEnabled) {
									//Execute INSERT on replica for appender/store devices
									//UPDATE and DELETE on replica supported only for store devices.
									if (log.opType == OperType.INSERT) {
										if (replicaInsertPrepStmt != null) {
											//Check if this was on a different table, if yes then flush it out.
											if (tableId != prevTableId) {
												//Table has changed
												replicaInsertPrepStmt.executeBatch();
												replicaInsertPrepStmt.close();
												replicaInsertPrepStmt = null;
												currentInsertBatchCountOnReplica = 0;
												//Execute checkpoint UPDATE
												bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, log.commitId, log.changeNumber, log.txnChangeNumber);
												replicaConn.commit();
												this.lastConsolidatedCommitIdOnReplica = log.commitId;
												this.lastConsolidatedChangeNumberOnReplica = log.changeNumber;
												replicaConn.setAutoCommit(false);
											}
										} 
										if (replicaInsertPrepStmt == null) {
											replicaInsertPrepStmt = replicaConn.prepareStatement(getReplicaInsertPrepStmt(srcTable, log.argValues.size()));										
										}									
										bindReplicaInsertPrepStmt(srcTable, replicaInsertPrepStmt, log.argValues, log.colMap);
										++currentInsertBatchCountOnReplica;
										if (currentInsertBatchCountOnReplica == insertBatchSize) {
											if (replicaInsertPrepStmt != null) {
												replicaInsertPrepStmt.executeBatch();
												replicaInsertPrepStmt.close();
												replicaInsertPrepStmt = null;
											}
											currentInsertBatchCountOnReplica = 0;
											//Execute checkpoint UPDATE
											bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, log.commitId, log.changeNumber, log.txnChangeNumber);
											replicaConn.commit();
											this.lastConsolidatedCommitIdOnReplica = log.commitId;
											this.lastConsolidatedChangeNumberOnReplica = log.changeNumber;
											
										}
									}
								} else if (this.isStoreDevice) {
									if (log.opType == OperType.UPDATE) {
										//Flush any pending INSERT batch
										if (currentInsertBatchCountOnReplica > 0) {
											if (replicaInsertPrepStmt != null) {
												replicaInsertPrepStmt.executeBatch();
												replicaInsertPrepStmt.close();
												replicaInsertPrepStmt = null;
											}
											currentInsertBatchCountOnReplica = 0;
											if (lastLog != null) {
												bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
											}
											replicaConn.commit();
											replicaConn.setAutoCommit(false);
											if (lastLog != null) {
												this.lastConsolidatedCommitIdOnReplica = lastLog.commitId;
												this.lastConsolidatedChangeNumberOnReplica = lastLog.changeNumber;
											}
										}
										//Reset insert prepared statement as we are switching to UPDATE
										if (replicaInsertPrepStmt != null) {
											replicaInsertPrepStmt.close();
											replicaInsertPrepStmt = null;
										}
										//Execute UPDATE on replica
										try (java.sql.PreparedStatement replicaUpdatePrepStmt = replicaConn.prepareStatement(getReplicaUpdatePrepStmt(srcTable))) {
											bindReplicaUpdatePrepStmt(srcTable, replicaUpdatePrepStmt, log.argValues, log.argCnt);
											replicaUpdatePrepStmt.executeUpdate();
										}
										bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, log.commitId, log.changeNumber, log.txnChangeNumber);
										replicaConn.commit();
										this.lastConsolidatedCommitIdOnReplica = log.commitId;
										this.lastConsolidatedChangeNumberOnReplica = log.changeNumber;
										replicaConn.setAutoCommit(false);
									} else if (log.opType == OperType.DELETE) {
										//Flush any pending INSERT batch
										if (currentInsertBatchCountOnReplica > 0) {
											if (replicaInsertPrepStmt != null) {
												replicaInsertPrepStmt.executeBatch();
												replicaInsertPrepStmt.close();
												replicaInsertPrepStmt = null;
											}
											currentInsertBatchCountOnReplica = 0;
											if (lastLog != null) {
												bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
											}
											replicaConn.commit();
											replicaConn.setAutoCommit(false);
											if (lastLog != null) {
												this.lastConsolidatedCommitIdOnReplica = lastLog.commitId;
												this.lastConsolidatedChangeNumberOnReplica = lastLog.changeNumber;
											}
										}
										//Reset insert prepared statement as we are switching to DELETE
										if (replicaInsertPrepStmt != null) {
											replicaInsertPrepStmt.close();
											replicaInsertPrepStmt = null;
										}
										//Execute DELETE on replica
										try (java.sql.PreparedStatement replicaDeletePrepStmt = replicaConn.prepareStatement(getReplicaDeletePrepStmt(srcTable))) {
											bindReplicaDeletePrepStmt(srcTable, replicaDeletePrepStmt, log.argValues);
											replicaDeletePrepStmt.executeUpdate();
										}
										bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, log.commitId, log.changeNumber, log.txnChangeNumber);
										replicaConn.commit();
										this.lastConsolidatedCommitIdOnReplica = log.commitId;
										this.lastConsolidatedChangeNumberOnReplica = log.changeNumber;
										replicaConn.setAutoCommit(false);
									}
								}
							} else if (log.ddlInfo != null) {
								if (currentInsertBatchCountOnReplica > 0) {
									if (replicaInsertPrepStmt != null) {
										replicaInsertPrepStmt.executeBatch();
										replicaInsertPrepStmt.close();
										replicaInsertPrepStmt = null;
									}
									currentInsertBatchCountOnReplica = 0;
									//Execute checkpoint UPDATE
									if (lastLog != null) {
										bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
									}
									replicaConn.commit();
									replicaConn.setAutoCommit(false);
									if (lastLog != null) {
										this.lastConsolidatedCommitIdOnReplica = lastLog.commitId;
										this.lastConsolidatedChangeNumberOnReplica = lastLog.changeNumber;
									}
								}
								//Reset prepared statement as schema may change as a result of DDL below.
								if (replicaInsertPrepStmt != null) {
									replicaInsertPrepStmt.close();
									replicaInsertPrepStmt = null;
								}

//Always apply DDL on in-memory replica (for fetchColumns in DDL switch below)
							try {
								executeDDLOnReplica(inMemoryReplicaConn, srcTable, log.opType, log.sql, log.ddlInfo);
							} catch (SQLException memEx) {
								//In-memory DDL may be idempotent; ignore
							}
							//Apply on on-disk replica only when enabled
							if (replicaConn != null) {
								executeDDLOnReplica(replicaConn, srcTable, log.opType, log.sql, log.ddlInfo);
								bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, log.commitId, log.changeNumber, log.txnChangeNumber);
								replicaConn.commit();
								this.lastConsolidatedCommitIdOnReplica = log.commitId;
								this.lastConsolidatedChangeNumberOnReplica = log.changeNumber;
								replicaConn.setAutoCommit(false);
							}
							} else {
								//throw new SyncLiteException("Invalid event log received : " + log.sql);
								//Ignore invalid event logs
							}
						}

					} catch (SQLException e) {
						throw new SyncLiteException("Failed to apply log to replica file : " + this.replicaPath, e);
					}

					//Check if table is allowed on destination else skip log.
					if (!ConfLoader.getInstance().isAllowedTable(dstIndex, log.tableName)) {
						lastLog = log;
						log = reader.readNextRecord();
						continue;
					}
					
					//Skip log based on lastConsolidated commitid and changenumber
					if ((log.commitId > this.lastConsolidatedCommitId) || ((log.commitId == this.lastConsolidatedCommitId) && (log.changeNumber > this.lastConsolidatedChangeNumber)) || ((log.commitId == this.lastConsolidatedCommitId) && (log.changeNumber == this.lastConsolidatedChangeNumber) && (log.txnChangeNumber > this.lastConsolidatedTxnChangeNumber))) {
						//
						//Check if prevOper or prevTable is different than this log's oper and table..
						//If yes then flush the existing batches
						//
						device.tracer.info("DIAG: cn=" + log.changeNumber + " commitId=" + log.commitId + " opType=" + log.opType + " table=" + log.tableName + " prevOp=" + (lastLog != null ? lastLog.opType : "null") + " insertCnt=" + currentInsertBatchCount + " deleteCnt=" + currentDeleteBatchCount + " argCnt=" + log.argCnt + " args=" + log.argValues);
						if (((prevTableId != null) && (prevTableId != tableId)) || ((lastLog != null) && (lastLog.opType != log.opType))) {
							if ((currentInsertBatchCount > 0) || (currentUpdateBatchCount > 0 ) || (currentDeleteBatchCount > 0)) {				
								//Execute checkpoint UPDATE
								device.tracer.info("DIAG: FLUSH BLOCK entered. insertCnt=" + currentInsertBatchCount + " deleteCnt=" + currentDeleteBatchCount + " updateCnt=" + currentUpdateBatchCount);
								if (lastLog != null) {
									updateDstCheckpointIfNeeded(dstExecutor, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber, beforeValues, afterValues);
								}
								beforeValues.clear();
								afterValues.clear();

								commitDstTran(dstExecutor);

								if (lastLog != null) {
									updateLocalCheckpointIfNeeded(lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
								}

								if (currentInsertBatchCount > 0) {
									device.tracer.info("Committed insert/upsert/replace batch of size : " + currentInsertBatchCount);
								}
								
								if (currentUpdateBatchCount > 0) {
									device.tracer.info("Committed update batch of size : " + currentUpdateBatchCount);
								}

								if (currentDeleteBatchCount > 0) {
									device.tracer.info("Committed delete batch of size : " + currentDeleteBatchCount);
								}

								++currentEventLogSegmentTxnCnt;
								++consolidatedTxnCount;
								if (lastLog != null) {
									this.lastConsolidatedCommitId = lastLog.commitId;
									this.lastConsolidatedChangeNumber = lastLog.changeNumber;
									this.lastConsolidatedTxnChangeNumber = lastLog.txnChangeNumber;
								}
								emptyTxn = true;
								dstExecutor.beginTran();
								currentInsertBatchCount = 0;
								currentUpdateBatchCount = 0;
								currentDeleteBatchCount = 0;
							}
						}
						if (log.ddlInfo == null) {
							//Execute INSERT/LOAD
							if (log.opType == OperType.INSERT) {
								//
								//Check if column filters are specified then iterate on columns and
								//Filter out log.args for columns which are not allowed.
								//
								boolean hasFilterMapper = false;
								if (ConfLoader.getInstance().getDstEnableFilterMapperRules(dstIndex) || ConfLoader.getInstance().getDstEnableValueMapper(dstIndex)) {
									if (ConfLoader.getInstance().tableHasFilterMapperRules(dstIndex, log.tableName) || ConfLoader.getInstance().tableHasValueMappings(dstIndex, log.tableName)) {
										hasFilterMapper = true;
									}
								}
								List<Object> argValues = log.argValues;
								if (hasFilterMapper) {
									if (log.colMap == null) {
										for(int i = 0; i < argValues.size(); ++i) {
											Column col = srcTable.columns.get(i);
											if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, log.tableName, col.column)) {
												continue;
											}
											Object colVal = valueMapper.mapValue(tableId, col, argValues.get(i));											
											afterValues.add(colVal);
										}
										argValues = afterValues;
									} else {
										//
										//We have to use column ordering as specified in colMap
										//colMap is populated if INSERT was done with column list
										//
										for (Column col : srcTable.columns) {
											if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, log.tableName, col.column)) {
												continue;
											}
											Object colVal = null;
											Integer colIdx = log.colMap.get(col.column);
											if (colIdx != null) {
												colVal = argValues.get(colIdx);
											}
											afterValues.add(colVal);
										}
										argValues = afterValues;
									}
								} else {
									if (log.colMap != null) {
										//
										//We have to use column ordering as specified in colMap
										//colMap is populated if INSERT was done with column list
										//
										//No filter mapper case implemented here
										//
										for (Column col : srcTable.columns) {
											Object colVal = null;
											Integer colIdx = log.colMap.get(col.column);
											if (colIdx != null) {
												colVal = argValues.get(colIdx);
											}
											afterValues.add(colVal);
										}
										argValues = afterValues;										
									}
								}
								//
								//Optimization to avoid new INSERT operation, new mapped operation 
								//and multiple copies of args being created here
								//
								Insert mappedInsert = tblMappedInsertOpers.get(srcTable.id);
								if (mappedInsert == null) {
									Insert srcInsert = new Insert(srcTable, argValues, applyInsertIdempotently);
									mappedInsert = tableMapper.mapInsert(srcInsert);
									tblMappedInsertOpers.put(srcTable.id, mappedInsert);
								} else {
									tableMapper.bindMappedInsert(mappedInsert, argValues);
								}								
								dstExecutor.execute(mappedInsert);
								currentTxnDstTables.add(mappedInsert.tbl.id);
								beforeValues.clear();
								afterValues.clear();
								++currentInsertBatchCount;
								emptyTxn = false;
								++currentEventLogSegmentOperCnt;
								if (currentInsertBatchCount == insertBatchSize) {						
									//Execute checkpoint UPDATE
									
									updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
									beforeValues.clear();
									afterValues.clear();

									commitDstTran(dstExecutor);
									
									updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
									
									device.tracer.info("Committed insert/upsert/replace batch of size : " + currentInsertBatchCount);
									
									this.lastConsolidatedCommitId = log.commitId;
									this.lastConsolidatedChangeNumber = log.changeNumber;
									this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
									
									++currentEventLogSegmentTxnCnt;
									++consolidatedTxnCount;
									emptyTxn = true;
									dstExecutor.beginTran();
									currentInsertBatchCount = 0;
								}
							} else if (log.opType == OperType.UPDATE) {
								//
								// When argCnt == 0 the log carries a raw SQL UPDATE (no bound args).
								// Handle it like DELETE_IF_PREDICATE: scope to this device and execute natively.
								//
								if (log.argCnt == 0 && log.sql != null && !log.sql.isBlank()) {
									UpdateIfPredicate sqlStmt = new UpdateIfPredicate(srcTable, log.sql);
									dstExecutor.execute(sqlStmt.map(tableMapper));
									updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
									beforeValues.clear();
									afterValues.clear();
									commitDstTran(dstExecutor);
									updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
									emptyTxn = true;
									++currentEventLogSegmentTxnCnt;
									++consolidatedTxnCount;
									this.lastConsolidatedCommitId = log.commitId;
									this.lastConsolidatedChangeNumber = log.changeNumber;
									this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
									dstExecutor.beginTran();
									++currentEventLogSegmentOperCnt;
								} else {
								boolean hasFilterMapper = false;
								if (ConfLoader.getInstance().getDstEnableFilterMapperRules(dstIndex) || ConfLoader.getInstance().getDstEnableValueMapper(dstIndex)) {
									if (ConfLoader.getInstance().tableHasFilterMapperRules(dstIndex, log.tableName) || ConfLoader.getInstance().tableHasValueMappings(dstIndex, log.tableName)) {
										hasFilterMapper = true;
									}
								}
								
								//
								//Resolve SET and WHERE columns from logged SQL for partial-column UPDATEs.
								//When argCnt matches 2 * numColumns, use existing full-row split.
								//Otherwise, use parsed SET/WHERE columns to split args properly.
								//
								List<Column> resolvedSetColumns = null;
								List<Column> resolvedWhereColumns = null;
								boolean usePartialColumns = false;
								
								if (log.argCnt != 2 * srcTable.columns.size() && log.setColNames != null && log.whereColNames != null) {
									resolvedSetColumns = new ArrayList<>();
									resolvedWhereColumns = new ArrayList<>();
									boolean allResolved = true;
									for (String colName : log.setColNames) {
										Column col = srcTable.getColumnByName(colName);
										if (col != null) {
											resolvedSetColumns.add(col);
										} else {
											allResolved = false;
											break;
										}
									}
									if (allResolved) {
										for (String colName : log.whereColNames) {
											Column col = srcTable.getColumnByName(colName);
											if (col != null) {
												resolvedWhereColumns.add(col);
											} else {
												allResolved = false;
												break;
											}
										}
									}
									if (allResolved && (resolvedSetColumns.size() + resolvedWhereColumns.size() == log.argCnt)) {
										usePartialColumns = true;
										
										//Check if WHERE columns contain all PK columns → reduce if so
										if (srcTable.hasPrimaryKey()) {
											boolean coversAllPKs = true;
											for (Column c : srcTable.columns) {
												if (c.pkIndex > 0) {
													boolean found = false;
													for (Column wc : resolvedWhereColumns) {
														if (wc.column.equals(c.column)) {
															found = true;
															break;
														}
													}
													if (!found) {
														coversAllPKs = false;
														break;
													}
												}
											}
											if (coversAllPKs) {
												List<Column> pkCols = new ArrayList<>();
												resolvedWhereColumns = pkCols;
												for (Column c : srcTable.columns) {
													if (c.pkIndex > 0) {
														pkCols.add(c);
													}
												}
												resolvedWhereColumns = pkCols;
											}
										}
									} else {
										resolvedSetColumns = null;
										resolvedWhereColumns = null;
									}
								}
								
								if (usePartialColumns) {
									//Split args: SET values first (JDBC order), then WHERE values
									int numSetCols = resolvedSetColumns.size();
									for (int i = 0; i < numSetCols; ++i) {
										Column col = resolvedSetColumns.get(i);
										Object colAfterVal = valueMapper.mapValue(tableId, col, log.argValues.get(i));
										afterValues.add(colAfterVal);
									}
									for (int i = 0; i < resolvedWhereColumns.size(); ++i) {
										Column col = resolvedWhereColumns.get(i);
										Object colBeforeVal = valueMapper.mapValue(tableId, col, log.argValues.get(numSetCols + i));
										beforeValues.add(colBeforeVal);
									}
								} else {
								for(int i = 1; i <= log.argCnt/2 ; ++i) {
									Column col = srcTable.columns.get(i-1);
									if (!ConfLoader.getInstance().isAllowedColumn(dstIndex, log.tableName, col.column)) {
										continue;
									}																			
									Object colBeforeVal = valueMapper.mapValue(tableId, col, log.argValues.get(i-1));
									beforeValues.add(colBeforeVal);

									int afterValIndex = (int) ((log.argCnt/2) + i - 1);
									Object colAfterVal = valueMapper.mapValue(tableId, col, log.argValues.get(afterValIndex));
									afterValues.add(colAfterVal);
								}
								}

								//
								//Optimization to avoid new UPDATE operation, new mapped operation 
								//and multiple copies of args being created here
								//
								DML mappedUpdate = tblMappedUpdateOpers.get(srcTable.id);
								int curUpdateWhereColCount = (usePartialColumns && resolvedWhereColumns != null) ? resolvedWhereColumns.size() : -1;
								int curUpdateSetColCount = (usePartialColumns && resolvedSetColumns != null) ? resolvedSetColumns.size() : -1;
								Integer prevUpdateWhereColCount = tblUpdateWhereColCount.get(srcTable.id);
								Integer prevUpdateSetColCount = tblUpdateSetColCount.get(srcTable.id);
								if (mappedUpdate == null || prevUpdateWhereColCount == null || prevUpdateSetColCount == null ||
										curUpdateWhereColCount != prevUpdateWhereColCount.intValue() ||
										curUpdateSetColCount != prevUpdateSetColCount.intValue()) {
									Update srcUpdate = new Update(srcTable, beforeValues, afterValues);
									if (usePartialColumns) {
										srcUpdate.whereColumns = resolvedWhereColumns;
										srcUpdate.setColumns = resolvedSetColumns;
									}
									mappedUpdate= (DML) tableMapper.mapOper(srcUpdate).get(0);
									tblMappedUpdateOpers.put(srcTable.id, mappedUpdate);
									tblUpdateWhereColCount.put(srcTable.id, curUpdateWhereColCount);
									tblUpdateSetColCount.put(srcTable.id, curUpdateSetColCount);
								} else {
									if (mappedUpdate.operType == OperType.DELETEINSERT) {
										tableMapper.bindMappedInsert(mappedUpdate, afterValues);
									} else {
										tableMapper.bindMappedUpdate(mappedUpdate, beforeValues, afterValues);
									}
								}
								dstExecutor.execute(mappedUpdate);
								currentTxnDstTables.add(mappedUpdate.tbl.id);
								beforeValues.clear();
								afterValues.clear();
								++currentUpdateBatchCount;
								emptyTxn = false;
								++currentEventLogSegmentOperCnt;
								if (currentUpdateBatchCount == updateBatchSize) {						
									//Execute checkpoint UPDATE
									
									updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, lastLog.txnChangeNumber, beforeValues, afterValues);
									beforeValues.clear();
									afterValues.clear();

									commitDstTran(dstExecutor);
									
									updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
									
									device.tracer.info("Committed update batch of size : " + currentInsertBatchCount);
									
									this.lastConsolidatedCommitId = log.commitId;
									this.lastConsolidatedChangeNumber = log.changeNumber;
									this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
									
									++currentEventLogSegmentTxnCnt;
									++consolidatedTxnCount;
									emptyTxn = true;
									dstExecutor.beginTran();
									currentUpdateBatchCount = 0;
								}
								} // end else (argCnt != 0)
							} else if (log.opType == OperType.DELETE) {
								//
								//Check if column filters are specified then iterate on columns and
								//Filter out log.args for columns which are not allowed.
								//
								boolean hasFilterMapper = false;
								if (ConfLoader.getInstance().getDstEnableFilterMapperRules(dstIndex) || ConfLoader.getInstance().getDstEnableValueMapper(dstIndex)) {
									if (ConfLoader.getInstance().tableHasFilterMapperRules(dstIndex, log.tableName) || ConfLoader.getInstance().tableHasValueMappings(dstIndex, log.tableName)) {
										hasFilterMapper = true;
									}
								}

								//
								//Resolve WHERE columns from the logged SQL to determine which columns
								//the argValues correspond to. This handles cases where DELETE only
								//specifies a subset of columns (e.g., PK only) in the WHERE clause.
								//
								List<Column> resolvedWhereColumns = null;
								if (log.whereColNames != null && log.argValues.size() < srcTable.columns.size()) {
									resolvedWhereColumns = new ArrayList<>();
									boolean allResolved = true;
									for (String colName : log.whereColNames) {
										Column col = srcTable.getColumnByName(colName);
										if (col != null) {
											resolvedWhereColumns.add(col);
										} else {
											allResolved = false;
											break;
										}
									}
									if (!allResolved || resolvedWhereColumns.size() != log.argValues.size()) {
										resolvedWhereColumns = null;
									} else if (srcTable.hasPrimaryKey()) {
										//Check if the WHERE columns contain all PK columns
										boolean coversAllPKs = true;
										for (Column c : srcTable.columns) {
											if (c.pkIndex > 0) {
												boolean found = false;
												for (Column wc : resolvedWhereColumns) {
													if (wc.column.equals(c.column)) {
														found = true;
														break;
													}
												}
												if (!found) {
													coversAllPKs = false;
													break;
												}
											}
										}
										if (coversAllPKs) {
											//Reduce to PK-only WHERE columns
											List<Column> pkCols = new ArrayList<>();
											List<Object> pkValues = new ArrayList<>();
											for (int idx = 0; idx < resolvedWhereColumns.size(); idx++) {
												if (resolvedWhereColumns.get(idx).pkIndex > 0) {
													pkCols.add(resolvedWhereColumns.get(idx));
													pkValues.add(log.argValues.get(idx));
												}
											}
											resolvedWhereColumns = pkCols;
											log.argValues = pkValues;
										}
									}
								}

								List<Object> argValues = log.argValues;
								if (hasFilterMapper && resolvedWhereColumns != null) {
									for(int i = 0; i < argValues.size(); ++i) {
										Column col = resolvedWhereColumns.get(i);
										if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, log.tableName, col.column)) {
											continue;
										}
										Object colVal = valueMapper.mapValue(tableId, col, argValues.get(i));											
										afterValues.add(colVal);
									}
									argValues = afterValues;
								} else if (hasFilterMapper) {
									for(int i = 0; i < argValues.size(); ++i) {
										Column col = srcTable.columns.get(i);
										if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, log.tableName, col.column)) {
											continue;
										}
										Object colVal = valueMapper.mapValue(tableId, col, argValues.get(i));											
										afterValues.add(colVal);
									}
									argValues = afterValues;
								}
								//
								//Optimization to avoid new DELETE operation, new mapped operation 
								//and multiple copies of args being created here
								//
								Delete mappedDelete= tblMappedDeleteOpers.get(srcTable.id);
								int curDeleteWhereColCount = (resolvedWhereColumns != null) ? resolvedWhereColumns.size() : -1;
								Integer prevDeleteWhereColCount = tblDeleteWhereColCount.get(srcTable.id);
								if (mappedDelete == null || prevDeleteWhereColCount == null || curDeleteWhereColCount != prevDeleteWhereColCount.intValue()) {
									Delete srcDelete = new Delete(srcTable, argValues);
									srcDelete.whereColumns = resolvedWhereColumns;
									mappedDelete= (Delete) tableMapper.mapOper(srcDelete).get(0);
									tblMappedDeleteOpers.put(srcTable.id, mappedDelete);
									tblDeleteWhereColCount.put(srcTable.id, curDeleteWhereColCount);
								} else {
									tableMapper.bindMappedDelete(mappedDelete, argValues);
								}
								dstExecutor.execute(mappedDelete);
								currentTxnDstTables.add(mappedDelete.tbl.id);
								beforeValues.clear();
								afterValues.clear();
								++currentDeleteBatchCount;
								emptyTxn = false;
								++currentEventLogSegmentOperCnt;
								if (currentDeleteBatchCount == deleteBatchSize) {						
									//Execute checkpoint UPDATE
									
									updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
									beforeValues.clear();
									afterValues.clear();

									commitDstTran(dstExecutor);
									
									updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
									
									device.tracer.info("Committed delete batch of size : " + currentInsertBatchCount);
									
									this.lastConsolidatedCommitId = log.commitId;
									this.lastConsolidatedChangeNumber = log.changeNumber;
									this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;

									++currentEventLogSegmentTxnCnt;
									++consolidatedTxnCount;
									emptyTxn = true;
									dstExecutor.beginTran();
									currentDeleteBatchCount = 0;
								}
							} else if ((log.opType == OperType.DELETE_IF_PREDICATE) || (log.opType == OperType.MINUS) || (log.opType == OperType.FINISHBATCH)) {
								//This has been introduced for dbreader app ( to support DB to DB incremental replication)

								Oper sqlStmt = null;
								switch (log.opType) {
								case DELETE_IF_PREDICATE:
									//
									//This has been introduced for dbreader app 
									//to support soft delete replication in incremental replication)
									//								
									sqlStmt = new DeleteIfPredicate(srcTable, log.sql);
									break;
								case MINUS:
									//
									//This has been introduced for dbreader app 
									//to support delete synchronization in incremental replication)
									//								
									sqlStmt = new Minus(srcTable, log.sql);
									break;
								case FINISHBATCH:
									//
									//This has been introduced for dbreader app 
									//to support data export on each batch for data lake target
									//								
									sqlStmt = new FinishBatch(srcTable);
									break;
								default:
								}
								
								dstExecutor.execute(sqlStmt.map(tableMapper));
								updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
								beforeValues.clear();
								afterValues.clear();
								
								commitDstTran(dstExecutor);
								updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
								emptyTxn = true;
								++currentEventLogSegmentTxnCnt;
								++consolidatedTxnCount;
								
								this.lastConsolidatedCommitId = log.commitId;
								this.lastConsolidatedChangeNumber = log.changeNumber;
								this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
								
								dstExecutor.beginTran();
								++currentEventLogSegmentOperCnt;
							} else if (log.opType == OperType.SHUTDOWN){ 
								//FINISH REPLICATION received. terminate the job
								updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
								commitDstTran(dstExecutor);
								updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
								SyncDriver.getInstance().shutdownJob();
							} else if (log.opType == OperType.LOAD) {
								//TODO
								//DEPRECATE LOAD

								Path dataFileInUpload = Path.of(device.getDeviceUploadRoot().toString(), log.argValues.get(1).toString());								
								Path dataFileDirInRoot = Path.of(device.getDeviceDataRoot().toString(), currentEventLogSegment.path.getFileName() + ".data");
								Path dataFileInRoot = Path.of(dataFileDirInRoot.toString(), log.argValues.get(1).toString());
								try {									
									Files.createDirectories(dataFileDirInRoot);									
									if (Files.exists(dataFileInUpload)) {
										Files.copy(dataFileInUpload, dataFileInRoot, StandardCopyOption.REPLACE_EXISTING);
									} else {
										throw new DstExecutionException("Data file " + dataFileInUpload + " missing");
									}
								} catch (IOException e) {
									throw new DstExecutionException("Failed to copy data file from " + dataFileInUpload + " to " + dataFileInRoot); 
								}							

								String format = log.argValues.get(2).toString();
								char delimiter = log.argValues.get(3).toString().charAt(0);
								String nullString = log.argValues.get(4).toString();
								boolean header = (Integer.valueOf(log.argValues.get(5).toString()) == 1);
								char quote =  log.argValues.get(6).toString().charAt(0);
								char escape = log.argValues.get(7).toString().charAt(0);
								LoadFile loadOper = new LoadFile(srcTable, Collections.EMPTY_MAP, dataFileInRoot, format, delimiter, nullString, header, quote, escape);
								List<Oper> mappedOpers = tableMapper.mapOper(loadOper);
								dstExecutor.execute(mappedOpers);

								//Get the inserted record count from executed LoadFile opers

								long loadedRecordCount = 0;
								for (Oper oper : mappedOpers) {
									if (oper instanceof LoadFile) {
										loadedRecordCount += ((LoadFile) oper).loadedRecordCount;
									}
								}
								//Bump up INSERT stats								
								Long insertOpCnt = opStats.get(OperType.INSERT);
								if (insertOpCnt == null) {
									opStats.put(OperType.INSERT, loadedRecordCount);
								} else {
									opStats.put(OperType.INSERT, insertOpCnt + loadedRecordCount);
								}										
								updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
								beforeValues.clear();
								afterValues.clear();
								commitDstTran(dstExecutor);
								updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
								
								this.lastConsolidatedCommitId = log.commitId;
								this.lastConsolidatedChangeNumber = log.changeNumber;
								this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
								
								emptyTxn = true;
								++currentEventLogSegmentTxnCnt;
								++consolidatedTxnCount;
								dstExecutor.beginTran();
								++currentEventLogSegmentOperCnt;
							}
						} else if (log.ddlInfo != null) {
							//Handle all DDLs
							//Get schema from in-memory replica (already updated by executeDDLOnReplica above)
							//to construct DDL Oper for dst. No on-disk replica file needed.

							switch(log.ddlInfo.ddlType) {
							case CREATETABLE:
								//executeDDLIgnoreException(log.sql);
								List<Column> newTableCols = device.schemaReader.fetchColumns(inMemoryReplicaConn, srcTable.id);
								Oper createTableOper= srcTable.generateCreateTableOper(tableMapper, newTableCols);
								if (createTableOper != null) {
									dstExecutor.execute(createTableOper.map(tableMapper));
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case DROPTABLE:
								//executeDDLIgnoreException(log.sql);
								Oper dropTableOper = srcTable.generateDropTableOper(tableMapper);
								if (dropTableOper != null) {
									dstExecutor.execute(dropTableOper.map(tableMapper));
									try {
										consolidatorMetadataMgr.deleteSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									deleteSchemaAfterDrop(dstExecutor, srcTable);
								}
								//Remove src and dst tables
								ConsolidatorDstTable dstTable = tableMapper.mapTable(srcTable);
								ConsolidatorDstTable.remove(dstTable.id);								
								ConsolidatorDstTable.remove(srcTable.id);								
								break;
							case ADDCOLUMN:
								//executeDDLIgnoreException(log.sql);
								newTableCols = device.schemaReader.fetchColumns(inMemoryReplicaConn, srcTable.id);
								AddColumn addColOper= (AddColumn) srcTable.generateAddColumnOper(newTableCols);
								if (addColOper != null) {
									dstExecutor.execute(addColOper.map(tableMapper));
									srcTable.applyAddColumn(addColOper);
									tblMappedInsertOpers.remove(srcTable.id);
									tblMappedUpdateOpers.remove(srcTable.id);
									tblMappedDeleteOpers.remove(srcTable.id);
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case DROPCOLUMN:
								//executeDDLIgnoreException(log.sql);
								newTableCols = device.schemaReader.fetchColumns(inMemoryReplicaConn, srcTable.id);
								DropColumn dropColOper= (DropColumn) srcTable.generateDropColumnOper(newTableCols);
								if (dropColOper != null) {
									dstExecutor.execute(dropColOper.map(tableMapper));
									srcTable.applyDropColumn(dropColOper);
									tableMapper.remove(srcTable);
									tblMappedInsertOpers.remove(srcTable.id);
									tblMappedUpdateOpers.remove(srcTable.id);
									tblMappedDeleteOpers.remove(srcTable.id);
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case ALTERCOLUMN:
								newTableCols = device.schemaReader.fetchColumns(inMemoryReplicaConn, srcTable.id);
								AlterColumn alterColOper= (AlterColumn) srcTable.generateAlterColumnOper(newTableCols);
								if (alterColOper != null) {
									dstExecutor.execute(alterColOper.map(tableMapper));
									srcTable.applyAlterColumn(alterColOper);
									tableMapper.remove(srcTable);
									tblMappedInsertOpers.remove(srcTable.id);
									tblMappedUpdateOpers.remove(srcTable.id);
									tblMappedDeleteOpers.remove(srcTable.id);
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case RENAMECOLUMN:
								//executeDDLIgnoreException(log.sql);
								Oper renameColOper= srcTable.generateRenameColumnOper(log.ddlInfo.oldColumnName, log.ddlInfo.columnName);
								if (renameColOper != null) {
									dstExecutor.execute(renameColOper.map(tableMapper));
									tableMapper.remove(srcTable);
									tblMappedInsertOpers.remove(srcTable.id);
									tblMappedUpdateOpers.remove(srcTable.id);
									tblMappedDeleteOpers.remove(srcTable.id);
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case RENAMETABLE:
								Oper renameTableOper = srcTable.generateRenameTableOper(tableMapper, log.ddlInfo.oldTableName, log.ddlInfo.tableName);
								if (renameTableOper != null) {
									dstExecutor.execute(renameTableOper.map(tableMapper));
									try {
										consolidatorMetadataMgr.upsertSchema(srcTable);
									} catch (SQLException e) {
										throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
									}
									persistSchemaAfterDDL(dstExecutor, srcTable);
								}
								break;
							case REFRESHTABLE:
								//Remove dst tables
								dstTable = tableMapper.mapTable(srcTable);
								ConsolidatorDstTable.remove(dstTable.id);								
								newTableCols = device.schemaReader.fetchColumns(inMemoryReplicaConn, srcTable.id);
								//refresh schema for srcTable
								srcTable.refreshColumns(tableMapper, newTableCols);
								//Reload dst table
								dstTable = tableMapper.mapTable(srcTable);
								
								//refresh stored src schema 
								try {
									consolidatorMetadataMgr.upsertSchema(srcTable);
								} catch (SQLException e) {
									throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
								}
								persistSchemaAfterDDL(dstExecutor, srcTable);
								break;								
							case PUBLISHCOLUMNLIST:
								dstTable = tableMapper.mapTable(srcTable);
								ConsolidatorDstTable.remove(dstTable.id);
								
								//This operation is introduced to make the order of columns in Table object
								//same as that on the source db as it may have changed as a result of DDL operations
								//We need to have the order same at all times for INSERT without collist to work correctly.
								//
								//Reorder column objects as per the list received in this operation.
								
								List<Column> newColObjList = new ArrayList<Column>();
								String[] newColNames =  log.ddlInfo.columnName.split(",");
								for (String colName : newColNames) {
									Column colObj = srcTable.colMap.get(colName);
									newColObjList.add(colObj);
								}
								//refresh schema for srcTable
								srcTable.refreshColumns(tableMapper, newColObjList);
								
								//Reload dst table
								dstTable = tableMapper.mapTable(srcTable);
								
								//refresh stored src schema 
								try {
									consolidatorMetadataMgr.upsertSchema(srcTable);
								} catch (SQLException e) {
									throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
								}
								persistSchemaAfterDDL(dstExecutor, srcTable);
								break;
								
							default:
								break;
							}
							// Invalidate DML operation caches for this table. DDL may have changed the
							// table schema (ADDCOLUMN, DROPCOLUMN, ALTERCOLUMN, REFRESHTABLE, etc.), and
							// any previously-cached mapped INSERT/UPDATE/DELETE would generate stale SQL
							// with wrong columns (e.g. INSERT including a dropped column, or missing a
							// newly added column). Removing forces rebuild from the updated srcTable schema.
							tblMappedInsertOpers.remove(srcTable.id);
							tblMappedUpdateOpers.remove(srcTable.id);
							tblMappedDeleteOpers.remove(srcTable.id);
							updateDstCheckpointIfNeeded(dstExecutor, log.commitId, log.changeNumber, log.txnChangeNumber, beforeValues, afterValues);
							beforeValues.clear();
							afterValues.clear();
							commitDstTran(dstExecutor);
							updateLocalCheckpointIfNeeded(log.commitId, log.changeNumber, log.txnChangeNumber);
							emptyTxn = true;
							if (!ddlIgnoredInConsolidation) {
								++currentEventLogSegmentTxnCnt;
								++consolidatedTxnCount;
								++currentEventLogSegmentOperCnt;
							}
							
							this.lastConsolidatedCommitId = log.commitId;
							this.lastConsolidatedChangeNumber = log.changeNumber;
							this.lastConsolidatedTxnChangeNumber = log.txnChangeNumber;
							
							dstExecutor.beginTran();
							} else {
							//throw new SyncLiteException("Invalid event log received : " + log.sql);
							//Ignore invalid event logs
						}						
					}
					lastLog = log;
					prevTableId = tableId;
					log = reader.readNextRecord();
				}

				if (!emptyTxn) {
					//execute and commit the last non empty transaction on replica
					if (replicaConn != null && currentInsertBatchCountOnReplica > 0) {
						if (replicaInsertPrepStmt != null) {
							replicaInsertPrepStmt.executeBatch();
							replicaInsertPrepStmt.close();
							replicaInsertPrepStmt = null;
						}
						bindAndExecuteReplicaCheckpointUpdatePrepStmt(replicaCheckpointUpdatePrepStmt, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
						replicaConn.commit();
						replicaConn.setAutoCommit(false);
						this.lastConsolidatedCommitIdOnReplica = lastLog.commitId;
						this.lastConsolidatedChangeNumberOnReplica = lastLog.changeNumber;
						this.lastConsolidatedTxnChangeNumberOnReplica = lastLog.txnChangeNumber;
					}

					if ((currentInsertBatchCount > 0) || (currentUpdateBatchCount > 0 ) || (currentDeleteBatchCount > 0)) {				
						//Execute checkpoint UPDATE on dst if there was some log seen and commit the non empty txn
						if (lastLog != null) {
							updateDstCheckpointIfNeeded(dstExecutor, lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber, beforeValues, afterValues);
						}
						commitDstTran(dstExecutor);
						if (lastLog != null) {
							updateLocalCheckpointIfNeeded(lastLog.commitId, lastLog.changeNumber, lastLog.txnChangeNumber);
						}
						
						if (currentInsertBatchCount > 0) {
							device.tracer.info("Committed insert/upsert/replace batch of size : " + currentInsertBatchCount);
						}
						if (currentUpdateBatchCount > 0) {
							device.tracer.info("Committed update batch of size : " + currentUpdateBatchCount);
						}
						if (currentDeleteBatchCount > 0 ) {
							device.tracer.info("Committed delete batch of size : " + currentDeleteBatchCount);
						}
						++currentEventLogSegmentTxnCnt;
						++consolidatedTxnCount;
						if (lastLog != null) {
							this.lastConsolidatedCommitId = lastLog.commitId;
							this.lastConsolidatedChangeNumber = lastLog.changeNumber;
							this.lastConsolidatedTxnChangeNumber = lastLog.txnChangeNumber;
						}
					}
				}							
			} finally {
				if (replicaConn != null) {
					try { replicaConn.close(); } catch (Exception ignore) {}
				}
			}

			
			statsCollector.updateTableAndLogStatsForLogSegment(tableStats, currentEventLogSegment.sequenceNumber, currentEventLogSegmentTxnCnt, currentEventLogSegment.getSize());
			Monitor.getInstance().registerChangedDevice(device);

			//
			//We keep track of last applied cdc log segment in metadata also (apart from destination db that keeps checkpoint info)
			//to account for skipped/empty/all logs filtered out, kind of log segment and use this information upon restart 
			//and be able to resume from the most recent applied log segment
			//
			consolidatorMetadataMgr.updateLastConsolidatedCDCLogSegmentSeqNum(currentEventLogSegment.sequenceNumber);

			this.lastConsolidatedChangeNumber = -1;
			this.lastConsolidatedTxnChangeNumber = -1;
			this.lastConsolidatedChangeNumberOnReplica = -1;
			this.lastConsolidatedTxnChangeNumberOnReplica = -1;

			device.tracer.debug("Consolidated " + currentEventLogSegmentOperCnt + " operations from segment : " + this.currentEventLogSegment.path);
			return currentEventLogSegmentOperCnt;
		} catch (SyncLiteException | SQLException e) {
			throw new SyncLiteException("SyncLite event processor failed to process event log segment : " + currentEventLogSegment, e);
		}
	}

	private final void executeDDLOnReplica(Connection replicaConn, Table srcTable, OperType opType, String sql, DDLInfo ddlInfo) throws SQLException {
		//
		//If sql is REFRESH table then translate it to DROP TABLE followed by CREATE TABLE
		//
		if (opType == OperType.REFRESHTABLE) {
			String dropSql = "DROP TABLE IF EXISTS " + srcTable.id.table;
			try (Statement stmt = replicaConn.createStatement()) {
				stmt.execute(dropSql);
			}
			// Search for the word "refresh" in lowercase
	        int refreshIndex = sql.strip().toLowerCase().indexOf("refresh");
	        if (refreshIndex != -1) {
	            // Replace "refresh" with "create"
	            String createSql = sql.strip().substring(0, refreshIndex) + "CREATE" + sql.substring(refreshIndex + 7);
				try (Statement stmt = replicaConn.createStatement()) {
					stmt.execute(createSql);				}

	        }
		} else if (opType == OperType.ALTERCOLUMN) {
			//
			//SQLite does not support alter column
			//We are supporting this only for dblogger device as below 
			//1. Drop column (unlogged)
			//2. Create Column (unlogged)
			//
			String dropColumnSql = "ALTER TABLE " + ddlInfo.tableName + " DROP COLUMN " + ddlInfo.columnName;
			String addColumnSql = "ALTER TABLE " + ddlInfo.tableName + " ADD COLUMN " + ddlInfo.columnName +  " " + ddlInfo.colDef;
			try (Statement stmt = replicaConn.createStatement()) {
				stmt.execute(dropColumnSql);
				stmt.execute(addColumnSql);
			} catch (SQLException e) {
				//Following check to add idempotent behavior to ADD COLUMN and DROP COLUMN sqls
				if (e.getMessage().contains("duplicate column name") || e.getMessage().contains("no such column")) {
					return;
				}
				throw e;
			}			
		} else if (opType == OperType.PUBLISHCOLUMNLIST) {
			//
			//Ignore this operation on replica as it is only to reorder column sequence in synclite's Table object.
			//			
		} else {
			try {
				try (Statement stmt = replicaConn.createStatement()) {
					stmt.execute(sql);
				}
			} catch (SQLException e) {
				//Following check to add idempotent behavior to DDL sqls
				if (e.getMessage().contains("duplicate column name") || e.getMessage().contains("no such column") || e.getMessage().contains("already exists")) {
					return;
				}
				throw e;
			}
		}
	}

	private final void bindReplicaInsertPrepStmt(Table srcTable, PreparedStatement pStmt, List<Object> argValues, HashMap<String,Integer> colMap) throws SQLException {
		if (colMap == null) {
			int index = 1;
			for (Object o : argValues) {
				pStmt.setObject(index, o);
				++index;
			}
		} else {
			//
			//We have to use column ordering as specified in colMap
			//colMap is populated if INSERT was done with column list
			//
			int index = 1;
			for (Column col : srcTable.columns) {
				Object colVal = null;
				Integer colIdx = colMap.get(col.column);
				if (colIdx != null) {
					colVal = argValues.get(colIdx);
				}
				pStmt.setObject(index, colVal);
				++index;
			}
		}
		pStmt.addBatch();
	}

	private void bindAndExecuteReplicaCheckpointUpdatePrepStmt(PreparedStatement pstmt, long commitId, long changeNumber, long txnChangeNumber) throws SQLException {
		pstmt.clearBatch();
		pstmt.setLong(1, commitId);
		pstmt.setLong(2, changeNumber);
		pstmt.setLong(3, txnChangeNumber);
		if (this.currentEventLogSegment == null) {
			pstmt.setLong(4, 0);
		} else {
			pstmt.setLong(4, this.currentEventLogSegment.sequenceNumber);
		}
		pstmt.setLong(5, this.consolidatedTxnCount + 1); //+1 since this value will be incremented only after applying the txn on dst
		pstmt.execute();
	}

	private final String getReplicaInsertPrepStmt(Table srcTable, int size) {
		StringBuilder prepSql = new StringBuilder();
		boolean idempotentInsert = false;
		if (this.applyInsertIdempotently) {
			if (srcTable.hasPrimaryKey()) {
				idempotentInsert = true;
			}
		}
		StringBuilder insertColList = new StringBuilder();
		boolean first = true;
		for (Column c : srcTable.columns) {
			if (!first) {
				insertColList.append(",");
			}
			insertColList.append(c.column);
			first = false;
		}

		if (idempotentInsert) {
			prepSql.append("INSERT OR REPLACE INTO " + srcTable.id.table + "(" + insertColList.toString() + ")" + " VALUES(");
		} else {
			prepSql.append("INSERT INTO " + srcTable.id.table + "(" + insertColList.toString() + ")" + " VALUES(");
		}
		
		for (int i = 0; i < size; ++i) {
			if (i > 0) {
				prepSql.append(",");
			}
			prepSql.append("?");
		}
		prepSql.append(")");
		return prepSql.toString();
	}

	private final String getReplicaUpdatePrepStmt(Table srcTable) {
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ").append(srcTable.id.table).append(" SET ");
		boolean first = true;
		for (Column c : srcTable.columns) {
			if (!first) {
				sql.append(", ");
			}
			sql.append(c.column).append(" = ?");
			first = false;
		}
		sql.append(" WHERE ");
		boolean hasPK = srcTable.hasPrimaryKey();
		first = true;
		for (Column c : srcTable.columns) {
			if (hasPK && c.pkIndex <= 0) {
				continue;
			}
			if (!first) {
				sql.append(" AND ");
			}
			sql.append(c.column).append(" IS ?");
			first = false;
		}
		return sql.toString();
	}

	private final String getReplicaDeletePrepStmt(Table srcTable) {
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ").append(srcTable.id.table).append(" WHERE ");
		boolean hasPK = srcTable.hasPrimaryKey();
		boolean first = true;
		for (Column c : srcTable.columns) {
			if (hasPK && c.pkIndex <= 0) {
				continue;
			}
			if (!first) {
				sql.append(" AND ");
			}
			sql.append(c.column).append(" IS ?");
			first = false;
		}
		return sql.toString();
	}

	private final void bindReplicaUpdatePrepStmt(Table srcTable, java.sql.PreparedStatement pStmt, List<Object> argValues, long argCnt) throws SQLException {
		int numCols = (int)(argCnt / 2);
		int index = 1;
		//Bind SET clause with after-values
		for (int i = 0; i < numCols; i++) {
			pStmt.setObject(index, argValues.get(numCols + i));
			index++;
		}
		//Bind WHERE clause with before-values (PK columns only, or all if no PK)
		boolean hasPK = srcTable.hasPrimaryKey();
		for (int i = 0; i < numCols; i++) {
			Column col = srcTable.columns.get(i);
			if (hasPK && col.pkIndex <= 0) {
				continue;
			}
			pStmt.setObject(index, argValues.get(i));
			index++;
		}
	}

	private final void bindReplicaDeletePrepStmt(Table srcTable, java.sql.PreparedStatement pStmt, List<Object> argValues) throws SQLException {
		int index = 1;
		boolean hasPK = srcTable.hasPrimaryKey();
		for (int i = 0; i < argValues.size(); i++) {
			Column col = srcTable.columns.get(i);
			if (hasPK && col.pkIndex <= 0) {
				continue;
			}
			pStmt.setObject(index, argValues.get(i));
			index++;
		}
	}

	/*
	private final void executeDDLIgnoreException(String sql) {
		String url = "jdbc:sqlite:" + this.replicaPath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(sql);
			}
		} catch (SQLException e) {
			//Ignore as we might be executing this DDL more than once in case of a crash restart. 
		}        
	}
	 */

	@Override
	public long consolidateDevice() throws SyncLiteException {
		// TODO Auto-generated method stub
		return 0;
	}

}
