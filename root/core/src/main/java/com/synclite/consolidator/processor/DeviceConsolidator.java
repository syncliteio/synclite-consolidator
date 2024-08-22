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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.device.DeviceStatus;
import com.synclite.consolidator.exception.DstDuplicateKeyException;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.log.CDCLogPosition;
import com.synclite.consolidator.log.CDCLogSegment;
import com.synclite.consolidator.oper.BeginTran;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.CommitTran;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.DataType;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.schema.TableMapper;
import com.synclite.consolidator.schema.ValueMapper;
import com.synclite.consolidator.watchdog.Monitor;
public class DeviceConsolidator extends DeviceProcessor {

	//private static final String createLogTableSql = "CREATE TABLE IF NOT EXISTS cdclog(commit_id LONG, database_name TEXT, schema_name TEXT, table_name TEXT, op_type TEXT, sql TEXT, change_number LONG PRIMARY KEY);";
	//private static final String createLogValuesTableSql = "CREATE TABLE IF NOT EXISTS cdclog_values(change_number LONG, column_index LONG, before_value BLOB, after_value BLOB);";
	//private static final String createLogSchemasTableSql  = "CREATE TABLE IF NOT EXISTS cdclog_schemas(change_number LONG, database_name TEXT, schema_name TEXT, table_name TEXT, column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER, column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER)";

	private TableMapper userTableMapper;
	private TableMapper systemTableMapper;
	private CDCLogPosition restartCDCLogPosition;
	private CDCLogSegment currentCDCLogSegment;
	private long lastConsolidatedCommitId;
	private long lastConsolidatedChangeNumber;
	private long consolidatedTxnCount;
	private final ConsolidatorMetadataManager consolidatorMetadataMgr;
	private boolean applyInsertIdempotently;
	private boolean hasProcessedAllSegments = false;
	private DeviceDstInitializer dstInitializer;
	private DeviceStatsCollector statsCollector;
	private HashMap<TableID, HashMap<OperType, Long>> tableStats = new HashMap<TableID, HashMap<OperType, Long>>();
	private HashSet<TableID> currentTxnDstTables = new HashSet<TableID>();
	protected DeviceConsolidator(Device device, int dstIndex) throws SyncLiteException {
		super(device, dstIndex);
		this.userTableMapper = TableMapper.getUserTableMapperInstance(dstIndex);
		this.systemTableMapper = TableMapper.getSystemTableMapperInstance(dstIndex);
		this.restartCDCLogPosition = null;
		this.currentCDCLogSegment = null;
		this.lastConsolidatedCommitId = 0;
		this.lastConsolidatedChangeNumber = -1;
		this.consolidatedTxnCount = 0;
		this.consolidatorMetadataMgr = device.getConsolidatorMetadataMgr(this.dstIndex);
		this.statsCollector = device.getDeviceStatsCollector(this.dstIndex);
		this.dstInitializer = new DeviceDstInitializer(device, userTableMapper, systemTableMapper, statsCollector, dstIndex);		
		this.applyInsertIdempotently = ConfLoader.getInstance().getDstIdempotentDataIngestion(dstIndex);
		device.updateDeviceStatus(DeviceStatus.SYNCING, "");
	}
	
	private final void reloadCheckpointInfo() throws SyncLiteException {
		try {
			consolidatorMetadataMgr.loadSchemas(device);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to laod consolidation src tables from metadata file : ", e);
		}
		ConsolidatorSrcTable replicatorCheckpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex));
		replicatorCheckpointTable.setIsSystemTable();
		for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(this.dstIndex); ++i) {
			try {
				try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer) ) {
					this.restartCDCLogPosition = dstExecutor.readCDCLogPosition(device.getDeviceUUID(), device.getDeviceName(), systemTableMapper.mapTable(replicatorCheckpointTable));
					break;
				}
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
		if (this.restartCDCLogPosition != null) {
			//If we had recorded a higher log segment sequence number in our metadata 
			//(possibly due to skipped/all logs filtered out/empty log segments) then 
			//bump up restartCDCLogPosition
			//
			if (consolidatorMetadataMgr.getLastConsolidatedCDCLogSegmentSeqNum() > restartCDCLogPosition.logSegmentSequenceNumber) {
				this.restartCDCLogPosition.logSegmentSequenceNumber = consolidatorMetadataMgr.getLastConsolidatedCDCLogSegmentSeqNum();
			}
			this.currentCDCLogSegment = device.getCDCLogSegment(this.restartCDCLogPosition.logSegmentSequenceNumber);
			this.lastConsolidatedCommitId = this.restartCDCLogPosition.commitId;
			this.lastConsolidatedChangeNumber = this.restartCDCLogPosition.changeNumber;
			this.consolidatedTxnCount = this.restartCDCLogPosition.txnCount;
			device.updateLastConsolidatedCommitID(this.lastConsolidatedCommitId);
		}
	}

	@Override
	public final long syncDevice() throws SyncLiteException {
		long consolidatedOperCount = 0;
		if (consolidatorMetadataMgr.getInitializationStatus() == 1) {
			return doSync();
		} else if (device.getStatus() == DeviceStatus.SYNCING){
			this.dstInitializer.trySnapshotConsolidation(device.getDataBackupSnapshot());
		}
		return consolidatedOperCount;
	}

	@Override
	public boolean hasMoreWork() {
		return !hasProcessedAllSegments;
	}

	private final long doSync() throws SyncLiteException {
		long consolidatedOperCount = 0;
		if (this.restartCDCLogPosition == null) {
			reloadCheckpointInfo();
			if (this.restartCDCLogPosition == null) {
				return 0;
			}
		}
		if (this.currentCDCLogSegment == null) {
			this.currentCDCLogSegment = device.getCDCLogSegment(this.restartCDCLogPosition.logSegmentSequenceNumber);
			if (this.currentCDCLogSegment == null) {
				return 0;
			}
		}
		/*
        if (!currentCDCLogSegment.isApplied()) {
            //If next log segment has been created then we can be assured that this log segment
            //is replay-able.
            CDCLogSegment nextCDCLogSegment = device.getNextCDCLogSegment(currentCDCLogSegment);
            if (nextCDCLogSegment != null) {
                device.tracer.info("Started consolidation of cdc log segment : " + currentCDCLogSegment);
                for (long i = 0; i < PropsLoader.getInstance().getOperRetryCount(); ++i) {
                    try {
                        importedOperCount = doApply();
                        device.updateLastImportedCommitID(this.lastImportedCommitId);
                        break;
                    } catch (DstExecutionException e) {
                        if (i == (PropsLoader.getInstance().getOperRetryCount()-1)) {
                            throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
                        }
                        try {
                            Thread.sleep(PropsLoader.getInstance().getOperRetryIntervalMs());
                        } catch (InterruptedException e1) {
                            Thread.interrupted();
                        }
                        device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
                    }
                }
                device.tracer.info("Finished consolidation of cdc log segment : " + currentCDCLogSegment);
                currentCDCLogSegment = nextCDCLogSegment;
                device.purgeCDCLogSegmentsUpto(currentCDCLogSegment);
            }
        }
		 */

		boolean lookForNextLogSegment = false;
		if (!currentCDCLogSegment.isApplied()) {
			if (currentCDCLogSegment.isReadyToApply()) {
				device.tracer.info("Started consolidating cdc log segment : " + currentCDCLogSegment + " on dst : " + dstIndex);
				for (long i = 0; i < ConfLoader.getInstance().getDstOperRetryCount(dstIndex); ++i) {
					try {
						consolidatedOperCount = doApply();
						device.updateLastConsolidatedCommitID(this.lastConsolidatedCommitId);
						break;					
					} catch (SyncLiteException e) {
						if (e instanceof DstDuplicateKeyException) {
							this.applyInsertIdempotently = true;
							device.tracer.info("Insert failed with duplicate key exception, retrying transaction with insert converted to delete+insert");
						}
						if (i == (ConfLoader.getInstance().getDstOperRetryCount(dstIndex)-1)) {
							Boolean skipFailedLogSegments = ConfLoader.getInstance().getDstSkipFailedLogFiles(dstIndex);
							if (!skipFailedLogSegments) {
								throw new SyncLiteException("Dst txn failed after all retry attempts : ", e);
							} else {
								skipLogSegment(currentCDCLogSegment);
								break;
							}
						}
						try {
							Thread.sleep(ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex));
						} catch (InterruptedException e1) {
							Thread.interrupted();
						}
						device.tracer.info("Retry attempt : " + (i + 2)  + " : Retrying transaction after an exception from dst :" + e);
					}
				}
				device.tracer.info("Finished consolidating cdc log segment : " + currentCDCLogSegment + " on dst : " + dstIndex);
				lookForNextLogSegment = true;
			} else {
				//current segment is not ready to apply yet
				lookForNextLogSegment = false;
				hasProcessedAllSegments = false;
			}
		} else {
			lookForNextLogSegment = true;
		}

		if (lookForNextLogSegment) {
			CDCLogSegment nextCDCLogSegment = device.getNextCDCLogSegmentToProcess(currentCDCLogSegment);
			if (nextCDCLogSegment != null) {
				currentCDCLogSegment = nextCDCLogSegment;
				hasProcessedAllSegments = false;
			} else {
				hasProcessedAllSegments = true;
			}
		}
		return consolidatedOperCount;
	}

	private final void skipLogSegment(CDCLogSegment currentCDCLogSegment) {
		try {
			Path failedLogDir = this.device.getFailedLogSegmentsDirectoryPath(this.dstIndex);
			Path targetPath = failedLogDir.resolve(currentCDCLogSegment.path.getFileName());
			Files.copy(currentCDCLogSegment.path, targetPath, StandardCopyOption.REPLACE_EXISTING);

			consolidatorMetadataMgr.updateLastConsolidatedCDCLogSegmentSeqNum(currentCDCLogSegment.sequenceNumber);
			
			this.lastConsolidatedChangeNumber = -1;
			device.tracer.info("Skipped failed log segment : " + currentCDCLogSegment);
		} catch(Exception e) {
			device.tracer.error("Failed to skip and mark failed log segment : " + currentCDCLogSegment, e);
		}
	}

	private long doApply() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.currentCDCLogSegment.path;
		long currentCDCLogSegmentTxnCnt = 0;
		long currentCDCLogSegmentOperCnt = 0;
		TableMapper tableMapper = userTableMapper;
		ValueMapper valueMapper = tableMapper.getValueMapper();
		HashMap<TableID, Insert> tblMappedInserts = new HashMap<TableID, Insert>();
		HashMap<TableID, Update> tblMappedUpdates = new HashMap<TableID, Update>();
		HashMap<TableID, Delete> tblMappedDeletes = new HashMap<TableID, Delete>();

		tableStats.clear();
		try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, this.dstIndex, device.tracer)) {
			try (Connection conn = DriverManager.getConnection(url)) {
				try (PreparedStatement cdcLogReaderPstmt = conn.prepareStatement(currentCDCLogSegment.getLogReaderSql(conn))) {
					try (PreparedStatement cdcLogSchemaReaderPstmt = conn.prepareStatement(CDCLogSegment.cdcLogSchemaReaderSql)) {
						//cdcLogReaderPstmt.setLong(1, this.lastConsolidatedCommitId);
						//cdcLogReaderPstmt.setLong(2, this.lastConsolidatedChangeNumber);
						try (ResultSet rs = cdcLogReaderPstmt.executeQuery()) {
							List<Object> afterValues = new ArrayList<Object>(); 
							List<Object> beforeValues = new ArrayList<Object>();

							TableID prevTableId = null;
							while (rs.next()) {
								//"SELECT commit_id, database_name, schema_name, table_name, op_type, change_number FROM cdclog WHERE commit_id > ?";
								long commitId = rs.getLong(1);
								String database = rs.getString(2);
								String schema = rs.getString(3);
								String table = rs.getString(4);
								if (!ConfLoader.getInstance().isAllowedTable(dstIndex, table)) {
									continue;
								}
								OperType opType = OperType.valueOf(rs.getString(5));
								long changeNumber = rs.getLong(6);
								long argCnt = rs.getLong(7);

								ConsolidatorSrcTable srcTable = null;
								TableID tableId =  null;
								if (table != null) {
									//Avoid TableID lookup if same table logs
									if ((prevTableId != null) && (prevTableId.table.equals(table)) &&(prevTableId.database.equals(database)))  {
										tableId = prevTableId;
									} else {
										tableId =  TableID.from(device.getDeviceUUID(), device.getDeviceName(), 1, database, schema, table);
									}

									srcTable = ConsolidatorSrcTable.from(tableId);                                                                                
									
									tableMapper = srcTable.getIsSystemTable() ? systemTableMapper : userTableMapper;
									valueMapper = tableMapper.getValueMapper();
									HashMap<OperType, Long> opStats = tableStats.get(tableId);										
									if (opStats == null) {
										opStats = new HashMap<OperType, Long>();
										tableStats.put(tableId, opStats);
									}										
									Long opCnt = opStats.get(opType);
									if (opCnt == null) {
										opStats.put(opType, 1L);
									} else {
										opStats.put(opType, opCnt + 1L);
									}										
								} 

								if (commitId < this.lastConsolidatedCommitId) {
									continue;
								} else {
									if (commitId == this.lastConsolidatedCommitId) {
										if (changeNumber <= this.lastConsolidatedChangeNumber) {
											continue;
										}
									}
								}

								//Fetch logValues for IUD
								//"SELECT column_index, before_value, after_value WHERE change_number = ? ORDER BY column_index";
								switch (opType) {
								case INSERT :
									//
									//We do column filtering and value mapping here itself to avoid an additional loop over all arguments
									//
									for(int i = 1; i <= argCnt; ++i) {
										Column col = srcTable.columns.get(i-1);
										if (!ConfLoader.getInstance().isAllowedColumn(dstIndex, table, col.column)) {
											continue;
										}
										Object colVal = rs.getObject(CDCLogSegment.getArgColPrefix() + i);
										colVal = valueMapper.mapValue(tableId, col, colVal);
										afterValues.add(colVal);
									}									
									Insert mappedInsert = tblMappedInserts.get(srcTable.id);
									if (mappedInsert == null) {
										Insert srcInsert = new Insert(srcTable, afterValues, applyInsertIdempotently);
										mappedInsert = (Insert) (tableMapper.mapOper(srcInsert).get(0));
										tblMappedInserts.put(srcTable.id, mappedInsert);
									} else {
										tableMapper.bindMappedInsert(mappedInsert, afterValues);
									}
									dstExecutor.execute(mappedInsert);
									currentTxnDstTables.add(mappedInsert.tbl.id);
									afterValues.clear();
									++currentCDCLogSegmentOperCnt;
									break;
								case UPDATE:
									for(int i = 1; i <= argCnt/2 ; ++i) {
										Column col = srcTable.columns.get(i-1);
										if (!ConfLoader.getInstance().isAllowedColumn(dstIndex, table, col.column)) {
											continue;
										}
										Object colBeforeVal = rs.getObject(CDCLogSegment.getArgColPrefix() + i);										
										colBeforeVal = valueMapper.mapValue(tableId, col, colBeforeVal);
										beforeValues.add(colBeforeVal);

										Object colAfterVal = rs.getObject(CDCLogSegment.getArgColPrefix() + ((argCnt/2) + i));									
										colAfterVal = valueMapper.mapValue(tableId, col, colAfterVal);
										afterValues.add(colAfterVal);
									}									
									Update mappedUpdate = tblMappedUpdates.get(srcTable.id);
									if (mappedUpdate == null) {
										Update srcUpdate = new Update(srcTable, beforeValues, afterValues);
										mappedUpdate = (Update) (tableMapper.mapOper(srcUpdate).get(0));
										tblMappedUpdates.put(srcTable.id, mappedUpdate);
									} else {
										tableMapper.bindMappedUpdate(mappedUpdate, beforeValues, afterValues);
									}
									dstExecutor.execute(mappedUpdate);
									currentTxnDstTables.add(mappedUpdate.tbl.id);

									beforeValues.clear();
									afterValues.clear();
									++currentCDCLogSegmentOperCnt;
									break;
								case DELETE:
									for(int i = 1; i <= argCnt; ++i) {
										Column col = srcTable.columns.get(i-1);
										if (!ConfLoader.getInstance().isAllowedColumn(dstIndex, table, col.column)) {
											continue;
										}
										Object colVal = rs.getObject(CDCLogSegment.getArgColPrefix() + i);										
										colVal = valueMapper.mapValue(tableId, col, colVal);
										beforeValues.add(colVal);
									}
									
									Delete mappedDelete = tblMappedDeletes.get(srcTable.id);
									if (mappedDelete == null) {
										Delete srcDelete = new Delete(srcTable, beforeValues);
										mappedDelete = (Delete) (tableMapper.mapOper(srcDelete).get(0));
										tblMappedDeletes.put(srcTable.id, mappedDelete);
									} else {
										tableMapper.bindMappedDelete(mappedDelete, beforeValues);
									}
									dstExecutor.execute(mappedDelete);
									currentTxnDstTables.add(mappedDelete.tbl.id);

									beforeValues.clear();
									++currentCDCLogSegmentOperCnt;
									break;
								case BEGINTRAN:
									dstExecutor.execute(tableMapper.mapOper(new BeginTran()));
									device.tracer.debug("Consolidating Txn with CommitID : " + commitId);
									break;
								case COMMITTRAN:
									//Execute triggers if specified.
									//
									//Check if trigger statements are set on tables involved in this transaction
									//If yes then execute
									//		
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
									dstExecutor.execute(tableMapper.mapOper(new CommitTran()));									
									this.currentTxnDstTables.clear();									
									device.tracer.debug("Consolidated Txn with CommitID : " + commitId);
									this.lastConsolidatedCommitId = commitId;
									this.lastConsolidatedChangeNumber = changeNumber;
									//Turn off insert as delete+insert for next transaction. 
									resetApplyInsertIdempotently();
									++currentCDCLogSegmentTxnCnt;
									break;
								case CHECKPOINTTRAN:
									dstExecutor.execute(tableMapper.mapOper(new CommitTran()));
									device.tracer.debug("Partially committed Txn with CommitID : " + commitId + " upto change number : " + changeNumber);
									this.lastConsolidatedChangeNumber = changeNumber;
									break;
								case CREATETABLE:
									//SELECT column_index, column_name, column_type, column_not_null,
									//column_default_value, column_primary_key, column_auto_increment
									List<Column> newTableCols = new ArrayList<Column>();
									cdcLogSchemaReaderPstmt.setLong(1, changeNumber);
									try (ResultSet rsSchema= cdcLogSchemaReaderPstmt.executeQuery()) {
										while(rsSchema.next()) {
											long cid = rsSchema.getLong(1);
											String columnName = rsSchema.getString(2);
											String columnType= rsSchema.getString(3);
											int isNotNull = rsSchema.getInt(4);
											String defaultValue = rsSchema.getString(5);
											int isPrimaryKey = rsSchema.getInt(6);
											int isAutoIncrement = rsSchema.getInt(7);
											DataType dataType = new DataType(columnType, device.schemaReader.getJavaSqlType(columnType), device.schemaReader.getStorageClass(columnType));
											Column c = new Column(cid, columnName , dataType, isNotNull, defaultValue, isPrimaryKey, isAutoIncrement);
											newTableCols.add(c);
										}
										Oper createTableOper= srcTable.generateCreateTableOper(tableMapper, newTableCols);
										if (createTableOper != null) {
											dstExecutor.execute(createTableOper.map(tableMapper));
											try {
												consolidatorMetadataMgr.upsertSchema(srcTable);
											} catch (SQLException e) {
												throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
											}
										}
										++currentCDCLogSegmentOperCnt;
									}
									break;
								case DROPTABLE:
									Oper dropTableOper = srcTable.generateDropTableOper(tableMapper);
									if (dropTableOper != null) {
										dstExecutor.execute(dropTableOper.map(tableMapper));
										try {
											consolidatorMetadataMgr.deleteSchema(srcTable);
										} catch (SQLException e) {
											throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
										}
									}
									++currentCDCLogSegmentOperCnt;
									break;
								case ADDCOLUMN:
									//SELECT column_index, column_name, column_type, column_not_null,
									//column_default_value, column_primary_key, column_auto_increment
									newTableCols = new ArrayList<Column>();
									cdcLogSchemaReaderPstmt.setLong(1, changeNumber);
									try (ResultSet rsSchema= cdcLogSchemaReaderPstmt.executeQuery()) {
										while(rsSchema.next()) {
											long cid = rsSchema.getLong(1);
											String columnName = rsSchema.getString(2);
											String columnType= rsSchema.getString(3);
											int isNotNull = rsSchema.getInt(4);
											String defaultValue = rsSchema.getString(5);
											int isPrimaryKey = rsSchema.getInt(6);
											int isAutoIncrement = rsSchema.getInt(7);
											DataType dataType = new DataType(columnType, device.schemaReader.getJavaSqlType(columnType), device.schemaReader.getStorageClass(columnType));
											Column c = new Column(cid, columnName , dataType, isNotNull, defaultValue, isPrimaryKey, isAutoIncrement);
											newTableCols.add(c);
										}
										Oper addColOper= srcTable.generateAddColumnOper(newTableCols);
										if (addColOper != null) {
											dstExecutor.execute(addColOper.map(tableMapper));
											try {
												consolidatorMetadataMgr.upsertSchema(srcTable);
											} catch (SQLException e) {
												throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
											}
										}
										++currentCDCLogSegmentOperCnt;
									}
									break;									
								case DROPCOLUMN:
									//SELECT column_index, column_name, column_type, column_not_null,
									//column_default_value, column_primary_key, column_auto_increment
									newTableCols = new ArrayList<Column>();
									cdcLogSchemaReaderPstmt.setLong(1, changeNumber);
									try (ResultSet rsSchema= cdcLogSchemaReaderPstmt.executeQuery()) {
										while(rsSchema.next()) {
											long cid = rsSchema.getLong(1);
											String columnName = rsSchema.getString(2);
											String columnType= rsSchema.getString(3);
											int isNotNull = rsSchema.getInt(4);
											String defaultValue = rsSchema.getString(5);
											int isPrimaryKey = rsSchema.getInt(6);
											int isAutoIncrement = rsSchema.getInt(7);
											DataType dataType = new DataType(columnType, device.schemaReader.getJavaSqlType(columnType), device.schemaReader.getStorageClass(columnType));
											Column c = new Column(cid, columnName , dataType, isNotNull, defaultValue, isPrimaryKey, isAutoIncrement);
											newTableCols.add(c);
										}
										Oper dropColOper= srcTable.generateDropColumnOper(newTableCols);
										if (dropColOper != null) {
											dstExecutor.execute(dropColOper.map(tableMapper));
											try {
												consolidatorMetadataMgr.upsertSchema(srcTable);
											} catch (SQLException e) {
												throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
											}
										}
										++currentCDCLogSegmentOperCnt;
									}
									break;
								case RENAMECOLUMN:
									//SELECT column_index, column_name, column_type, column_not_null,
									//column_default_value, column_primary_key, column_auto_increment
									cdcLogSchemaReaderPstmt.setLong(1, changeNumber);
									try (ResultSet rsSchema= cdcLogSchemaReaderPstmt.executeQuery()) {
										while(rsSchema.next()) {
											String newColumnName = rsSchema.getString(2);
											String oldColumnName = rsSchema.getString(9);
											if (oldColumnName != null) {
												Oper renameColOper= srcTable.generateRenameColumnOper(oldColumnName, newColumnName);
												if (renameColOper != null) {
													dstExecutor.execute(renameColOper.map(tableMapper));
													try {
														consolidatorMetadataMgr.upsertSchema(srcTable);
													} catch (SQLException e) {
														throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
													}
												}
												break;
											}
										}
										++currentCDCLogSegmentOperCnt;
									}
									break;
								case RENAMETABLE:
									cdcLogSchemaReaderPstmt.setLong(1, changeNumber);
									try (ResultSet rsSchema= cdcLogSchemaReaderPstmt.executeQuery()) {
										if(rsSchema.next()) {
											String oldTableName = rsSchema.getString(8);
											tableId =  TableID.from(device.getDeviceUUID(), device.getDeviceName(), 1, database, schema, oldTableName);
											srcTable = ConsolidatorSrcTable.from(tableId);
											Oper renameTableOper = srcTable.generateRenameTableOper(tableMapper, oldTableName, table);
											if (renameTableOper != null) {
												dstExecutor.execute(renameTableOper.map(tableMapper));
												try {
													consolidatorMetadataMgr.upsertSchema(srcTable);
												} catch (SQLException e) {
													throw new SyncLiteException("Failed to persist schema for table : " + srcTable.id + " in consolidator metadata file : ", e);
												}
											}
										}
									}
									break;
								default:
									throw new SyncLiteException("Unsupported operation type detected : " + opType + " for table " + tableId);
								}									
							}
						}
					}
				}

				this.consolidatedTxnCount += currentCDCLogSegmentTxnCnt;
				//Monitor.getInstance().incrTotalCDCLogSegmentCnt(1);
				statsCollector.updateTableAndLogStatsForLogSegment(tableStats, currentCDCLogSegment.sequenceNumber, currentCDCLogSegmentTxnCnt, currentCDCLogSegment.getSize());
				Monitor.getInstance().registerChangedDevice(device);

				//
				//We keep track of last applied cdc log segment in metadata also (apart from destination db that keeps checkpoint info)
				//to account for skipped/empty/all logs filtered out, kind of log segment and use this information upon restart 
				//and be able to resume from the most recent applied log segment
				//
				consolidatorMetadataMgr.updateLastConsolidatedCDCLogSegmentSeqNum(currentCDCLogSegment.sequenceNumber);

				this.lastConsolidatedChangeNumber = -1;
				device.tracer.debug("Consolidated " + currentCDCLogSegmentOperCnt + " operations from segment : " + this.currentCDCLogSegment.path);
			}
		} catch (SQLException | SyncLiteException e) {
			throw new SyncLiteException("SyncLite Consolidator failed to consolidate log segment : " + this.currentCDCLogSegment, e);
		}
		return currentCDCLogSegmentOperCnt;
	}

	private final void resetApplyInsertIdempotently() {
		if (! ConfLoader.getInstance().getDstIdempotentDataIngestion(dstIndex)) {
			this.applyInsertIdempotently = false;
		}
	}

	@Override
	public long consolidateDevice() throws SyncLiteException {
		long consolidatedOperCount = 0;
		if ((consolidatorMetadataMgr.getInitializationStatus() == 0) && (device.getStatus() == DeviceStatus.SYNCING)){
			this.dstInitializer.trySnapshotConsolidation(device.getDataBackupSnapshot());
		}
		return consolidatedOperCount;
	}

	private final void resetSchemas() throws SyncLiteException {
		try {
			consolidatorMetadataMgr.resetTableMetadata();
			consolidatorMetadataMgr.resetSchemas();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to reset schema and metadata tables in consolidator metadata file : " + consolidatorMetadataMgr.getMetadataFilePath());
		}
		ConsolidatorSrcTable.resetAll();
		ConsolidatorDstTable.resetAll();
		userTableMapper.resetAll();
		systemTableMapper.resetAll();
	}
	
}
