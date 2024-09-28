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

package com.synclite.consolidator.device;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.DevicePatternType;
import com.synclite.consolidator.global.MetadataManager;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.global.SyncLiteReplicatorInfo;
import com.synclite.consolidator.log.CDCLogSegment;
import com.synclite.consolidator.log.CommandLogSegment;
import com.synclite.consolidator.log.EventLogSegment;
import com.synclite.consolidator.nativedb.DB;
import com.synclite.consolidator.nativedb.PreparedStatement;
import com.synclite.consolidator.processor.DeviceStatsCollector;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.DataType;
import com.synclite.consolidator.schema.ReplicatorTable;
import com.synclite.consolidator.schema.StorageClass;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.stage.SyncLiteObjectType;
import com.synclite.consolidator.watchdog.Monitor;

public class Device {

	public class SchemaReader {
		private SchemaReader() {
		}

		public static final String tableInfoReaderSql = "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'table'";
		public void fetchTableSchema(Path dbPath, TableID id) throws SyncLiteException {
			ReplicatorTable tbl = ReplicatorTable.from(id);
			String url = "jdbc:sqlite:" + dbPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery(tableInfoReaderSql + " AND tbl_name = '" + id.table + "'")) {
						if (rs.next()) {
							String sql = rs.getString(2);
							tbl.setSql(sql);
							//tracer.debug(tbl.id + " : Read sql : " + sql);
						}
						fetchAndAddColumns(dbPath, tbl);
					}
				}
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to fetch schema for table : " + id, e);
			}
		}

		public void fetchTableSchema(DB dbConn, TableID id) throws SyncLiteException {
			ReplicatorTable tbl = ReplicatorTable.from(id);
			try (PreparedStatement nativePstmt = dbConn.prepare(tableInfoReaderSql + " AND tbl_name = '" + id.table + "'")) {
				int next = nativePstmt.stepQuery();
				if (next != 0) {
					String sql = nativePstmt.getString(1);
					tbl.setSql(sql);
					//tracer.debug(tbl.id + " : Read sql : " + sql);
				}
				fetchAndAddColumns(dbConn, tbl);
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to fetch schema for table : " + id, e);
			}
		}

		public List<ConsolidatorSrcTable> fetchConsolidatorSrcTables(Path dbPath, int dstIndex) throws SyncLiteException {
			List<ConsolidatorSrcTable> tables = new ArrayList<ConsolidatorSrcTable>();
			String url = "jdbc:sqlite:" + dbPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					//Just handle main database for now
					try (ResultSet rs = stmt.executeQuery(tableInfoReaderSql)) {
						while(rs.next()) {
							String tableName = rs.getString(1);
							String sql = rs.getString(2);
							ConsolidatorSrcTable tbl = ConsolidatorSrcTable.from(TableID.from(Device.this.uuid, Device.this.deviceName, dstIndex, "main", null, tableName));
							tbl.clearColumns();
							tbl.setSql(sql);
							fetchAndAddColumns(dbPath, tbl);
							tables.add(tbl);
						}
					}
				}
			} catch(Exception e) {
				throw new SyncLiteException("Schema collection failed : Failed to collect schema information from database : " +  dbPath + " with exception : ", e);
			}
			return tables;
		}

		public List<ReplicatorTable> fetchReplicatorTables(Path dbPath) throws SyncLiteException {
			List<ReplicatorTable> tables = new ArrayList<ReplicatorTable>();
			String url = "jdbc:sqlite:" + dbPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					//Just handle main database for now
					try (ResultSet rs = stmt.executeQuery(tableInfoReaderSql)) {
						while(rs.next()) {
							String tableName = rs.getString(1);
							String sql = rs.getString(2);
							ReplicatorTable tbl = ReplicatorTable.from(TableID.from(Device.this.uuid, Device.this.deviceName, 1, "main", null, tableName));
							tbl.clearColumns();
							tbl.setSql(sql);
							fetchAndAddColumns(dbPath, tbl);
							tables.add(tbl);
						}
					}
				}
			} catch(Exception e) {
				throw new SyncLiteException("Schema collection failed : Failed to collect schema information from database : " +  dbPath + " with exception : ", e);
			}
			return tables;
		}

		public List<Column> fetchColumns(Path dbPath, TableID tblID) throws SyncLiteException {
			String url = "jdbc:sqlite:" + dbPath;
			List<Column> cols = new ArrayList<Column>();
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt.executeQuery("pragma table_info(" + tblID.table + ")")) {
						while (rs.next()) {
							long cid = rs.getLong("cid");
							String colName = rs.getString("name").toLowerCase();
							String colType = rs.getString("type");
							int isNotNull = rs.getInt("notnull");
							String defaultValue = rs.getString("dflt_value");
							int pkIndex = rs.getInt("pk");
							//populate autoIncr later
							DataType dataType = new DataType(colType, schemaReader.getJavaSqlType(colType), schemaReader.getStorageClass(colType));
							Column c = new Column(cid, colName, dataType, isNotNull, defaultValue, pkIndex, 0);
							cols.add(c);
						}
					}
				}
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to fetch schema for table : " + tblID, e);
			}    
			return cols;
		}

		public void fetchAndAddColumns(Path dbPath, Table tbl) throws SyncLiteException {
			for (Column col : fetchColumns(dbPath, tbl.id)) {
				tbl.addColumn(col);
			}
		}

		public void fetchAndAddColumns(DB nativeDBConn, Table tbl) throws SyncLiteException {
			try (PreparedStatement nativePstmt = nativeDBConn.prepare("pragma table_info(" + tbl.id.table + ")");){
				int next = nativePstmt.stepQuery();
				while (next != 0) {
					long cid = nativePstmt.getLong(0);
					String colName = nativePstmt.getString(1).toLowerCase();
					String colType = nativePstmt.getString(2);
					int isNotNull = nativePstmt.getInt(3);
					String defaultValue = nativePstmt.getString(4);
					int pkIndex = nativePstmt.getInt(5);
					//populate autoIncr later
					DataType dataType = new DataType(colType, schemaReader.getJavaSqlType(colType), schemaReader.getStorageClass(colType));
					Column c = new Column(cid, colName, dataType, isNotNull, defaultValue, pkIndex, 0);
					tbl.addColumn(c);
					//tracer.debug(tbl.id + " : Read Column : " + c);
					next = nativePstmt.stepQuery();
				}
			} catch (SQLException e) {
				throw new SyncLiteException("Failed to fetch schema for table : " + tbl.id, e);
			}
		}

		public final void removeReplicatorTable(TableID id) throws SyncLiteException {
			ReplicatorTable.remove(id);
		}

		public final ReplicatorTable getReplicatorTableWithRefreshedSchema(TableID id) throws SyncLiteException {
			ReplicatorTable tbl = ReplicatorTable.from(id);
			tbl.clearColumns();
			fetchTableSchema(getReplica(1), tbl.id);
			return tbl;
		}

		public final ReplicatorTable getReplicatorTableWithRefreshedSchema(DB dbConn, TableID id) throws SyncLiteException {
			ReplicatorTable tbl = ReplicatorTable.from(id);
			tbl.clearColumns();
			fetchTableSchema(dbConn, tbl.id);
			return tbl;
		}

		public String getSelectSql(Path dbPath, ConsolidatorSrcTable srcTable) {
			StringBuilder builder = new StringBuilder();
			builder.append("SELECT ");
			boolean first = true;
			for (Column c : srcTable.columns) {
				if (first) {
					builder.append(c.column);
					first = false;
				} else {
					builder.append(",");
					builder.append(c.column);
				}
			}
			builder.append(" FROM ");
			builder.append(srcTable.id.database);
			builder.append(".");
			builder.append(srcTable.id.table);

			return builder.toString();
		}

		public StorageClass getStorageClass(String type) {
			String typeToCheck = type.toLowerCase().trim().split("[ (]")[0];

			switch(typeToCheck) {
			case "smallserial" :
			case "serial" :
			case "bigserial" :	
			case "bit" :
			case "integer" :
			case "int" :
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "bigint":        	
			case "int2":
			case "int4":	
			case "int8":
			case "long":
			case "byteint":	
			case "unsigned":
				return StorageClass.INTEGER;
			case "text":
			case "varchar":
			case "varchar2":	
			case "nvarchar":
			case "char":
			case "nchar":
			case "native":        	
			case "character":
			case "varying":	
			case "array":
			case "integer[]":
			case "bigint[]":
			case "text[]":
			case "boolean[]":
			case "float[]":
			case "numeric[]":
			case "timestamp[]":
			case "date[]":
			case "time[]":
			case "character[]":
			case "json[]":
			case "jsonb[]":
			case "vector":	
				return StorageClass.TEXT;
			case "clob":
			case "dbclob":	
				return StorageClass.CLOB;
			case "blob":
			case "bytea":	
			case "binary":
			case "varbinary":
			case "image":
			case "object":
			case "geography" :
			case "geometry" :
	        case "raw" :	
	        case "sdo_geometry" :
	        case "sdo_topo_geometry" :	
	        case "bfile" :	
	        case "ref" :
	        case "ordicom" :
	        case "ordaudio" :
	        case "ordvideo" :
	        case "orddoc" :
	        case "table" :	
	        case "associative":
	        case "varray":
	        case "graphic":
	        case "vargraphic":				
				return StorageClass.BLOB;
			case "real":
			case "double":
			case "float":
			case "numeric":
			case "money":
			case "smallmoney":
			case "number":
			case "decimal":	
				return StorageClass.REAL;
			case "date":
				return StorageClass.DATE;
			case "datetime":
			case "datetime2":	
			case "timestamp":
				return StorageClass.TIMESTAMP;
			case "time":
				return StorageClass.TIME;
			}

			return StorageClass.TEXT;
		}

		public JDBCType getJavaSqlType(String type) {
			String typeToCheck = type.toLowerCase().trim().split("[ (]")[0];

			switch(typeToCheck) {
			case "smallserial" :
			case "serial" :
			case "bigserial" :	
			case "bit" :
			case "integer" :
			case "int" :
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "bigint":        	
			case "int2":
			case "int4":	
			case "int8":
			case "long":
			case "byteint":	
			case "unsigned":
				return JDBCType.BIGINT;
			case "boolean":	
			case "bool":
				return JDBCType.BOOLEAN;
			case "text":
			case "varchar":
			case "varchar2":	
			case "nvarchar":
			case "char":
			case "nchar":
			case "native":        	
			case "character":
			case "varying":	
				return JDBCType.VARCHAR;
			case "clob":
			case "dbclob":	
				return JDBCType.CLOB;
			case "blob":
			case "bytea":	
			case "binary":
			case "varbinary":
			case "image":
			case "object":
			case "geography" :
			case "geometry" :
	        case "raw" :	
	        case "sdo_geometry" :
	        case "sdo_topo_geometry" :	
	        case "bfile" :	
	        case "ref" :
	        case "ordicom" :
	        case "ordaudio" :
	        case "ordvideo" :
	        case "orddoc" :
	        case "table" :	
	        case "associative":
	        case "varray":
	        case "graphic":
	        case "vargraphic":		
				return JDBCType.BLOB;
			case "real":
			case "double":
			case "float":
			case "numeric":
			case "money":
			case "smallmoney":
			case "number":
			case "decimal":	
				return JDBCType.DOUBLE;
			case "date":
				return JDBCType.DATE;
			case "datetime":
			case "datetime2":
			case "timestamp":
				return JDBCType.TIMESTAMP;
			case "time":	
				return JDBCType.TIME;
			
			case "array":
			case "integer[]":
			case "bigint[]":
			case "text[]":
			case "boolean[]":
			case "float[]":
			case "numeric[]":
			case "timestamp[]":
			case "date[]":
			case "time[]":
			case "character[]":
			case "json[]":
			case "jsonb[]":
			case "vector":	
				return JDBCType.ARRAY;						
			}


			return JDBCType.VARCHAR;
		}
		/*
		public JDBCType getJavaSqlType(String type) {
			String typeToCheck = type.toLowerCase().trim();

			switch(typeToCheck) {
			case "integer" :
				return JDBCType.INTEGER;
			case "int" :
				return JDBCType.INTEGER;
			case "tinyint":
				return JDBCType.TINYINT;
			case "smallint":
				return JDBCType.SMALLINT;
			case "mediumint":
				return JDBCType.INTEGER;
			case "bigint":
				return JDBCType.BIGINT;
			case "unsigned bigint":
				return JDBCType.NUMERIC;
			case "int2":
				return JDBCType.SMALLINT;
			case "int4":
				return JDBCType.INTEGER;
			case "int8":
				return JDBCType.BIGINT;
			case "text":
				return JDBCType.VARCHAR;
			case "clob":
				return JDBCType.CLOB;
			case "blob":
				return JDBCType.BLOB;
			case "real":
				return JDBCType.REAL;
			case "double":
				return JDBCType.DOUBLE;
			case "double precision":
				return JDBCType.DOUBLE;
			case "float":
				return JDBCType.FLOAT;
			case "numeric":
				return JDBCType.NUMERIC;
			case "boolean":
				return JDBCType.BOOLEAN;
			case "date":
				return JDBCType.VARCHAR;
			case "datetime":
				return JDBCType.VARCHAR;
			}

			if (typeToCheck.startsWith("decimal")) {
				return JDBCType.DECIMAL;
			}
			return JDBCType.VARCHAR;
		}
		 */
	}


	private static ConcurrentHashMap<Path, Device> devices = new ConcurrentHashMap<Path, Device>();
	private static ConcurrentHashMap<String, Device> nameToDeviceMap = new ConcurrentHashMap<String, Device>();
	private static ConcurrentHashMap<String, Device> idToDeviceMap = new ConcurrentHashMap<String, Device>();
	private String uuid;
	private Path rootPath;
	private Path uploadPath;
	private Path localCommandPath;
	private Path remoteCommandPath;
	private HashMap<Integer, Path> failedLogSegmentsDirectoryPaths = new HashMap<Integer, Path>();
	private String dbName;
	private long dbID;
	private String deviceName;
	private DeviceType deviceType;
	private int allowsConcurrentWriters;
	private DeviceStatus status;
	private String statusDescription;
	private String detectionTime;
	private String registeredTime;
	private String lastStatusUpdateTime;
	private long lastHeartbeatTS;
	private MetadataManager loggerMetadataMgr;
	private MetadataManager replicatorMetadataMgr;
	private HashMap<Integer, ConsolidatorMetadataManager> consolidatorMetadataMgrs = new HashMap<Integer, ConsolidatorMetadataManager>();
	private volatile long lastReplicatedCommitID;
	private volatile long lastConsolidatedCommitID;
	private volatile long logsToProcessSince;
	public SchemaReader schemaReader;
	public Logger tracer;
	private BlockingQueue<DeviceCommand> deviceCommandQueue = new LinkedBlockingQueue<DeviceCommand>();
	private Lock processingLock = new ReentrantLock();
	private long processedOperCount = 0;
	private long processedTxnCount = 0;
	private long processedLogSize = 0;
	private long processedLogSegmentCount = 0;	
	private Path currentDataLakeObject;
	private Path backupSnapshot;
	private HashMap<Integer, Path> replicaPaths = new HashMap<Integer, Path>();
	private static DeviceStageManager deviceStageManager = DeviceStageManager.getDataStageManagerInstance();
	private List<Integer> allDstIndexes = new ArrayList<Integer>();
	private List<String> dstDBAliases = new ArrayList<String>() ;
	private String dstDBAliasStr = "";
	private HashMap<Integer, DeviceStatsCollector> statsCollectors = new HashMap<Integer, DeviceStatsCollector>();

	private Device(Path root, Path upload) throws SyncLiteException {
		this.rootPath = root;
		this.uploadPath = upload;
		DeviceIdentifier deviceIdentifier = validateDeviceDataRoot(root);
		if (deviceIdentifier != null) {
			this.uuid = deviceIdentifier.uuid;
			this.deviceName = deviceIdentifier.name;
		} else {
			throw new SyncLiteException("Bad device. The device root path is invalid");
		}
		this.status = DeviceStatus.UNREGISTERED;
		initLogger();
		this.schemaReader = new SchemaReader();
		this.lastReplicatedCommitID = System.currentTimeMillis();
		this.lastConsolidatedCommitID = System.currentTimeMillis();
		this.logsToProcessSince = Long.MAX_VALUE;
		this.deviceType = DeviceType.UNKNOWN;

		initializeDstIndexes();    
		initializeConsolidatorMetadataFile();
		initializeReplicatorMetadataFile();
		initializeDeviceStatsCollector();
		if (this.status != DeviceStatus.UNREGISTERED) {
			Monitor.getInstance().incrRegisteredDeviceCnt(1L);
		}
		initReplicas();

		if (this.status == DeviceStatus.SUSPENDED) {
			checkLimits();
		}

		initializeCommandDirectory();	
		initializeFailedLogDirectory();
		Monitor.getInstance().incrResynchronizationCnt(this.dbID);
		this.tracer.info("Device initialized with status : " + this.status);
	}

	private void initializeFailedLogDirectory() throws SyncLiteException {
		for (int dstIndex : allDstIndexes) {
			boolean skipFailedLogs = ConfLoader.getInstance().getDstSkipFailedLogFiles(dstIndex);
			if (skipFailedLogs) {
				Path p = rootPath.resolve("failed_logs_for_dst_" + dstIndex);
				this.failedLogSegmentsDirectoryPaths.put(dstIndex, p);		
				try {
					Files.createDirectories(p);
				} catch (IOException e) {
					throw new SyncLiteException("Failed to create failed log segments directort at : " + p);
				}
			}
		}
	}

	public Long getLastConsolidatedLogSegmentSequenceNumber() {
		long minLogSegmentSeqNum = Long.MAX_VALUE;
		for (int dstIndex : allDstIndexes) {
			if (minLogSegmentSeqNum > this.consolidatorMetadataMgrs.get(dstIndex).getLastConsolidatedCDCLogSegmentSeqNum()) {
				minLogSegmentSeqNum = this.consolidatorMetadataMgrs.get(dstIndex).getLastConsolidatedCDCLogSegmentSeqNum();
			}			
		}
		return minLogSegmentSeqNum;
	}
	
	public Path getFailedLogSegmentsDirectoryPath(int dstIndex) {
		return this.failedLogSegmentsDirectoryPaths.get(dstIndex);
	}

	private final void initializeDeviceStatsCollector() throws SyncLiteException {
		try {
			for (int dstIndex : allDstIndexes) {
				DeviceStatsCollector statsCollector = new DeviceStatsCollector(this, dstIndex);
				this.statsCollectors.put(dstIndex, statsCollector);
			}
		} catch (SyncLiteException e) {
			throw new SyncLiteException("Failed to initialize device stats collectors : " , e);
		}
	}

	private final void resetDeviceTableStats(int dstIndex) throws SyncLiteException {
		try {
			if (statsCollectors != null) {
				statsCollectors.get(dstIndex).resetTableStats();
			}
		} catch (SyncLiteException e) {
			throw new SyncLiteException("Failed to reset table statistics in device stats collector : ", e);
		}
	}

	private final void initializeDstIndexes() {		   
		if (ConfLoader.getInstance().getNumDestinations() > 1) {        	
			DevicePatternType patType = ConfLoader.getInstance().getMapDevicesToDstPatternType();
			boolean first = true;
			StringBuilder dstDBAliasesStrBuilder = new StringBuilder();
			for (int idx = 1; idx <= ConfLoader.getInstance().getNumDestinations(); ++idx) {
				Pattern dstPattern = ConfLoader.getInstance().getMapDevicesToDstPattern(idx);        		
				if (patType == DevicePatternType.DEVICE_NAME_PATTERN) {
					if (dstPattern.matcher(this.deviceName).matches()) {
						this.allDstIndexes.add(idx);
						this.dstDBAliases.add(ConfLoader.getInstance().getDstAlias(idx));
						if (!first) {
							dstDBAliasesStrBuilder.append(", ");
						}
						dstDBAliasesStrBuilder.append(ConfLoader.getInstance().getDstAlias(idx));
						first = false;
					}
				} else if (patType == DevicePatternType.DEVICE_ID_PATTERN) {
					if (dstPattern.matcher(this.uuid).matches()) {
						this.allDstIndexes.add(idx);
						this.dstDBAliases.add(ConfLoader.getInstance().getDstAlias(idx));
						if (!first) {
							dstDBAliasesStrBuilder.append(", ");
						}
						dstDBAliasesStrBuilder.append(ConfLoader.getInstance().getDstAlias(idx));
						first = false;
					}        			
				}
			}
			
			if (this.allDstIndexes.size() > 0) {
				this.dstDBAliasStr = dstDBAliasesStrBuilder.toString();
			} else {
				//Assign default destination
				this.allDstIndexes.add(ConfLoader.getInstance().getDefaultDstIndexForUnmappedDevices());
				this.dstDBAliases.add(ConfLoader.getInstance().getDstAlias(ConfLoader.getInstance().getDefaultDstIndexForUnmappedDevices()));
				this.dstDBAliasStr =  ConfLoader.getInstance().getDstAlias(ConfLoader.getInstance().getDefaultDstIndexForUnmappedDevices());			
			}
		} else {
			this.allDstIndexes.add(1);
			this.dstDBAliases.add(ConfLoader.getInstance().getDstAlias(1));
			this.dstDBAliasStr = ConfLoader.getInstance().getDstAlias(1);
		}
	}

	private final void initReplicas() throws SyncLiteException {
		if (this.status != DeviceStatus.UNREGISTERED) {
			if (SyncLiteLoggerInfo.isTransactionalDeviceType(this.deviceType)) {
				//
				//All destinations share same replica 
				//
				Path replicaPath = SyncLiteLoggerInfo.getDataBackupPath(rootPath, dbName);
				for (int dstIndex : this.allDstIndexes) {
					this.replicaPaths.put(dstIndex, replicaPath);
				}
			} else {
				//
				//For telemetry and appender devices, we maintain one replica per destination
				//
				Path replicaPath = SyncLiteLoggerInfo.getDataBackupPath(rootPath, dbName);
				for (int dstIndex : this.allDstIndexes) {
					Path dstReplicaPath = SyncLiteReplicatorInfo.getReplicaPath(rootPath, dbName, dstIndex);
					if (! Files.exists(dstReplicaPath)) {
						//TODO
						//If a new destination is added at runtime, a copy of the existing replica is made..
						//
						try {
							Files.copy(replicaPath, dstReplicaPath, StandardCopyOption.REPLACE_EXISTING);
						} catch (IOException e) {
							throw new SyncLiteException("Failed to make a copy of replica file " + replicaPath +" for destination : " + dstIndex, e);
						}
					}
					this.replicaPaths.put(dstIndex, dstReplicaPath);
				}
			}
		}
	}

	public DeviceCommand peekDeviceCommand() {
		return deviceCommandQueue.peek();
	}

	public DeviceCommand pollDeviceCommand() {
		return deviceCommandQueue.poll();
	}

	public void putDeviceCommand(DeviceCommand deviceCommand) {
		try {
			deviceCommandQueue.put(deviceCommand);
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private final void initLogger() {
		//Driver.initLoggerConfig(Path.of(root.toString(), "synclite_device.trace"));
		//this.tracer = org.apache.logging.log4j.LogManager.getLogger(this.uuid);
		this.tracer = Logger.getLogger(this.uuid);
		switch (ConfLoader.getInstance().getTraceLevel()) {
		case ERROR:
			this.tracer.setLevel(Level.ERROR);
			break;
		case INFO:
			this.tracer.setLevel(Level.INFO);
			break;
		case DEBUG:
			this.tracer.setLevel(Level.DEBUG);
			break;
		}
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("SyncLiteConsolidatorDeviceTracer");
		String deviceTraceFileName = "synclite_device.trace";
		fa.setFile(Path.of(Device.this.rootPath.toString(), deviceTraceFileName).toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] [%t] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setAppend(true);
		fa.activateOptions();
		tracer.addAppender(fa);
	}

	@Override
	public String toString() {
		return " Device at path : " + this.rootPath.toString() + ", status : " + this.status;
	}

	public List<Integer> getAllDstIndexes() {
		return this.allDstIndexes;
	}

	public DeviceStatus getStatus() {
		return this.status;
	}

	public String getStatusDescription() {
		return this.statusDescription;
	}

	public Boolean allowsConcurrentWriters() {
		return (this.allowsConcurrentWriters == 1);
	}
	
	public Path getDeviceDataRoot() {
		return this.rootPath;
	}

	public Path getDeviceLocalCommandRoot() {
		return this.localCommandPath;
	}

	public Path getDeviceRemoteCommandRoot() {
		return this.remoteCommandPath;
	}

	public Path getDeviceUploadRoot() {
		return this.uploadPath;
	}

	public String getDeviceUUID() {
		return this.uuid;
	}

	public String getDeviceName() {
		return this.deviceName;
	}

	public String getDBName() {
		return this.dbName;
	}

	public String getDstDBAliasStr() {		
		return this.dstDBAliasStr;
	}

	public DeviceType getDeviceType() {
		return this.deviceType;
	}

	public MetadataManager getLoggerMetadataMgr() {
		return this.loggerMetadataMgr;
	}

	public MetadataManager getReplicatorMetadataMgr() {
		return this.replicatorMetadataMgr;
	}

	public ConsolidatorMetadataManager getConsolidatorMetadataMgr(int dstIndex) {
		return this.consolidatorMetadataMgrs.get(dstIndex);
	}

	private boolean disablePeriodicSnapshots() {
		return (ConfLoader.getInstance().getDeviceSnapshotIntervalS() == 0);
	}

	public long getLastReplicatedCommitID() {
		return this.lastReplicatedCommitID;
	}

	public long getLastConsolidatedCommitID() {
		return this.lastConsolidatedCommitID;
	}

	public long getLogsToProcessSince() {
		return this.logsToProcessSince;
	}

	public DeviceStatsCollector getDeviceStatsCollector(int dstIndex) {
		return this.statsCollectors.get(dstIndex);
	}

	public long getProcessedLogSize() {
		return this.processedLogSize;
	}

	public long getProcessedOperCount() {
		return this.processedOperCount;
	}

	public long getProcessedTxnCount() {
		return this.processedTxnCount;
	}

	public long getProcessedLogSegmentCount() {
		return this.processedLogSegmentCount;
	}

	public long getLatency() {

		if (this.logsToProcessSince == Long.MAX_VALUE) {
			//All logs processed
			return 0;
		}

		return (System.currentTimeMillis() - this.logsToProcessSince);
	}

	public void updateLastReplicatedCommitID(long lastReplicatedCommitID) {
		this.lastReplicatedCommitID = lastReplicatedCommitID;
	}

	public void updateLastConsolidatedCommitID(long lastConsolidatedCommitID) {
		this.lastConsolidatedCommitID = lastConsolidatedCommitID;
	}

	public void updateLogsToProcessSince(long logsToProcessSince) {
		this.logsToProcessSince = logsToProcessSince;
	}

	public void resetInitializedSnapshot(int dstIndex) throws SyncLiteException {    	
		consolidatorMetadataMgrs.get(dstIndex).resetInitializedSnapshot();
	}

	public static Device getDeviceByName(String deviceName) {
		return nameToDeviceMap.get(deviceName);
	}

	public static Device getDeviceByID(String deviceID) {
		return idToDeviceMap.get(deviceID);
	}

	public static void remove(Device dev) {
		devices.remove(dev.getDeviceDataRoot());
		nameToDeviceMap.remove(dev.getDeviceName());
		idToDeviceMap.remove(dev.getDeviceUUID());
	}


	public static Device getInstance(Path root, Path upload) throws SyncLiteException {
		if (root == null) {
			return null;
		}
		return devices.computeIfAbsent(upload.getFileName(), s -> {
			try {
				Device dev = new Device(root, upload);
				nameToDeviceMap.put(dev.getDeviceName(), dev);
				idToDeviceMap.put(dev.getDeviceUUID(), dev);
				return dev;
			} catch (SyncLiteException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public static Device findInstance(Path upload) {
		return devices.get(upload.getFileName());
	}

	private final void initializeCommandDirectory() throws SyncLiteException {
		if (ConfLoader.getInstance().getEnableDeviceCommandHandler()) {
			//set and create command dir in stage for this device
			this.localCommandPath = rootPath.resolve("commands");			
			try {
				Files.createDirectories(localCommandPath);
			} catch (IOException e) {
				throw new SyncLiteException("Failed to create local command path : " + this.localCommandPath);
			}
			this.remoteCommandPath = ConfLoader.getInstance().getDeviceCommandRoot().resolve(uploadPath.getFileName());
			if (! deviceStageManager.containerExists(remoteCommandPath, SyncLiteObjectType.COMMAND_CONTAINER)) {
				deviceStageManager.createContainer(remoteCommandPath, SyncLiteObjectType.COMMAND_CONTAINER);
			}
		}
	}

	private final void initializeReplicatorMetadataFile() throws SyncLiteException {
		Path replicatorMetadataFile = Path.of(rootPath.toString(), SyncLiteReplicatorInfo.getMetadataFileName());
		try {
			replicatorMetadataMgr = MetadataManager.getInstance(replicatorMetadataFile);
		} catch (SQLException e) {
			throw new SyncLiteException("Bad device. Failed to open replicator metadata file : " + replicatorMetadataFile, e);
		}

		try {

			String strVal = replicatorMetadataMgr.getStringProperty("database_name");
			if (strVal != null) {
				this.dbName = strVal;
			}

			strVal = replicatorMetadataMgr.getStringProperty("device_type");
			if (strVal != null) {
				this.deviceType = DeviceType.valueOf(strVal);
			}

			strVal = replicatorMetadataMgr.getStringProperty("device_name");
			if (strVal != null) {
				if (!this.deviceName.equals(strVal)) {
					throw new SyncLiteException("Bad device. Device name mismatch in device root and metadata file : " + replicatorMetadataFile);
				}
			}

			strVal = replicatorMetadataMgr.getStringProperty("allow_concurrent_writers");
			if (strVal != null) {
				this.allowsConcurrentWriters = Integer.valueOf(strVal);
			} else {
				this.allowsConcurrentWriters = 0;
			}

			strVal = replicatorMetadataMgr.getStringProperty("registered_time");
			if (strVal != null) {
				this.registeredTime = strVal;
			}

			strVal = replicatorMetadataMgr.getStringProperty("detection_time");
			if (strVal != null) {
				this.detectionTime = strVal;
			}

			strVal = replicatorMetadataMgr.getStringProperty("last_status_update_time");
			if (strVal != null) {
				this.lastStatusUpdateTime = strVal;
			}

			strVal = replicatorMetadataMgr.getStringProperty("status_description");
			if (strVal != null) {
				this.statusDescription = strVal;
			}

			strVal = replicatorMetadataMgr.getStringProperty("status");
			if (strVal != null) {
				this.status = DeviceStatus.valueOf(strVal);
			} else {
				this.status = DeviceStatus.valueOf("UNREGISTERED");
			}

			strVal = replicatorMetadataMgr.getStringProperty("data_backup_snapshot");
			if (strVal != null) {
				this.backupSnapshot = Path.of(strVal);
			} else {
				this.backupSnapshot = null;
			}
			
		} catch (SQLException e) {
			throw new SyncLiteException("Bad device. Failed to read/write records in metadata file.", e);
		}
	}


	private final void initializeConsolidatorMetadataFile() throws SyncLiteException {
		
		for (int dstIndex : allDstIndexes) {
			Path consolidatorMetadataFile = Path.of(rootPath.toString(), SyncLiteConsolidatorInfo.getMetadataFileName(dstIndex));
			ConsolidatorMetadataManager mgr = null;
			try {
				mgr = ConsolidatorMetadataManager.getInstance(consolidatorMetadataFile, this, dstIndex);
				mgr.initializeConsolidatorMetadataFile();
				consolidatorMetadataMgrs.put(dstIndex, mgr);
			} catch (SQLException e) {
				throw new SyncLiteException("Bad device. Failed to open consolidator metadata file : " + consolidatorMetadataFile, e);
			}
		}
	}


	public static DeviceIdentifier validateDeviceDataRoot(Path deviceRoot) {
		Path deviceName = deviceRoot.getFileName();
		String deviceNameStr = deviceName.toString();
		if (deviceNameStr.startsWith(SyncLiteLoggerInfo.getDeviceDataRootPrefix())) {
			String[] tokens = deviceName.toString().split("-");

			if (tokens.length == 6) {
				//String uuidStr =  String.join(tokens[1], "-", tokens[2], "-", tokens[3], "-" , tokens[4], "-" , tokens[5]);
				String uuidStr = deviceNameStr.substring(deviceNameStr.indexOf("-") + 1);
				try {
					UUID deviceUUID = UUID.fromString(uuidStr);
					if (deviceUUID.toString().equals(uuidStr)) {
						return new DeviceIdentifier(uuidStr, "");
					}
					return null;
				} catch (IllegalArgumentException e) {
					return null;
				}
			} else if (tokens.length == 7) {
				//String uuidStr = String.join(tokens[2], "-", tokens[3], "-", tokens[4], "-" , tokens[5], "-" , tokens[6]);
				String deviceNameStrSubStr = deviceNameStr.substring(deviceNameStr.indexOf("-") + 1);
				String uuidStr = deviceNameStrSubStr.substring(deviceNameStrSubStr.indexOf("-") + 1);
				try {
					UUID deviceUUID = UUID.fromString(uuidStr);
					if (deviceUUID.toString().equals(uuidStr)) {
						return new DeviceIdentifier(uuidStr, tokens[1]);
					}
					return null;
				} catch (IllegalArgumentException e) {
					return null;
				}
			}
		}
		return null;
	}

	public boolean tryRegisterDevice() throws SyncLiteException {
		Path loggerMetadataFile = null;
		try {
			loggerMetadataFile = findLoggerMetadataFile();
		} catch(SyncLiteStageException e) {
			tracer.error("Failed to operate metadata file in the device stage/data root with exception + ", e);
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to search metadata file in the device root");
			return false;
		}
		if (loggerMetadataFile == null) {
			updateDeviceStatus(status, "Metadata file not available yet");
			return false;
		}

		try {
			this.loggerMetadataMgr = MetadataManager.getInstance(loggerMetadataFile);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Metadata file is unreadable");
			return false;
		}

		try {
			String uuid = loggerMetadataMgr.getStringProperty("uuid");
			if (uuid == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to find uuid record in the metadata file");
				return false;
			} else {
				if (!this.uuid.equalsIgnoreCase(uuid)) {
					updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. UUID record mismatch found in the metadata file");
					return false;
				}
			}
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read uuid record in the metadata file");
			return false;
		}

		try {
			String name = loggerMetadataMgr.getStringProperty("database_name");
			if (name == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to find database_name record in the metadata file");
				return false;
			}
			this.dbName = name;
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read database_name record in the metadata file");
			return false;
		}


		try {
			String deviceName = loggerMetadataMgr.getStringProperty("device_name");
			if (deviceName != null) {
				if (!this.deviceName.equals(deviceName)) {
					updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Device name in device root does not match metadata file contents : " + loggerMetadataMgr.getMetadataFilePath());
				}
			}
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read device_name record in the metadata file");
			return false;
		}

		try {
			String deviceTypeStr = loggerMetadataMgr.getStringProperty("device_type");
			if (deviceTypeStr == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Device type missing in the metadata file : " + loggerMetadataMgr.getMetadataFilePath());
				return false;
			} else {
				this.deviceType = DeviceType.valueOf(deviceTypeStr);
				if (deviceType == null) {
					updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Invalid device type in the metadata file : " + loggerMetadataMgr.getMetadataFilePath());
					return false;
				}
			}
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read device_type record in the metadata file");
			return false;
		}

		try {
			String allowConcurrentWritersStr = loggerMetadataMgr.getStringProperty("allow_concurrent_writers");
			if (allowConcurrentWritersStr == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Allow Concurrent Writers property missing in the metadata file : " + loggerMetadataMgr.getMetadataFilePath());
				return false;
			} else {
				this.allowsConcurrentWriters = Integer.valueOf(allowConcurrentWritersStr);
				if (deviceType == null) {
					updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Allow Concurrent Writers property missing in the metadata file : " + loggerMetadataMgr.getMetadataFilePath());
					return false;
				}
			}
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read device_type record in the metadata file");
			return false;
		}

		try {
			Long databaseID = loggerMetadataMgr.getLongProperty("database_id");
			if (databaseID == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to find database_id record in the metadata file");
				return false;
			}
			this.dbID = databaseID;
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read database_id record in the metadata file");
			return false;
		}

		try {
			Long backupShipped = loggerMetadataMgr.getLongProperty("backup_shipped");
			if (backupShipped == null) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to find backup status record in the metadata file");
				return false;
			}
			if (backupShipped == 0) {
				return false;
			}
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to read backup status record in the metadata file");
			return false;
		}

		try {
			replicatorMetadataMgr.upsertProperty("database_name", this.dbName);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write database_name record in the replicator metadata file");
		}

		try {
			replicatorMetadataMgr.upsertProperty("device_name", this.deviceName);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write database_name record in the replicator metadata file");
		}

		try {
			replicatorMetadataMgr.upsertProperty("device_type", this.deviceType);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write device_type record in the replicator metadata file");
		}

		try {
			replicatorMetadataMgr.upsertProperty("allow_concurrent_writers", this.allowsConcurrentWriters);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write allow_concurrent_writers record in the replicator metadata file");
		}

		try {
			replicatorMetadataMgr.upsertProperty("database_id", this.dbID);
		} catch (SQLException e) {
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write database_id record in the replicator metadata file");
		}
		
		for(int dstIndex : allDstIndexes) {
			try {
				consolidatorMetadataMgrs.get(dstIndex).upsertProperty("database_name", this.dbName);
			} catch (SQLException e) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write device_name record in the importer metadata file");
			}

			try {
				consolidatorMetadataMgrs.get(dstIndex).upsertProperty("device_name", this.deviceName);
			} catch (SQLException e) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write device_name record in the importer metadata file");
			}

			try {
				consolidatorMetadataMgrs.get(dstIndex).upsertProperty("device_type", this.deviceType);
			} catch (SQLException e) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write device_type record in the importer metadata file");
			}

			try {
				consolidatorMetadataMgrs.get(dstIndex).upsertProperty("database_id", this.dbID);
			} catch (SQLException e) {
				updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to write database_id record in the importer metadata file");
			}
		}

		try {
			downloadReplica();
		} catch(SyncLiteStageException e) {
			tracer.debug("Failed to download replica from device stage ", e);
			updateDeviceStatus(DeviceStatus.REGISTRATION_FAILED, "Bad device. Unable to download replica from device stage");
			return false;
		}

		updateDeviceStatus(DeviceStatus.REGISTERED, "");
		initReplicas();
		Monitor.getInstance().incrRegisteredDeviceCnt(1L);
		return true;
	}

	private final void downloadReplica() throws SyncLiteException{
		Path replicaInRoot = SyncLiteLoggerInfo.getDataBackupPath(rootPath, dbName);
		Path replicaInUpload = SyncLiteLoggerInfo.getDataBackupPath(uploadPath, dbName);
		try {
			long publishTime = deviceStageManager.downloadObject(replicaInUpload, replicaInRoot, SyncLiteObjectType.DATA);
			if(publishTime == 0) {
				throw new SyncLiteException("Device replica missing in device stage");
			}
			//dataBackupSnapshot();
		} catch (SyncLiteStageException e) {
			throw new SyncLiteException("Failed to copy replica file from device stage to root directory " + rootPath, e);
		}
	}

	public final void updateDeviceStatus(DeviceStatus status, String statusDescription) throws SyncLiteException {
		try {
			this.status = status;
			replicatorMetadataMgr.upsertProperty("status", this.status.toString());
			this.statusDescription = statusDescription;
			replicatorMetadataMgr.upsertProperty("status_description", this.statusDescription);
			if (status == DeviceStatus.UNREGISTERED) {
				this.detectionTime = Instant.now().toString();
				replicatorMetadataMgr.upsertProperty("detection_time", this.detectionTime);
			} else if (status == DeviceStatus.REGISTERED) {
				this.registeredTime = Instant.now().toString();
				replicatorMetadataMgr.upsertProperty("registered_time", this.registeredTime);
			}
			this.lastStatusUpdateTime = Instant.now().toString();
			replicatorMetadataMgr.upsertProperty("last_status_update_time", this.lastStatusUpdateTime);
		} catch (SQLException e) {
			//throw new SyncLiteException("Bad device. Failed to update device status in metadata file", e);
			//Ignore this is not fatal
		}
		Monitor.getInstance().registerChangedDevice(this);
	}

	private final Path findLoggerMetadataFile() throws SyncLiteStageException {
		Path loggerMetadataFilePath = deviceStageManager.findObjectWithSuffix(uploadPath, SyncLiteLoggerInfo.getMetadataFileSuffix(), SyncLiteObjectType.METADATA);
		if (loggerMetadataFilePath != null) {
			String metadataFileName = loggerMetadataFilePath.getFileName().toString();
			Path loggerMetadataFileInRoot = Path.of(rootPath.toString(), metadataFileName);
			deviceStageManager.downloadObject(loggerMetadataFilePath, loggerMetadataFileInRoot, SyncLiteObjectType.METADATA);
			return loggerMetadataFileInRoot;
		}
		return null;
	}


	public CDCLogSegment getNewCDCLogSegment(long sequenceNumber) throws SyncLiteException {
		CDCLogSegment segment = new CDCLogSegment(this, sequenceNumber, SyncLiteReplicatorInfo.getCDCLogSegmentPath(this.rootPath, this.dbName, this.dbID, sequenceNumber));
		if (segment.path.toFile().exists()) {
			segment.path.toFile().delete();
		}
		tracer.debug("Creating a new CDC log segment : " + segment);
		return segment;
	}

	public CommandLogSegment getNextCommandLogSegmentToProcess(CommandLogSegment logSegment) throws SyncLiteException {
		long nextLogSegmentSeqNum = logSegment.sequenceNumber + 1;
		return getCommandLogSegment(nextLogSegmentSeqNum);
	}


	public EventLogSegment getNextEventLogSegmentToProcess(EventLogSegment logSegment) throws SyncLiteException {
		long nextLogSegmentSeqNum = logSegment.sequenceNumber + 1;
		return getEventLogSegment(nextLogSegmentSeqNum);
	}

	public CDCLogSegment getNextCDCLogSegmentToProcess(CDCLogSegment logSegment) throws SyncLiteException {
		long nextLogSegmentSeqNum = logSegment.sequenceNumber + 1;
		Path nextLogSegmentPath = SyncLiteReplicatorInfo.getCDCLogSegmentPath(rootPath, dbName, this.dbID, nextLogSegmentSeqNum);
		CDCLogSegment nextLogSegment = new CDCLogSegment(this, nextLogSegmentSeqNum, nextLogSegmentPath);
		if (nextLogSegmentPath.toFile().exists()) {
			//Check if the next to next cdc log segment is already created then only return this
			//for applying.
			//We cannot apply log segment that is being written onto right now.
			//
			/*            long nextToNextLogSegmentSeqNum = logSegment.sequenceNumber + 2;
            Path nextToNextLogSegmentPath = SyncLiteMirror.getCDCLogSegmentPath(root, dbName, this.dbID, nextToNextLogSegmentSeqNum);
            if (nextToNextLogSegmentPath.toFile().exists()) {
                //Check if nextLogSegment has been closed by the replicator
                if (nextLogSegment.isClosed()) {
                    return nextLogSegment;
                }
            }
			 */
			if (nextLogSegment.isReadyToApply()) {
				return nextLogSegment;
			}
		}
		return null;
	}

	public CDCLogSegment getNextCDCLogSegment(CDCLogSegment logSegment) {
		long nextLogSegmentSeqNum = logSegment.sequenceNumber + 1;
		Path nextLogSegmentPath = SyncLiteReplicatorInfo.getCDCLogSegmentPath(rootPath, dbName, this.dbID, nextLogSegmentSeqNum);
		if (nextLogSegmentPath.toFile().exists()) {
			return new CDCLogSegment(this, logSegment.sequenceNumber + 1, nextLogSegmentPath);
		}
		return null;
	}

	public Path getReplica(int dstIndex) throws SyncLiteException {
		if (this.status == DeviceStatus.UNREGISTERED) {
			throw new SyncLiteException("Device is not registered yet. Current state : " + this.status);
		}
		return this.replicaPaths.get(dstIndex);
	}

	public long getReplicaSize() throws SyncLiteException {
		if (this.status == DeviceStatus.UNREGISTERED) {
			throw new SyncLiteException("Device is not registered yet. Current state : " + this.status);
		}
		
		//Get the total size of replicas
		
		try {
			long size = 0;
			for (int dstIndex : allDstIndexes) {
				size += Files.size(this.replicaPaths.get(dstIndex));
			}
			return size;
		} catch (IOException e) {
			throw new SyncLiteException("Failed to get replica size for replica : " + this.replicaPaths, e);
		}
	}


	public Path getCurrentDataLakeObject() {
		return this.currentDataLakeObject;
	}

	public void setCurrentDataLakeObject(Path fPath) {
		this.currentDataLakeObject = fPath;
	}

	public void setLastHeartbeatTS(long ts) {
		this.lastHeartbeatTS = ts;
	}

	public long getLastHeartbeatTS() {
		return this.lastHeartbeatTS;
	}

	public Path getStatsFile() throws SyncLiteException {
		Path statsFile = SyncLiteConsolidatorInfo.getStatsFilePath(rootPath, dbName);
		return statsFile;    	
	}

	public Path getCommandLogSegmentPath(long commandLogSegmentSequenceNumber) {
		return SyncLiteLoggerInfo.getCommandLogSegmentPath(rootPath, dbName, dbID, commandLogSegmentSequenceNumber);
	}    

	public Path getCommandLogSegmentPathInUploadDir(long commandLogSegmentSequenceNumber) {
		return SyncLiteLoggerInfo.getCommandLogSegmentPath(uploadPath, dbName, dbID, commandLogSegmentSequenceNumber);
	}

	public CommandLogSegment getCommandLogSegment(long commandLogSegmentSequenceNumber) throws SyncLiteException {        
		//Check in upload and if exists then copy to root
		Path logSegmentPathInRoot = getCommandLogSegmentPath(commandLogSegmentSequenceNumber);
		Path logSegmentPathInUpload = getCommandLogSegmentPathInUploadDir(commandLogSegmentSequenceNumber);
		try {
			long publishTime = deviceStageManager.downloadObject(logSegmentPathInUpload, logSegmentPathInRoot, SyncLiteObjectType.LOG);
			if (publishTime > 0) {
				CommandLogSegment segment = new CommandLogSegment(this, commandLogSegmentSequenceNumber, logSegmentPathInRoot, publishTime);
				//Check if the segment is ready to apply
				if (segment.isReadyToApply()) {
					
					//Now download the associated txn Files if needed
					if (allowsConcurrentWriters == 1) {
						List<Path> txnFiles = deviceStageManager.findObjectsWithSuffixPrefix(uploadPath, segment.path.getFileName().toString(), SyncLiteLoggerInfo.getCommandLogTxnFileSuffix(), SyncLiteObjectType.LOG);
						if ((txnFiles != null) && !txnFiles.isEmpty()) {
							for (Path f : txnFiles) {
								Path txnFilePathInRoot = rootPath.resolve(f.getFileName().toString());
								Path txnFilePathInUpload = uploadPath.resolve(f.getFileName().toString());								
								long t = deviceStageManager.downloadObject(txnFilePathInUpload, txnFilePathInRoot, SyncLiteObjectType.LOG);
								if (t == 0) {
									//Download failed.
									tracer.error("Failed to download txn file : " + txnFilePathInUpload + " for command log segment : " + segment);
									return null;				
								}
							}
						}
					}					
					return segment;
				}
			}
		} catch (Exception e) {
			//throw new SyncLiteException("Failed to read and copy command log segment : " + logSegmentPathInUpload + " to " + logSegmentPathInRoot, e);
			tracer.error("Failed to read and copy command log segment : " + logSegmentPathInUpload + " to " + logSegmentPathInRoot + " : " + e.getMessage(), e);
		}
		return null;
	}

	public Path getCDCLogSegmentPath(long cdcLogSegmentSequenceNumber) {
		return SyncLiteReplicatorInfo.getCDCLogSegmentPath(rootPath, dbName, this.dbID, cdcLogSegmentSequenceNumber);
	}

	public CDCLogSegment getCDCLogSegment(long cdcLogSegmentSequenceNumber) {
		Path logSegmentPath = getCDCLogSegmentPath(cdcLogSegmentSequenceNumber);
		if (logSegmentPath.toFile().exists()) {
			return new CDCLogSegment(this, cdcLogSegmentSequenceNumber, logSegmentPath);
		}
		return null;
	}

	public Path getEventLogSegmentPath(long eventLogSegmentSequenceNumber) {
		return SyncLiteLoggerInfo.getEventLogSegmentPath(rootPath, dbName, dbID, eventLogSegmentSequenceNumber);
	}

	public Path getEventLogSegmentPathInUploadDir(long eventLogSegmentSequenceNumber) {
		return SyncLiteLoggerInfo.getEventLogSegmentPath(uploadPath, dbName, dbID, eventLogSegmentSequenceNumber);
	}

	public EventLogSegment getEventLogSegment(long eventLogSegmentSequenceNumber) throws SyncLiteException {
		//Check in upload and if exists then copy to root
		Path logSegmentPathInRoot = getEventLogSegmentPath(eventLogSegmentSequenceNumber);
		Path logSegmentPathInUpload = getEventLogSegmentPathInUploadDir(eventLogSegmentSequenceNumber);
		try {
			long publishTime = deviceStageManager.downloadObject(logSegmentPathInUpload, logSegmentPathInRoot, SyncLiteObjectType.LOG);
			if (publishTime > 0) {
				EventLogSegment segment = new EventLogSegment(this, eventLogSegmentSequenceNumber, logSegmentPathInRoot, publishTime);
				//Check if this log segment is ready
				if (segment.isReadyToApply()) {
					
					//Now download the associated txn Files if needed
					if (allowsConcurrentWriters == 1) {
						List<Path> txnFiles = deviceStageManager.findObjectsWithSuffixPrefix(uploadPath, segment.path.getFileName().toString(), SyncLiteLoggerInfo.getEventLogTxnFileSuffix(), SyncLiteObjectType.LOG);
						if ((txnFiles != null) && !txnFiles.isEmpty()) {
							for (Path f : txnFiles) {
								Path txnFilePathInRoot = rootPath.resolve(f.getFileName().toString());
								Path txnFilePathInUpload = uploadPath.resolve(f.getFileName().toString());								
								long t = deviceStageManager.downloadObject(txnFilePathInUpload, txnFilePathInRoot, SyncLiteObjectType.LOG);
								if (t == 0) {
									//Download failed.
									tracer.error("Failed to download txn file : " + txnFilePathInUpload + " for event log segment : " + segment);
									return null;
								}
							}
						}
					}					
					return segment;
				}
			}
		} catch (Exception e) {
			//throw new SyncLiteException("Failed to read and copy event log segment : " + logSegmentPathInUpload + " to " + logSegmentPathInRoot);
			tracer.error("Failed to read and copy event log segment : " + logSegmentPathInUpload + " to " + logSegmentPathInRoot);
		}
		return null;
	}

	private final void cleanupDevice() throws SyncLiteException {
		try {
			Files.walk(rootPath).map(Path::toFile).filter(s->!(s.toString().endsWith(".trace"))).forEach(File::delete);
		} catch (IOException e) {
			throw new SyncLiteException("Failed to cleanup device :", e);
		}
	}

	public static void putDeviceCommandForMatchingDeviceIDs(Pattern commandDeviceIDPattern, DeviceCommand commandType) {
		for (Device dev : devices.values()) {
			if (commandDeviceIDPattern.matcher(dev.getDeviceUUID()).matches()) {
				dev.putDeviceCommand(commandType);
			}
		}
	}

	public static void putDeviceCommandForMatchingDeviceNames(Pattern commandDeviceNamePattern, DeviceCommand commandType) {
		for (Device dev : devices.values()) {
			if (commandDeviceNamePattern.matcher(dev.getDeviceName()).matches()) {
				dev.putDeviceCommand(commandType);
			}
		}
	}

	public boolean aquireProcessingLock() {
		return processingLock.tryLock();
	}

	public void releaseProcessingLock() {
		try {
			processingLock.unlock();
		} catch (Exception e) {
			//Ignore
		}
	}

	public void incrTotalProcessedLogSize(long size) {
		this.processedLogSize += size;
		Monitor.getInstance().incrTotalProcessedLogSize(size);
	}

	public void incrTotalProcessedOperCount(long cnt) {
		this.processedOperCount  += cnt;
		Monitor.getInstance().incrTotalProcessedOperCount(cnt);
	}

	public void incrTotalProcessedTxnCount(long cnt) {
		this.processedTxnCount  += cnt;
		Monitor.getInstance().incrTotalDstTxnCnt(cnt);
	}

	public void incrTotalProcessedLogSegmentCount(long cnt) {
		this.processedLogSegmentCount += cnt;
		Monitor.getInstance().incrTotalCDCLogSegmentCnt(cnt);
	}

	public void checkLimits() throws SyncLiteException {
		if (ConfLoader.getInstance().getPerDeviceOperationCountLimit() < this.processedOperCount) {
			String errorMsg = "Device exceeded processed operation count limit of " + ConfLoader.getInstance().getPerDeviceOperationCountLimit() + ". Please renew your license.";
			tracer.error(errorMsg);
			updateDeviceStatus(DeviceStatus.SUSPENDED, errorMsg);
		} else if (ConfLoader.getInstance().getPerDeviceProcessedLogSizeLimit() < this.processedLogSize) {
			String errorMsg = "Device has exceeded processed log size limit of " + ConfLoader.getInstance().getPerDeviceProcessedLogSizeLimit() + " bytes. Please renew your license.";
			tracer.error(errorMsg);
			updateDeviceStatus(DeviceStatus.SUSPENDED, errorMsg);
		} else {
			if (this.status != DeviceStatus.SYNCING) {
				updateDeviceStatus(DeviceStatus.SYNCING, "");
			}
		}		
	}

	public final void reInitialize() throws SyncLiteException {
		for (int dstIndex : allDstIndexes) {
			reInitialize(dstIndex);
		}
	}

	private final void reInitialize(int dstIndex) throws SyncLiteException {
		tracer.info("Marking device for reinitialization for dst : " + dstIndex);
		this.consolidatorMetadataMgrs.get(dstIndex).resetInitializationStatus();	
		dataBackupSnapshot();
		resetDeviceTableStats(dstIndex);
		tracer.info("Marked device for reinitialization for dst : " + dstIndex);
	}

	public final void dispatchDeviceCommand(String command, String commandDetails) throws SyncLiteException {
		//Create a file <CurrentTS>.<COMMAND> under device command root in device stage.
		try  {
			long currentTS = System.currentTimeMillis();

			Path commandFilePath = localCommandPath.resolve(currentTS + "." + command);

			Files.createFile(commandFilePath);

			Files.writeString(commandFilePath, commandDetails);

			deviceStageManager.uploadObject(this.remoteCommandPath, commandFilePath, SyncLiteObjectType.COMMAND);

			//Files.delete(commandFilePath);
			tracer.info("Successfully dispatched command : " + command);	
		} catch (IOException e) {
			throw new SyncLiteException("Failed to dispatch command : " + command + " to device :" + this, e);
		}
	}

	private final void deleteDeviceCommand(Path localCommandPath) throws SyncLiteException {
		Path remoteCommandToDelete = this.remoteCommandPath.resolve(localCommandPath.getFileName());
		try  {			 
			deviceStageManager.deleteObject(remoteCommandToDelete, SyncLiteObjectType.COMMAND);
			Files.delete(localCommandPath);
		} catch (IOException | SyncLiteStageException e) {
			throw new SyncLiteException("Failed to delete command : " + remoteCommandToDelete + " from stage :" + this, e);
		}
	}

	public final void cleanupDeviceCommands() throws SyncLiteException {
		if (Files.exists(this.localCommandPath)) {
			long commandTimeoutS = ConfLoader.getInstance().getDeviceCommandTimeoutS();
			long currentTime = System.currentTimeMillis();

			long deleteCommandsBeforeTS = currentTime - (commandTimeoutS * 1000);
			//Find and delete commands before deleteCommandBeforeTS
			//Iterate on devices localCommandPath and delete commands before this TS, first from stage and then from localCommandPath.
			//
			try (var directoryStream = Files.newDirectoryStream(this.localCommandPath)) {
				for (Path cmdFile : directoryStream) {
					String [] tokens = cmdFile.getFileName().toString().split("\\.");
					long cmdTS = 0;
					if (tokens.length == 2) {
						try {
							cmdTS = Long.parseLong(tokens[0]);
							if (cmdTS < deleteCommandsBeforeTS) {
								//Delete this command from remote and local command path.
								deleteDeviceCommand(cmdFile);
							}
						} catch (NumberFormatException e) {
							continue;
						}
					}
				}
			} catch (Exception e) {
				tracer.error("Failed to cleanup timed out device commands", e);
			}
		} 
	}

	public void dataBackupSnapshot() throws SyncLiteException {
		Path backup = SyncLiteLoggerInfo.getDataBackupPath(this.rootPath, this.dbName);
		try {

			if (!Files.exists(backup)) {
				throw new SyncLiteException("Device data backup file missing : " + backup);
			}

			if (this.backupSnapshot != null) {
				replicatorMetadataMgr.deleteProperty("data_backup_snapshot");
				this.backupSnapshot = null;
			}

			Path snapshotPath = SyncLiteReplicatorInfo.getDataBackupSnapshotPath(this.rootPath, this.dbName);
			Files.copy(backup, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
		
			replicatorMetadataMgr.upsertProperty("data_backup_snapshot",snapshotPath);
			this.backupSnapshot = snapshotPath;
			
		} catch (Exception e) {
			throw new SyncLiteException("Failed to take a snapshot of data backup file : " + backup, e);
		}
	}

	public Path getDataBackupSnapshot() {
		return this.backupSnapshot;
	}

}

