package com.synclite.consolidator.watchdog;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.synclite.consolidator.Main;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.UserCommand;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

public class Monitor {

	private static Monitor INSTANCE;

	public static final Monitor getInstance() {
		return INSTANCE;
	}
	public static synchronized Monitor createAndGetInstance(Logger tracer) {		
		if (INSTANCE != null) {
			return INSTANCE;
		} else {
			INSTANCE = new Monitor(tracer);
		}
		return INSTANCE;
	}

	private abstract class Dumper extends Thread {
		public void run() {
			while (!Thread.interrupted()) {
				try {            	
					dump();
					Thread.sleep(screenRefreshIntervalMs);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
		}

		protected abstract void dump();

		protected abstract void init() throws SyncLiteException;

		protected abstract void schedule();
	}

	private class ScreenDumper extends Dumper {

		private void clearScreen() {
			System.out.print("\033[H\033[2J");
			System.out.flush();
			//System.out.print("\f");
			//for(int i = 0; i < 80*300; i++) // Default Height of cmd is 300 and Default width is 80

			//    System.out.print("\b"); // Prints a backspace
		}

		@Override
		protected void dump() {
			clearScreen();
			System.out.println("====================================================================================");
			System.out.println(header);
			System.out.println("====================================================================================");
			System.out.println("Detected Devices : " + detectedDeviceCnt);
			System.out.println("Registered Devices : " + registeredDeviceCnt);
			System.out.println("Initialized Devices : " + initializedDeviceCnt);
			System.out.println("Failed Devices : " + failedDeviceCnt);
			System.out.println("Total Device Initializations : " + totalInitializationCnt.get());
			System.out.println("Total Device Re-synchronizations : " + totalResyncronizationCnt);
			System.out.println("Total Consolidated Tables : " + ConsolidatorSrcTable.getCount());
			if (Main.COMMAND == UserCommand.SYNC) {
				System.out.println("Total Log Segments Applied : " +  totalCDCLogSegmentCnt.get());
			}
			System.out.println("Last Dashboard Update Time : " + Instant.now());
		}


		@Override
		protected void init() {
		}

		@Override
		protected void schedule() {
			start();
		}	
	}

	private class PrometheusDumper extends Dumper{

		private Gauge detectedDeviceCnt;
		private Gauge registeredDeviceCnt;
		private Gauge initializedDeviceCnt;
		private Gauge failedDeviceCnt;
		private Gauge totalConsolidatedTableCnt;
		private Gauge totalLogSegmentsCnt;
		private Gauge totalProcessedLogSize;
		private Gauge totalProcessedOperCnt;
		private Gauge totalProcessedTxnCnt;
		private Gauge latency;
		private Gauge lastHeardbeatTime;
		private ScheduledExecutorService excutionService;

		@Override
		protected void dump() {
			try {
				this.detectedDeviceCnt.set(Monitor.this.detectedDeviceCnt.longValue());
				this.registeredDeviceCnt.set(Monitor.this.registeredDeviceCnt.longValue());
				this.initializedDeviceCnt.set(Monitor.this.initializedDeviceCnt.longValue());
				this.failedDeviceCnt.set(Monitor.this.failedDeviceCnt.longValue());
				this.failedDeviceCnt.set(Monitor.this.failedDeviceCnt.longValue());
				this.totalConsolidatedTableCnt.set(ConsolidatorSrcTable.getCount());
				this.totalLogSegmentsCnt.set(Monitor.this.totalCDCLogSegmentCnt.get());
				this.totalProcessedOperCnt.set(Monitor.this.totalProcessedOperCount.get());
				this.totalProcessedTxnCnt.set(Monitor.this.totalDstTxnCnt.get());
				this.totalProcessedLogSize.set(Monitor.this.totalProcessedLogSize.get());
				this.latency.set(getGlobalLatency());
				this.lastHeardbeatTime.set(System.currentTimeMillis());

				// Push the metrics to the PushGateway
				URL pushGatewayURL = ConfLoader.getInstance().getPrometheusPushGatewayURL();				 
				PushGateway pushGateway = new PushGateway(pushGatewayURL.getHost() + ":" + pushGatewayURL.getPort());
				pushGateway.pushAdd(CollectorRegistry.defaultRegistry, "SyncLiteConsolidator");

			} catch (Exception e) {
				tracer.error("Prometheus statistics publisher failed with exception : ", e);
			}
		}

		@Override
		protected void init() throws SyncLiteException {
			try {
				detectedDeviceCnt = Gauge.build()
						.name("Detected_Devices")
						.help("Detected Devices")
						.register();

				registeredDeviceCnt = Gauge.build()
						.name("Registered_Devices")
						.help("Registered Devices")
						.register();

				initializedDeviceCnt = Gauge.build()
						.name("Initialized_Devices")
						.help("Initialized Devices")
						.register();

				failedDeviceCnt = Gauge.build()
						.name("Failed_Devices")
						.help("Failed Devices")
						.register();

				totalConsolidatedTableCnt = Gauge.build()
						.name("Total_Consolidated_Tables")
						.help("Total_Consolidated_Tables")
						.register();

				totalLogSegmentsCnt = Gauge.build()
						.name("Total_Log_Segments_Applied")
						.help("Total_Log_Segments_Applied")
						.register();

				totalProcessedOperCnt = Gauge.build()
						.name("Total_Processed_Operation_Count")
						.help("Total_Processed_Operation_Count")
						.register();
				
				totalProcessedTxnCnt = Gauge.build()
						.name("Total_Processed_Transaction_Count")
						.help("Total_Processed_Transaction_Count")
						.register();
				
				totalProcessedLogSize = Gauge.build()
						.name("Total_Processed_Log_Size")
						.help("Total_Processed_Log_Size")
						.register();
				
				latency = Gauge.build()
						.name("Latency")
						.help("Latency")
						.register();

				lastHeardbeatTime = Gauge.build()
						.name("Job_Last_Heartbeat_Time")
						.help("Job_Last_Hearthbeat_Time")
						.register();

				excutionService = Executors.newScheduledThreadPool(1);
			} catch (Exception e) {
				tracer.error("Failed to initialize Prometheus statistics publisher : ", e);
			}
		}

		@Override
		protected void schedule() {
			if (excutionService != null) {
				excutionService.scheduleWithFixedDelay(this::dump, 0, ConfLoader.getInstance().getPrometheusStatisticsPublisherIntervalS(), TimeUnit.SECONDS);
			}
		}
	}

	private class FileDumper extends Dumper{
		ScreenDumper screenDumper;
		Connection statsConn;
		PreparedStatement updateDashboardPstmt;
		PreparedStatement deleteDeviceStatusPstmt;
		PreparedStatement insertDeviceStatusPstmt;

		private String createDashboardTableSql = "CREATE TABLE if not exists dashboard(header TEXT, detected_devices LONG, registered_devices LONG, initialized_devices LONG, failed_devices LONG, total_device_initializations LONG, total_device_resynchronizations LONG, total_consolidated_tables LONG, total_log_segments_applied LONG, total_processed_log_size LONG, total_processed_oper_count LONG, total_processed_txn_count LONG, latency LONG, last_heartbeat_time LONG, last_job_start_time LONG)";
		private String insertDashboardTableSql = "INSERT INTO dashboard (header, detected_devices, registered_devices, initialized_devices, failed_devices, total_device_initializations, total_device_resynchronizations, total_consolidated_tables, total_log_segments_applied, total_processed_log_size, total_processed_oper_count, total_processed_txn_count, latency, last_heartbeat_time, last_job_start_time) VALUES('$1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";
		private String updateDashboardTableSql = "UPDATE dashboard SET detected_devices = ?, registered_devices = ?, initialized_devices = ? , failed_devices = ?, total_device_initializations = ?, total_device_resynchronizations = ?, total_consolidated_tables = ?, total_log_segments_applied = ?, total_processed_log_size = ?, total_processed_oper_count = ?, total_processed_txn_count = ?, latency = ?, last_heartbeat_time = ?, last_job_start_time = ?";
		private String selectDashboardTableSql = "SELECT detected_devices FROM dashboard;";

		private String createDeviceStatusTableSql = "CREATE TABLE if not exists device_status(synclite_device_id TEXT, synclite_device_name TEXT, synclite_device_type TEXT, status TEXT, status_description TEXT, path TEXT, database_name TEXT, destination_database_alias TEXT, log_segments_applied LONG, processed_log_size LONG, processed_oper_count LONG, processed_txn_count LONG, latency LONG, last_heartbeat_time LONG, last_consolidated_commit_id LONG, PRIMARY KEY(synclite_device_id, synclite_device_name))";
		private String insertDeviceStatusTableSql = "INSERT INTO device_status(synclite_device_id, synclite_device_name, synclite_device_type, status, status_description, path, database_name, destination_database_alias, log_segments_applied, processed_log_size, processed_oper_count, processed_txn_count, latency, last_heartbeat_time, last_consolidated_commit_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		private String deleteDeviceStatusTableSql = "DELETE FROM device_status WHERE synclite_device_id = ? AND synclite_device_name = ?";

		private FileDumper() {
			this.screenDumper = new ScreenDumper();
		}

		private final void initStats() throws SyncLiteException {
			String url = "jdbc:sqlite:" + Path.of(ConfLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator_statistics.db").toString();
			try {
				statsConn = DriverManager.getConnection(url);

				try (Statement stmt = statsConn.createStatement()) {
					stmt.execute(createDashboardTableSql);
					try (ResultSet rs = stmt.executeQuery(selectDashboardTableSql)) {
						if (!rs.next()) {
							insertDashboardTableSql = insertDashboardTableSql.replace("$1", Monitor.this.header);
							stmt.execute(insertDashboardTableSql);
						}
					}	
					//Add a new column last_job_start_time to dashboard table if not present.
					String checkColSql = "PRAGMA table_info('dashboard')";
		            try (ResultSet rs = stmt.executeQuery(checkColSql)) {
		                boolean lastJobStartTimeExists = false;
		                while (rs.next()) {
		                    String name = rs.getString("name");
		                    if (name.equals("last_job_start_time")) {
		                    	lastJobStartTimeExists = true;
		                        break;
		                    }
		                }		                
		                if (!lastJobStartTimeExists) {
			                String addColSql = "ALTER TABLE dashboard ADD COLUMN last_job_start_time LONG DEFAULT(0)";
		                	stmt.execute(addColSql);
		                }
		            }
					updateDashboardPstmt = statsConn.prepareStatement(updateDashboardTableSql);        			

					stmt.execute(createDeviceStatusTableSql);
					//Upgrade code path
					
					
					//Add a new column last_consolidated_commit_id to device_status table if not present.
					checkColSql = "PRAGMA table_info('device_status')";
		            try (ResultSet rs = stmt.executeQuery(checkColSql)) {
		                boolean lastConsolidatedCommitIDExists = false;
		                while (rs.next()) {
		                    String name = rs.getString("name");
		                    if (name.equals("last_consolidated_commit_id")) {
		                    	lastConsolidatedCommitIDExists = true;
		                        break;
		                    }
		                }		                
		                if (!lastConsolidatedCommitIDExists) {
			                String addColSql = "ALTER TABLE device_status ADD COLUMN last_consolidated_commit_id LONG DEFAULT(0)";
		                	stmt.execute(addColSql);
		                }
		            }

					deleteDeviceStatusPstmt = statsConn.prepareStatement(deleteDeviceStatusTableSql);
					insertDeviceStatusPstmt = statsConn.prepareStatement(insertDeviceStatusTableSql);
				}        		

			} catch (SQLException e) {
				throw new SyncLiteException("Failed to create/open SyncLiteConsolidator statistics file at : " + url, e);
			}

		}

		@Override
		protected void dump() {
			try {
				//screenDumper.dump();
				long currentTime = System.currentTimeMillis();
				if ((lastStatChangeTime < lastStatFlushTime) && ((currentTime - lastStatFlushTime) < heartbeatIntervalMs)) {
					//Skip the update if there is nothing to be updated
					//However force an update if last update was done 30 seconds back
					//as this update also serves as a heartbeat of the consolidator job
					return;
				}				
				statsConn.setAutoCommit(false);
				updateDashboardPstmt.setLong(1, detectedDeviceCnt.get());
				updateDashboardPstmt.setLong(2, registeredDeviceCnt.get());
				updateDashboardPstmt.setLong(3, initializedDeviceCnt.get());
				updateDashboardPstmt.setLong(4, failedDeviceCnt.get());
				updateDashboardPstmt.setLong(5, totalInitializationCnt.get());
				updateDashboardPstmt.setLong(6, totalResyncronizationCnt.get());
				updateDashboardPstmt.setLong(7, ConsolidatorSrcTable.getCount());
				updateDashboardPstmt.setLong(8, totalCDCLogSegmentCnt.get());
				updateDashboardPstmt.setLong(9, totalProcessedLogSize.get());
				updateDashboardPstmt.setLong(10, totalProcessedOperCount.get());
				updateDashboardPstmt.setLong(11, totalDstTxnCnt.get());
				updateDashboardPstmt.setLong(12, getGlobalLatency());
				updateDashboardPstmt.setLong(13, currentTime);
				updateDashboardPstmt.setLong(14, Main.jobStartTime);
				updateDashboardPstmt.addBatch();
				updateDashboardPstmt.executeBatch();	

				if (changedDevices.size() > 0) {
					Device device = changedDevices.poll();
					while (device != null) {
						deleteDeviceStatusPstmt.setString(1, device.getDeviceUUID());
						deleteDeviceStatusPstmt.setString(2, device.getDeviceName());
						deleteDeviceStatusPstmt.execute();
						insertDeviceStatusPstmt.setString(1, device.getDeviceUUID());
						insertDeviceStatusPstmt.setString(2, device.getDeviceName());
						insertDeviceStatusPstmt.setString(3, device.getDeviceType().toString());
						insertDeviceStatusPstmt.setString(4, device.getStatus().toString());
						insertDeviceStatusPstmt.setString(5, device.getStatusDescription());
						insertDeviceStatusPstmt.setString(6, device.getDeviceDataRoot().toString());
						insertDeviceStatusPstmt.setString(7, device.getDBName());						
						insertDeviceStatusPstmt.setString(8, device.getDstDBAliasStr());
						insertDeviceStatusPstmt.setLong(9, device.getProcessedLogSegmentCount());
						insertDeviceStatusPstmt.setLong(10, device.getProcessedLogSize());
						insertDeviceStatusPstmt.setLong(11, device.getProcessedOperCount());
						insertDeviceStatusPstmt.setLong(12, device.getProcessedTxnCount());
						insertDeviceStatusPstmt.setLong(13, device.getLatency());
						insertDeviceStatusPstmt.setLong(14, device.getLastHeartbeatTS());
						insertDeviceStatusPstmt.setLong(15, device.getLastConsolidatedCommitID());
						insertDeviceStatusPstmt.execute();
						device = changedDevices.poll();
					}
				}
				statsConn.commit();
				statsConn.setAutoCommit(true);
				lastStatFlushTime = currentTime;
				//TODO keep doing vacuum on statistics file every periodically. 

			} catch (SQLException e) {
				tracer.error("SyncLite statistics dumper failed with exception : ", e);
			}

			/*
			 *             System.out.println("Detected Devices : " + detectedDeviceCnt);
                System.out.println("Registered Devices : " + registeredDeviceCnt);
                System.out.println("Initialized Devices : " + initializedDeviceCnt);
                System.out.println("Failed Devices : " + failedDeviceCnt);
                System.out.println("Total device initializations : " + totalInitializationCnt.get());
                System.out.println("Total device re-synchronizations : " + totalResyncronizationCnt);
                System.out.println("Total Source Tables : " + ConsolidatorSrcTable.getCount());
                if (Main.COMMAND == UserCommand.SYNC) {
                    System.out.println("Total Log Segments Processed : " + totalCommandLogSegmentCnt);
                    System.out.println("Total Transactions Processed : " + totalSyncliteTxnCnt);
                    System.out.println("Total Transactions Applied to Destination : " + totalDstTxnCnt);
                    System.out.println("Max Latency : " + formatLatency(maxLatency));
                    System.out.println("Min Latency : " + formatLatency(minLatency));
                    System.out.println("Avg Latency : " + formatLatency(avgLatency));
                }   		
        	}	
			 */

		}

		@Override
		protected void init() throws SyncLiteException {
			initStats();
		}

		@Override
		protected void schedule() {
			start();
		}
	}

	private String header;
	private AtomicLong detectedDeviceCnt = new AtomicLong(0);
	private AtomicLong failedDeviceCnt = new AtomicLong(0);
	private AtomicLong registeredDeviceCnt = new AtomicLong(0);
	private AtomicLong initializedDeviceCnt = new AtomicLong(0);
	private AtomicLong totalInitializationCnt = new AtomicLong(0);
	private AtomicLong totalResyncronizationCnt = new AtomicLong(0);
	private AtomicLong totalCommandLogSegmentCnt = new AtomicLong(0);
	private AtomicLong totalCDCLogSegmentCnt = new AtomicLong(0);
	private AtomicLong totalSyncliteTxnCnt = new AtomicLong(0);
	private AtomicLong totalDstTxnCnt = new AtomicLong(0);
	private AtomicLong totalProcessedLogSize = new AtomicLong(0);
	private AtomicLong totalProcessedOperCount = new AtomicLong(0);
	private volatile Device lastQueuedDevice = null;
	private static long screenRefreshIntervalMs = 1000;
	private static final long heartbeatIntervalMs = 30000;  
	private volatile long lastStatChangeTime = System.currentTimeMillis();
	private long lastStatFlushTime = System.currentTimeMillis();
	private Dumper dumper;
	private List<Dumper>additionalDumpers = new ArrayList<Dumper>();
	private BlockingQueue<Device> changedDevices = new LinkedBlockingQueue<Device>();
	private Logger tracer;

	private Monitor(Logger tracer) {
		this.tracer = tracer;
		if (Main.COMMAND == UserCommand.SYNC) {
			this.header = "Consolidating devices from data directory " + ConfLoader.getInstance().getDeviceDataRoot()  + " into " + getDstNames() + "...";
		} else if (Main.COMMAND == UserCommand.IMPORT) {
			this.header = "Importing devices from data directory " + ConfLoader.getInstance().getDeviceDataRoot()  + " into " + getDstNames() + "...";
		}
		if (ConfLoader.getInstance().getGuiDashboard()) {
			dumper = new FileDumper();
		} else {
			dumper = new ScreenDumper();
		}

		if (ConfLoader.getInstance().getEnablePrometheusStatisticsPublisher()) {
			additionalDumpers.add(new PrometheusDumper());	
		}		
		screenRefreshIntervalMs = ConfLoader.getInstance().getUpdateStatisticsIntervalS() * 1000;
	}

	public void setlastQueuedDevice(Device device) {
		this.lastQueuedDevice = device;
	}

	public final void registerChangedDevice(Device d) {
		try {
			changedDevices.put(d);
			this.lastStatChangeTime = System.currentTimeMillis();
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private String getDstNames() {		
		StringBuilder dstNames = new StringBuilder();		
		for (int dstIndex=1 ; dstIndex <= ConfLoader.getInstance().getNumDestinations() ; ++dstIndex) {
			if (dstIndex == 1) {
				dstNames.append(getDstName(dstIndex));
			} else {
				dstNames.append(", ");
				dstNames.append(getDstName(dstIndex));
			}
		}
		return dstNames.toString();
	}

	private String getDstName(int dstIndex) {
		return ConfLoader.getInstance().getDstAlias(dstIndex) + " (" +  ConfLoader.getInstance().getDstTypeName(dstIndex) + ")";
	}

	private long getGlobalLatency() {
		Device lastQueuedDevice = this.lastQueuedDevice;
		if (lastQueuedDevice != null) {
			return lastQueuedDevice.getLatency();
		}
		return 0;
	}
	
	public long getTotalProcessedLogSize() {
		return this.totalProcessedLogSize.get();
	}
	
	public long getTotalProcessedOperCount() {
		return this.totalProcessedOperCount.get();
	}
	
	public long getTotalDstTxnCount() {
		return this.totalDstTxnCnt.get();
	}
	
	public void setDetectedDeviceCnt(long totalDeviceCnt) {
		this.detectedDeviceCnt.set(totalDeviceCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrDetectedDeviceCnt(long deviceCnt) {
		this.detectedDeviceCnt.addAndGet(deviceCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void setFailedDeviceCnt(long failedDeviceCnt) {
		this.failedDeviceCnt.set(failedDeviceCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void setTotalCommandLogSegmentCnt(long logSegmentCnt) {
		this.totalCommandLogSegmentCnt.set(Math.max(0, logSegmentCnt));
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrTotalCommandLogSegmentCnt(long deltaCommandLogSegmentCnt) {
		this.totalCommandLogSegmentCnt.addAndGet(deltaCommandLogSegmentCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrTotalProcessedLogSize(long size) {
		this.totalProcessedLogSize.addAndGet(size);
	}

	public void incrTotalProcessedOperCount(long cnt) {
		this.totalProcessedOperCount.addAndGet(cnt);
	}

	public void setTotalCDCLogSegmentCnt(long logSegmentCnt) {
		this.totalCDCLogSegmentCnt.set(Math.max(0, logSegmentCnt));
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrTotalCDCLogSegmentCnt(long deltaCDCLogSegmentCnt) {
		this.totalCDCLogSegmentCnt.addAndGet(Math.max(deltaCDCLogSegmentCnt, 0));
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void setTotalSyncLiteTxnCnt(long txnCnt) {
		this.totalSyncliteTxnCnt.set(txnCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrTotalSyncLiteTxnCnt(long deltaTxnCnt) {
		this.totalSyncliteTxnCnt.addAndGet(deltaTxnCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrTotalDstTxnCnt(long deltaDstTxnCnt) {
		this.totalDstTxnCnt.addAndGet(deltaDstTxnCnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrRegisteredDeviceCnt(long cnt) {
		this.registeredDeviceCnt.addAndGet(cnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrInitializedDeviceCnt(long cnt) {
		this.initializedDeviceCnt.addAndGet(cnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrInitializationCnt(long cnt) {
		this.totalInitializationCnt.addAndGet(cnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}

	public void incrResynchronizationCnt(long cnt) {
		this.totalResyncronizationCnt.addAndGet(cnt);
		this.lastStatChangeTime = System.currentTimeMillis();
	}


	private final String formatLatency(AtomicLong latency) {
		if ((latency.get() < 0) || (latency.get() == Long.MAX_VALUE)) {
			return "UNKNOWN";
		}
		return latency.toString();
	}

	public void resetTotalDstTxnCnt() {
		this.totalDstTxnCnt.set(this.totalSyncliteTxnCnt.get());
	}

	public void start() throws SyncLiteException {
		dumper.init();
		dumper.schedule();

		for (Dumper d : additionalDumpers) {
			d.init();
			d.schedule();
		}
	}

	public final List<Path> getDeviceUploadRootsFromStats() {    		
		Path statsFilePath = Path.of(ConfLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator_statistics.db");
		if (!Files.exists(statsFilePath)) {
			return Collections.emptyList();
		}
		List<Path> deviceUploadRoots = new ArrayList<Path>();
		String url = "jdbc:sqlite:" + statsFilePath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT path FROM device_status")) {
					while (rs.next()) {
						Path deviceUploadPath = Path.of(ConfLoader.getInstance().getDeviceUploadRoot().toString(), Path.of(rs.getString(1)).getFileName().toString());
						deviceUploadRoots.add(deviceUploadPath);
					}
				}
			}    			
		} catch (SQLException e) {
			tracer.error("Failed to retrieve device details from statistics file : ", e);
		}
		return deviceUploadRoots;
	}

	public final void deleteDeviceFromStats(Device d) throws SyncLiteException {
		Path statsFilePath = Path.of(ConfLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator_statistics.db");
		String url = "jdbc:sqlite:" + statsFilePath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("DELETE FROM device_status where synclite_device_id = '" + d.getDeviceUUID() + "' AND synclite_device_name = '" + d.getDeviceName() + "'");
			}
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to delete device : " + d + " from statistics file");		 
		}
	}
}
