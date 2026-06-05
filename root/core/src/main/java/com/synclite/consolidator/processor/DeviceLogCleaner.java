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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.log.CDCLogSegment;
import com.synclite.consolidator.log.EventLogSegment;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.stage.SyncLiteObjectType;

public class DeviceLogCleaner {

	private static final String LAST_CLEANED_KEY = "last_cleaned_log_segment_seq_num";

	private Device device;
	private long cleanedUpto = -1L;
	private static final ConcurrentHashMap<Device, DeviceLogCleaner> logCleaners = new ConcurrentHashMap<Device, DeviceLogCleaner>();
	private static DeviceStageManager deviceStageManager = DeviceStageManager.getDataStageManagerInstance();
	private DeviceLogCleaner(Device device) throws SyncLiteException {
		this.device = device;
		// Persist + restore the cleanup watermark so a consolidator restart
		// doesn't re-walk every segment from 0 just to discover they were
		// already deleted. The value lives in the per-device metadata DB
		// alongside other device-scoped keys (`status`, `database_name`,
		// ...). Best-effort: a missing/corrupt value falls back to -1
		// which preserves the original behavior.
		try {
			if (device.getDeviceMetadataMgr() != null) {
				Long persisted = device.getDeviceMetadataMgr().getLongProperty(LAST_CLEANED_KEY);
				if (persisted != null && persisted >= 0L) {
					this.cleanedUpto = persisted;
				}
			}
		} catch (Throwable t) {
			// Persistence is a cache; if it fails we silently fall back to
			// the original Java behavior (re-walk from -1). The cleaner must
			// never refuse to construct on a metadata-DB hiccup.
			try {
				device.tracer.warn("Failed to read " + LAST_CLEANED_KEY + " from device metadata; starting cleanup watermark at -1", t);
			} catch (Throwable ignored) {
			}
		}
	}

	public static DeviceLogCleaner getInstance(Device device) {
		if (device == null) {
			return null;
		}
		return logCleaners.computeIfAbsent(device, s -> {
			try {
				return new DeviceLogCleaner(s);
			} catch (SyncLiteException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public void markAppliedAndCleanUp() throws SyncLiteException {
		long lastConsolidatedLogNum = device.getLastConsolidatedLogSegmentSequenceNumber();
		if (lastConsolidatedLogNum <= cleanedUpto) {
			return;
		}
		if (SyncLiteLoggerInfo.isTransactionalDeviceType(device.getDeviceType())) {
			markAndcleanUpTxnLogsUpto(lastConsolidatedLogNum);
		} else {
			markAndCleanUpTelemetryLogsUpto(lastConsolidatedLogNum);
		}
	}
	
	private void markAndcleanUpTxnLogsUpto(long appliedLogSegmentSeqNum) throws SyncLiteException {
		CDCLogSegment seg = device.getCDCLogSegment(appliedLogSegmentSeqNum);
		if (seg != null) {
			seg.markApplied();
		}
		bestEffortCleanUpTxnDeviceLogsUpto(appliedLogSegmentSeqNum - 1);
	}

	private void markAndCleanUpTelemetryLogsUpto(long appliedLogSegmentSeqNum) throws SyncLiteException {		
		EventLogSegment seg = device.getEventLogSegment(appliedLogSegmentSeqNum);
		if (seg != null) {
			seg.markApplied();
		}
		bestEffortCleanUpTelemetryDeviceLogsUpto(appliedLogSegmentSeqNum - 1);
	}

	private void bestEffortCleanUpTxnDeviceLogsUpto(long targetSeqNum) {
		if (targetSeqNum < 0) {
			return;
		}

		long nextContiguousCleaned = this.cleanedUpto;
		for (long seqNum = this.cleanedUpto + 1; seqNum <= targetSeqNum; ++seqNum) {
			try {
				cleanUpTxnDeviceLogs(seqNum);
				if (seqNum == (nextContiguousCleaned + 1)) {
					nextContiguousCleaned = seqNum;
				}
			} catch (Exception e) {
				device.tracer.warn("Cleanup failed for transactional log segment : " + seqNum + ", will retry in next cleanup cycle", e);
			}
		}
		advanceCleanedUpto(nextContiguousCleaned);
	}

	private void bestEffortCleanUpTelemetryDeviceLogsUpto(long targetSeqNum) {
		if (targetSeqNum < 0) {
			return;
		}

		long nextContiguousCleaned = this.cleanedUpto;
		for (long seqNum = this.cleanedUpto + 1; seqNum <= targetSeqNum; ++seqNum) {
			try {
				cleanUpTelemetryDeviceLogs(seqNum);
				if (seqNum == (nextContiguousCleaned + 1)) {
					nextContiguousCleaned = seqNum;
				}
			} catch (Exception e) {
				device.tracer.warn("Cleanup failed for telemetry log segment : " + seqNum + ", will retry in next cleanup cycle", e);
			}
		}
		advanceCleanedUpto(nextContiguousCleaned);
	}

	private void advanceCleanedUpto(long newWatermark) {
		if (newWatermark <= this.cleanedUpto) {
			return;
		}
		this.cleanedUpto = newWatermark;
		// Persisting the watermark is a best-effort optimization; a
		// failure here must never abort cleanup (the in-memory value is
		// already advanced, so subsequent passes still skip the cleaned
		// range while the process lives; restart-after-failure simply
		// re-walks the already-deleted range, which is harmless).
		try {
			if (device.getDeviceMetadataMgr() != null) {
				device.getDeviceMetadataMgr().upsertProperty(LAST_CLEANED_KEY, newWatermark);
			}
		} catch (Throwable t) {
			try {
				device.tracer.warn("Failed to persist " + LAST_CLEANED_KEY + "=" + newWatermark + "; will retry on next cleanup cycle", t);
			} catch (Throwable ignored) {
			}
		}
	}

	private void cleanUpTxnDeviceLogs(long logSegmentSeqNumber) throws SyncLiteException {
		if (logSegmentSeqNumber < 0) {
			return;
		}
		
		Path cdcLogSegmentPath = device.getCDCLogSegmentPath(logSegmentSeqNumber);
		try {
			if (Files.exists(cdcLogSegmentPath)) {
				Files.delete(cdcLogSegmentPath);
			}
		} catch (IOException e) {
			throw new SyncLiteException("Failed to delete CDC log segment at path : " + cdcLogSegmentPath , e);
		}
		
		Path cmdLogSegmentPath = device.getCommandLogSegmentPath(logSegmentSeqNumber);
		Path cmdLogSegmentPathInUploadDir = device.getCommandLogSegmentPathInUploadDir(logSegmentSeqNumber);
		
		//
		//Delete txn log files first 
		//
		if (device.allowsConcurrentWriters()) {
			try (Stream<Path> stream = Files.walk(cmdLogSegmentPath.getParent())) {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = stream.filter(s->SyncLiteLoggerInfo.isTxnFileForCmdLog(cmdLogSegmentPath, s)).collect(Collectors.toList());
				//Delete each from stage first and then from local
				for (Path f : txnFiles) {
					Path txnFilePathInUpload = cmdLogSegmentPathInUploadDir.getParent().resolve(f.getFileName().toString());
					deviceStageManager.deleteObject(txnFilePathInUpload, SyncLiteObjectType.LOG);

					Files.delete(f);	
				}
			} catch (IOException e) {
				throw new SyncLiteException("Failed to delete txn files for command log segment at path : " + cmdLogSegmentPath + " : " + e.getMessage(), e);
			}
		}

		
		try {
			if (Files.exists(cmdLogSegmentPath)) {
				Files.delete(cmdLogSegmentPath);
			}
		} catch (IOException e) {
			throw new SyncLiteException("Failed to delete command log segment at path : " + cmdLogSegmentPath , e);
		}		

		try {
			deviceStageManager.deleteObject(cmdLogSegmentPathInUploadDir, SyncLiteObjectType.LOG);
		} catch (SyncLiteStageException e) {
			throw new SyncLiteException("Failed to delete command log segment : " + cmdLogSegmentPath + " from device stage ", e);
		}		
		device.tracer.info("Cleaned processed segment : seqNum=" + logSegmentSeqNumber + " cdclogPath=" + cdcLogSegmentPath + " cmdlogPath=" + cmdLogSegmentPath);
	}

	//Method specifically for replication to SQLite case
	protected void cleanUpCommandLogs(long logSegmentSeqNumber ) throws SyncLiteException {
		if (logSegmentSeqNumber < 0) {
			return;
		}	

		Path cmdLogSegmentPath = device.getCommandLogSegmentPath(logSegmentSeqNumber);
		Path cmdLogSegmentPathInUploadDir = device.getCommandLogSegmentPathInUploadDir(logSegmentSeqNumber);

		//
		//Delete txn log files first 
		//
		if (device.allowsConcurrentWriters()) {
			try (Stream<Path> stream = Files.walk(cmdLogSegmentPath.getParent())) {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = stream.filter(s->SyncLiteLoggerInfo.isTxnFileForCmdLog(cmdLogSegmentPath, s)).collect(Collectors.toList());

				//Delete each from stage first and then from local

				for (Path f : txnFiles) {

					Path txnFilePathInUpload = cmdLogSegmentPathInUploadDir.getParent().resolve(f.getFileName().toString());
					deviceStageManager.deleteObject(txnFilePathInUpload, SyncLiteObjectType.LOG);

					Files.delete(f);	
				}
			} catch (IOException e) {
				throw new SyncLiteException("Failed to delete txn files for command log segment at path : " + cmdLogSegmentPath + " : " + e.getMessage(), e);
			}
		}
		
		try {
			if (Files.exists(cmdLogSegmentPath)) {
				Files.delete(cmdLogSegmentPath);
			}
		} catch (IOException e) {
			throw new SyncLiteException("Failed to delete command log segment at path : " + cmdLogSegmentPath , e);
		}		

		try {
			deviceStageManager.deleteObject(cmdLogSegmentPathInUploadDir, SyncLiteObjectType.LOG);
		} catch (SyncLiteStageException e) {
			throw new SyncLiteException("Failed to delete command log segment : " + cmdLogSegmentPath + " from device stage ", e);
		}		
		device.tracer.info("Cleaned processed command log segment : seqNum=" + logSegmentSeqNumber + " cmdlogPath=" + cmdLogSegmentPath);
	}

	private void cleanUpTelemetryDeviceLogs(long logSegmentSeqNumber) throws SyncLiteException {
		if (logSegmentSeqNumber < 0) {
			return;
		}		

		Path eventLogSegmentPath = device.getEventLogSegmentPath(logSegmentSeqNumber);
		
		//Delete data files corresponding to this event log segment first		
		Path eventLogSegmentDataDirPath = Path.of(eventLogSegmentPath + ".data");
		Path deviceUploadRoot = device.getDeviceUploadRoot();
		try {
			if (Files.exists(eventLogSegmentDataDirPath) && Files.isDirectory(eventLogSegmentDataDirPath)) {
				for (File f : eventLogSegmentDataDirPath.toFile().listFiles()) {
					//Delete this file and delete corresponding file from its upload root. 
					Path dataFileInUpload = Path.of(deviceUploadRoot.toString(), f.toPath().getFileName().toString());
					deviceStageManager.deleteObject(dataFileInUpload, SyncLiteObjectType.LOG);
					
					if (Files.exists(f.toPath())) {
						Files.delete(f.toPath());
					}
				}				
				//Finally delete the data file directory
				if (Files.exists(eventLogSegmentDataDirPath)) {
					Files.delete(eventLogSegmentDataDirPath);
				}
			}			
		} catch (SyncLiteStageException | IOException e) {
			throw new SyncLiteException("Failed to delete data files for event log segment at path : " + eventLogSegmentPath, e);
		}
		

		Path eventLogSegmentPathInUploadDir = device.getEventLogSegmentPathInUploadDir(logSegmentSeqNumber);
		
		//
		//Delete txn log files first 
		//
		if (device.allowsConcurrentWriters()) {
			try (Stream<Path> stream = Files.walk(eventLogSegmentPath.getParent())) {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = stream.filter(s->SyncLiteLoggerInfo.isTxnFileForEventLog(eventLogSegmentPath, s)).collect(Collectors.toList());
				//Delete each from stage first and then from local
				for (Path f : txnFiles) {
					Path txnFilePathInUpload = eventLogSegmentPathInUploadDir.getParent().resolve(f.getFileName().toString());
					deviceStageManager.deleteObject(txnFilePathInUpload, SyncLiteObjectType.LOG);

					Files.delete(f);	
				}
			} catch (IOException e) {
				throw new SyncLiteException("Failed to delete txn files for event log segment at path : " + eventLogSegmentPath + " : " + e.getMessage(), e);
			}
		}

		try {
			if (Files.exists(eventLogSegmentPath)) {
				Files.delete(eventLogSegmentPath);
			}
		} catch (IOException e) {
			throw new SyncLiteException("Failed to delete event log segment at path : " + eventLogSegmentPath , e);
		}		

		try {
			deviceStageManager.deleteObject(eventLogSegmentPathInUploadDir, SyncLiteObjectType.LOG);
		} catch (SyncLiteStageException e) {
			throw new SyncLiteException("Failed to delete event log segment : " + eventLogSegmentPath  + " from device stage ", e);
		}
		device.tracer.info("Cleaned processed event log segment : seqNum=" + logSegmentSeqNumber + " eventlogPath=" + eventLogSegmentPath);
	}
}
