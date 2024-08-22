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

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;
import com.synclite.consolidator.log.CDCLogSegment;
import com.synclite.consolidator.log.EventLogSegment;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.stage.SyncLiteObjectType;

public class DeviceLogCleaner {

	private Device device;
	private long cleanedUpto = -1L;
	private static final ConcurrentHashMap<Device, DeviceLogCleaner> logCleaners = new ConcurrentHashMap<Device, DeviceLogCleaner>();
	private static DeviceStageManager deviceStageManager = DeviceStageManager.getDataStageManagerInstance();
	private DeviceLogCleaner(Device device) throws SyncLiteException {
		this.device = device;
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
			markAndcleanUpTxnLogsUpto(device.getLastConsolidatedLogSegmentSequenceNumber());
		} else {
			markAndCleanUpTelemetryLogsUpto(device.getLastConsolidatedLogSegmentSequenceNumber());
		}
		this.cleanedUpto = lastConsolidatedLogNum;
	}
	
	private void markAndcleanUpTxnLogsUpto(long appliedLogSegmentSeqNum) throws SyncLiteException {
		CDCLogSegment seg = device.getCDCLogSegment(appliedLogSegmentSeqNum);
		cleanUpTxnDeviceLogs(appliedLogSegmentSeqNum - 1);
		if (seg != null) {
			seg.markApplied();
		}		
	}

	private void markAndCleanUpTelemetryLogsUpto(long appliedLogSegmentSeqNum) throws SyncLiteException {		
		EventLogSegment seg = device.getEventLogSegment(appliedLogSegmentSeqNum);
		cleanUpTelemetryDeviceLogs(appliedLogSegmentSeqNum - 1);
		if (seg != null) {
			seg.markApplied();
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
			try {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = Files.walk(cmdLogSegmentPath.getParent()).filter(s->SyncLiteLoggerInfo.isTxnFileForCmdLog(cmdLogSegmentPath, s)).collect(Collectors.toList());
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
			try {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = Files.walk(cmdLogSegmentPath.getParent()).filter(s->SyncLiteLoggerInfo.isTxnFileForCmdLog(cmdLogSegmentPath, s)).collect(Collectors.toList());

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
			try {
				//Get a list of all txn files for this log segment first
				List<Path> txnFiles = Files.walk(eventLogSegmentPath.getParent()).filter(s->SyncLiteLoggerInfo.isTxnFileForEventLog(eventLogSegmentPath, s)).collect(Collectors.toList());
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
	}
}
