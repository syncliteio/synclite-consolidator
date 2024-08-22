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

package com.synclite.consolidator.global;

import java.nio.file.Path;

import com.synclite.consolidator.device.DeviceType;
import com.synclite.consolidator.schema.TableID;

public final class SyncLiteLoggerInfo {

    public static String getCommandLogSegmentSuffix() {
        return ".sqllog";
    }

    public static String getEventLogSegmentSuffix() {
        return ".sqllog";
    }

    public static Path getCommandLogSegmentPath(Path dbPath, String dbName, long dbID, long seqNum) {
        //return Path.of(getCommandLogSegmentPrefix(dbPath, dbName) + dbID + "." + seqNum);
    	return dbPath.resolve(seqNum + getCommandLogSegmentSuffix());
    }

    public static Path getEventLogSegmentPath(Path dbPath, String dbName, long dbID, long seqNum) {
        //return Path.of(getEventLogSegmentPrefix(dbPath, dbName) + dbID + "." + seqNum);
    	return dbPath.resolve(seqNum + getEventLogSegmentSuffix());
    }

    public static TableID getLoggerCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, "synclite_txn");
    }

    public static TableID getDBReaderCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, "synclite_dbreader_checkpoint");
    }

    public static TableID getLogReaderCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, "synclite_logreader_checkpoint");
    }

    public static Path getDataBackupPath(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), dbName + ".synclite.backup");
    }

    public static Path getCmdLogTxnFilePath(Path cmdLogSegmentPath, long commitID) {
    	return Path.of(cmdLogSegmentPath + "." + commitID + getCommandLogTxnFileSuffix());
    }

    public static Path getEventLogTxnFilePath(Path eventLogSegmentPath, long commitID) {
    	return Path.of(eventLogSegmentPath + "." + commitID + getEventLogTxnFileSuffix());
    }

	public static String getCommandLogTxnFileSuffix() {
		return ".txn";
	}

	public static String getEventLogTxnFileSuffix() {
		return ".txn";
	}

    public static String getMetadataFileSuffix() {
        return ".synclite.metadata";
    }
    public static String getControlTableName() {
        return "metadata";
    }

    public static String getDeviceDataRootPrefix() {
        return "synclite-";
    }

    public static String getDeviceCommandRootPrefix() {
        return "syncliteclient-";
    }

    public static final long nextPowerOf2(long n)
    {
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        n++;
        return n;
    }

    public final static String prepareArgList(long argCnt) {
        StringBuilder argListBuilder = new StringBuilder();
        for (long i = 1; i <= argCnt; ++i) {
            if (i > 1) {
                argListBuilder.append(", ");
            }
            argListBuilder.append("arg" + i);
        }
        return argListBuilder.toString();
    }

    public final static String preparePStmtFillerList(long argCnt) {
        StringBuilder argListBuilder = new StringBuilder();
        for (long i = 1; i <= argCnt; ++i) {
            if (i > 1) {
                argListBuilder.append(", ");
            }
            argListBuilder.append("?");
        }
        return argListBuilder.toString();
    }

	public static boolean isTxnFileForCmdLog(Path cmdLogSegmentPath, Path filePath) {
		String fName = filePath.getFileName().toString();
		if (fName.startsWith(cmdLogSegmentPath.getFileName().toString())) {
			if (fName.endsWith(getCommandLogTxnFileSuffix())) {
				return true;
			}
		}			
		return false;
	}

	public static boolean isTxnFileForEventLog(Path eventLogSegmentPath, Path filePath) {
		String fName = filePath.getFileName().toString();
		if (fName.startsWith(eventLogSegmentPath.getFileName().toString())) {
			if (fName.endsWith(getEventLogTxnFileSuffix())) {
				return true;
			}
		}			
		return false;
	}
	
    public static boolean isTransactionalDeviceType(DeviceType deviceType) {
    	switch (deviceType) {
    	case SQLITE:
    	case DUCKDB:
    	case DERBY:
    	case H2:
    	case HYPERSQL:
    		return true;
    	default:
    		return false;
    	}
	}

	public static boolean isAppenderDevice(DeviceType deviceType) {
    	switch (deviceType) {
    	case SQLITE_APPENDER:
    	case DUCKDB_APPENDER:
    	case DERBY_APPENDER:
    	case H2_APPENDER:
    	case HYPERSQL_APPENDER:
    		return true;
    	default:
    		return false;
    	}
	}
	

	public static boolean isStreamingDevice(DeviceType deviceType) {
    	switch (deviceType) {
    	case TELEMETRY:
    	case STREAMING:
    		return true;
    	default:
    		return false;
    	}
	}

}
