package com.synclite.consolidator.global;

import java.nio.file.Path;

import com.synclite.consolidator.schema.TableID;

public class SyncLiteConsolidatorInfo {

    public static TableID getCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, getSyncLiteMetadataTableName());
    }

    public static String getSyncLiteMetadataTableName() {
    	return "synclite_metadata";
    }
    
    public static String getMetadataFileName(int dstIndex) {
        return "synclite_consolidator_metadata_" + dstIndex + ".db";
    }
    
    public static Path getStatsFilePath(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), "synclite_device_statistics.db");
    }
    
    public static String getSnapshotPathPrefix(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), dbName + ".snapshot.").toString();
    }

    public static Path getNextSnapshotPath(Path dbPath, String dbName, long ts) {
        return Path.of(getSnapshotPathPrefix(dbPath, dbName) + ts);
    }


    public static Path getDeviceReplicaPath(Path dbPath, String dbName, int dstIndex) {
        return Path.of(dbPath.toString(), dbName + ".synclite.backup");
    }

}
