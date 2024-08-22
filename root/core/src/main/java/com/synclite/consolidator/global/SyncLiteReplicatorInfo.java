package com.synclite.consolidator.global;

import java.nio.file.Path;

public class SyncLiteReplicatorInfo {

    public static String getCDCLogSegmentPrefix() {
        return ".cdclog";
    }

    public static Path getCDCLogSegmentPath(Path dbPath, String dbName, long dbID, long seqNum) {
        //return Path.of(getCDCLogSegmentPrefix(dbPath, dbName) + dbID + "." + seqNum);
    	return dbPath.resolve(seqNum + getCDCLogSegmentPrefix());
    }

    public static String getMetadataFileName() {
        return "synclite_replicator_metadata.db";
    }

	public static Path getDataBackupSnapshotPath(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), dbName + ".synclite.snapshot");
	}

	public static Path getReplicaPath(Path dbPath, String dbName, int dstIndex) {
        return Path.of(dbPath.toString(), dbName + "."  + dstIndex + ".synclite.backup");
	}
}
