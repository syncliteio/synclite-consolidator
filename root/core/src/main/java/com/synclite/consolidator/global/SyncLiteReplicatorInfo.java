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
