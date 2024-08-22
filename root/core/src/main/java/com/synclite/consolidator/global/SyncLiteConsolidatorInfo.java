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
