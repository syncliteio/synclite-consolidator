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

package com.synclite.consolidator.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;

public class LogSegment {
    public long sequenceNumber;
    public Path path;
    private boolean isApplied;
    private boolean isReadyToApply;
    protected Device device;
    protected long size;

    private static final String deleteMetadataTableEntrySql = "DELETE FROM metadata WHERE key = '$1'";
    private static final String insertMetadataTableEntrySql = "INSERT INTO metadata(key, value) VALUES('$1', '$2')";

    public LogSegment(Device device, long sequenceNumber, Path path) {
        this.sequenceNumber = sequenceNumber;
        this.path = path;
        this.isApplied = false;
        this.isReadyToApply = false;
        this.device = device;
    }

    @Override
    public String toString() {
        return "Sequence Number : " + sequenceNumber + ", Path : " + path;
    }

    public boolean isApplied() {
    	if (this.isApplied) {
    		return true;
    	}
        String url = "jdbc:sqlite:" + this.path;
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT value FROM metadata WHERE key = 'status'")) {
                    if (rs.next()) {
                        String value = rs.getString(1);
                        if (value.equalsIgnoreCase(LogSegmentStatus.APPLIED.toString())) {
                        	this.isApplied = true;                            
                        }                      
                    }
                }
            }
        } catch (SQLException e) {
            //throw new SyncLiteException("Failed to check cdc log segment closed : " + this.path + " with exception : ", e);
            return this.isApplied;
        }
        return this.isApplied;
    }

    public void markReadyToApply() throws SyncLiteException {
    	try {
    		upsertMetadataEntry("status", LogSegmentStatus.READY_TO_APPLY.toString());
    		this.isReadyToApply = true;
    	} catch (SQLException e) {
    		throw new SyncLiteException("Failed to mark " + LogSegmentStatus.READY_TO_APPLY.toString() + " log legment : " + this.path + " with exception : ", e);
    	}
    }

    public void markApplied() throws SyncLiteException {
    	try {
    		upsertMetadataEntry("status", LogSegmentStatus.APPLIED.toString());
    		this.isApplied = true;
    	} catch (SQLException e) {
    		throw new SyncLiteException("Failed to mark " + LogSegmentStatus.APPLIED.toString() + " log legment : " + this.path + " with exception : ", e);
    	}
    }

    public final boolean isReadyToApply() throws SyncLiteException {
    	if (this.isReadyToApply) {
    		return true;
    	}
        String url = "jdbc:sqlite:" + this.path;
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT value FROM metadata WHERE key = 'status'")) {
                    if (rs.next()) {
                        String value = rs.getString(1);
                        if (value.equalsIgnoreCase(LogSegmentStatus.READY_TO_APPLY.toString())) {
                        	this.isReadyToApply = true;                            
                        }
                    }
                }
            }
        } catch (SQLException e) {
            //throw new SyncLiteException("Failed to check cdc log segment closed : " + this.path + " with exception : ", e);
            return this.isReadyToApply;
        }
        return this.isReadyToApply;
    }

    
    public final void upsertMetadataEntry(String key, String value) throws SQLException {
        String url = "jdbc:sqlite:" + this.path;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(deleteMetadataTableEntrySql.replace("$1", key));
                stmt.execute(insertMetadataTableEntrySql.replace("$1", key).replace("$2", value));
                conn.commit();
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to upsert a metadata entry : <" + key + "," + value + "> in metadata table in log segment : " + this.path, e);
        }
    }

    public final long getSize() throws SyncLiteException {
    	try {
			return Files.size(this.path);
		} catch (IOException e) {			
			throw new SyncLiteException("Failed to get file size for log segment : " + this.path);
		}
    }
}
