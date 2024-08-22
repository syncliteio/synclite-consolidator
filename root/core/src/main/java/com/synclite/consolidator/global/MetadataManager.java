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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataManager {
	protected Path metadataFilePath;
	protected static final ConcurrentHashMap<Path, MetadataManager> metadataMgrs= new ConcurrentHashMap<Path, MetadataManager>();

	protected MetadataManager(Path metadataPath) throws SQLException {
		this.metadataFilePath = metadataPath;
		initializeMetadataTable();
	}

	public Path getMetadataFilePath() {
		return this.metadataFilePath;
	}

	private void initializeMetadataTable() throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("CREATE TABLE IF NOT EXISTS metadata(key TEXT, value TEXT);");
			}
		}
	}

	public static MetadataManager getInstance(Path metadataFilePath) throws SQLException {
		if (metadataFilePath == null) {
			return null;
		}
		return metadataMgrs.computeIfAbsent(metadataFilePath, s -> {
			try {
				return new MetadataManager(s);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public final void upsertProperties(HashMap<String,Object> values) throws SQLException {
		for (long i = 0; i < ConfLoader.getInstance().getSyncLiteOperRetryCount(); ++i) {
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {				
					for (Map.Entry<String, Object> entry : values.entrySet()) {
						stmt.execute("DELETE FROM metadata WHERE key = '" + entry.getKey() + "'");
						stmt.execute("INSERT INTO metadata(key, value) VALUES('" + entry.getKey() + "', '" + entry.getValue().toString() + "')");
					}
				}
				conn.commit();
				break;
			} catch (SQLException e) {
				if (e.getMessage().startsWith("SQLITE_BUSY")) {
					if (i == (ConfLoader.getInstance().getSyncLiteOperRetryCount()-1)) {
						throw new SQLException("Metadata multiple property upsert failed after all retry attempts : ",  e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getSyncLiteOperRetryIntervalMs());
					} catch (InterruptedException e1) {
						Thread.interrupted();
					}
				} else {
					throw e;
				}
			}
		}		
	}

	public final void upsertProperty(String key, Object value) throws SQLException {
		for (long i = 0; i < ConfLoader.getInstance().getSyncLiteOperRetryCount(); ++i) {
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("DELETE FROM metadata WHERE key = '" + key + "'");
					stmt.execute("INSERT INTO metadata(key, value) VALUES('" + key + "', '" + value.toString() + "')");
				}
				conn.commit();
				break;
			} catch (SQLException e) {
				if (e.getMessage().startsWith("SQLITE_BUSY")) {
					if (i == (ConfLoader.getInstance().getSyncLiteOperRetryCount()-1)) {
						throw new SQLException("Metadata property upsert failed after all retry attempts : property name : " + key + ", property value : " + value, e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getSyncLiteOperRetryIntervalMs());
					} catch (InterruptedException e1) {
						Thread.interrupted();
					}
				} else {
					throw e;
				}
			}
		}
	}

	
	public final void deleteProperty(String key) throws SQLException {
		for (long i = 0; i < ConfLoader.getInstance().getSyncLiteOperRetryCount(); ++i) {
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("DELETE FROM metadata WHERE key = '" + key + "'");
				}
				conn.commit();
				break;
			} catch (SQLException e) {
				if (e.getMessage().startsWith("SQLITE_BUSY")) {
					if (i == (ConfLoader.getInstance().getSyncLiteOperRetryCount()-1)) {
						throw new SQLException("Metadata property deletion failed after all retry attempts : property name : " + key, e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getSyncLiteOperRetryIntervalMs());
					} catch (InterruptedException e1) {
						Thread.interrupted();
					}
				} else {
					throw e;
				}
			}
		}
	}


	public final String getStringProperty(String key) throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT value FROM metadata WHERE key = '" + key + "'")) {
					if (rs.next()) {
						return rs.getString(1);
					}
				}
			}
		}
		return null;

	}

	public final Long getLongProperty(String key) throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT value FROM metadata WHERE key = '" + key + "'")) {
					if (rs.next()) {
						return rs.getLong(1);
					}
				}
			}
		}
		return null;
	}

}
