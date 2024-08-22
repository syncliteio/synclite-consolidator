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

package com.synclite.consolidator.connector;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


public class TelemetryFileConnector {

	private static final class InstanceHolder {
		private static final TelemetryFileConnector INSTANCES[] = createTelemetryFileConnectos();

		
	    private static final TelemetryFileConnector[] createTelemetryFileConnectos() {    	
	    	TelemetryFileConnector INSTANCES[] = new TelemetryFileConnector[ConfLoader.getInstance().getNumDestinations() + 1];
	    	
	    	for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
	        	switch (ConfLoader.getInstance().getDstType(dstIndex)) {
	        	case APACHE_ICEBERG:        		
	        		INSTANCES[dstIndex] = new SparkTelemetryConnector(dstIndex);
	        		break;
	        	case DUCKDB:        		
	        		INSTANCES[dstIndex] = new DuckDBTelemetryConnector(dstIndex);
	        		break;
	        	case MYSQL:	
	        		INSTANCES[dstIndex] = new MySQLTelemetryConnector(dstIndex);
	        		break;
	        	case MONGODB:
	        		INSTANCES[dstIndex] = new MongoDBTelemetryConnector(dstIndex);
	        		break;
	        	default:
	        		INSTANCES[dstIndex] = new TelemetryFileConnector(dstIndex);
	        	}    		
	    	}
	    	return INSTANCES;
		}
	}

	protected int dstIndex;
	private ConcurrentHashMap<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>();


	private synchronized DataSource getDataSource(String connURL, int dstIndex) {
		DataSource dataSource = dataSources.get(connURL); 
		if(dataSource == null)
		{
			HikariConfig config = getConfig(connURL);
			dataSource = new HikariDataSource(config);
			dataSources.put(connURL, dataSource);
		}
		return dataSource;
	}    

    protected HikariConfig getConfig(String connURL) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(connURL);
		String user = ConfLoader.getInstance().getDstUser(dstIndex);
		String password = ConfLoader.getInstance().getDstPassword(dstIndex);
		if (user != null) {
			config.setUsername(user);
		}
		if (password != null) {
			config.setPassword(password);
		}
		config.setMaximumPoolSize(ConfLoader.getInstance().getNumDeviceProcessors() + 1);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		config.setAutoCommit(false);
		return config;
    }

	public void resetDataSource(String connURL) {
		DataSource dSrc = dataSources.get(connURL);
		if (dSrc != null) {
			((HikariDataSource) dSrc).close();
			dataSources.remove(connURL);
		}
	}

	protected TelemetryFileConnector(int dstIndex) {
		this.dstIndex = dstIndex;
	}

	public static TelemetryFileConnector getInstance(int dstIndex) {
		return InstanceHolder.INSTANCES[dstIndex];
	}

	public Connection connect(String connUrl) throws DstExecutionException {
		try {
			//return DriverManager.getConnection(PropsLoader.getInstance().getDstConnStr());
			return getDataSource(connUrl, dstIndex).getConnection();
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to connect to destination telemetry file with URL : " + connUrl , e);
		}
	}

}
