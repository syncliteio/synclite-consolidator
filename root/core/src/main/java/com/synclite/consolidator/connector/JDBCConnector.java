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

import javax.sql.DataSource;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class JDBCConnector {

    private static final class InstanceHolder {
        private static final JDBCConnector[] INSTANCES = createJDBCConnectors();        
    }
    
    protected int dstIndex;
    private DataSource dataSource = null;
    
    private synchronized DataSource getDataSource()
    {
    	if(dataSource == null)
    	{
    		dataSource = new HikariDataSource(getConfig());
    	}
    	return dataSource;
    }

    protected HikariConfig getConfig() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(ConfLoader.getInstance().getDstConnStr(dstIndex));
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
		config.setConnectionTimeout(ConfLoader.getInstance().getDstConnectionTimeoutS(dstIndex) * 1000);
		config.setAutoCommit(false);
		return config;
    }
    
    private static final JDBCConnector[] createJDBCConnectors() {    	
    	JDBCConnector INSTANCES[] = new JDBCConnector[ConfLoader.getInstance().getNumDestinations() + 1];
    	
    	for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
        	switch (ConfLoader.getInstance().getDstType(dstIndex)) {
        	case APACHE_ICEBERG:
        		INSTANCES[dstIndex] = new SparkConnector(dstIndex);
        		break;
        	case FERRETDB:
        		INSTANCES[dstIndex] = new FerretDBConnector(dstIndex);
        		break;
        	case DUCKDB:        		
        		INSTANCES[dstIndex] = new DuckDBJDBCConnector(dstIndex);
        		break;
        	case MONGODB:
        		INSTANCES[dstIndex] = new MongoDBConnector(dstIndex);
        		break;
        	case MYSQL:	
        		INSTANCES[dstIndex] = new MySQLJDBCConnector(dstIndex);
        		break;
        	default:
        		INSTANCES[dstIndex] = new JDBCConnector(dstIndex);
        	}    		
    	}
    	return INSTANCES;
	}

	public void createPool() throws DstExecutionException
    {
    	try 
    	{
    		dataSource = getDataSource();
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to create a connection pool : ", e);
    	}    	
    }
    
    protected JDBCConnector(int dstIndex) {
    	this.dstIndex = dstIndex;
    }

    public static JDBCConnector getInstance(int dstIndex) {
        return InstanceHolder.INSTANCES[dstIndex];
    }

    public Connection connect() throws DstExecutionException {
        try {
            //return DriverManager.getConnection(PropsLoader.getInstance().getDstConnStr());
        	return getDataSource().getConnection();
        } catch (SQLException e) {
            throw new DstExecutionException("Failed to connect to destination database : ", e);
        }
    }
}
