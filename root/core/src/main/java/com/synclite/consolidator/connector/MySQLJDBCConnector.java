package com.synclite.consolidator.connector;

import com.zaxxer.hikari.HikariConfig;

public class MySQLJDBCConnector extends JDBCConnector {

	protected MySQLJDBCConnector(int dstIndex) {
		super(dstIndex);
	}
	
	@Override
    protected HikariConfig getConfig() {
		HikariConfig config = super.getConfig();
		config.addDataSourceProperty("rewriteBatchedStatements", true);
		return config;
    }
}
