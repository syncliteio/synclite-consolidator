package com.synclite.consolidator.connector;

import com.synclite.consolidator.global.ConfLoader;
import com.zaxxer.hikari.HikariConfig;

public class DuckDBJDBCConnector extends JDBCConnector {

	protected DuckDBJDBCConnector(int dstIndex) {
		super(dstIndex);
	}
	
	@Override
    protected HikariConfig getConfig() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(ConfLoader.getInstance().getDstConnStr(dstIndex));
		config.setMaximumPoolSize(ConfLoader.getInstance().getNumDeviceProcessors() + 1);
		config.setConnectionTimeout(ConfLoader.getInstance().getDstConnectionTimeoutS(dstIndex) * 1000);
		config.setAutoCommit(false);
		return config;
    }

}
