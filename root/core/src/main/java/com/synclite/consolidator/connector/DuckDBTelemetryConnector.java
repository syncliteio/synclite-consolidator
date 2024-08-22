package com.synclite.consolidator.connector;

import com.synclite.consolidator.global.ConfLoader;
import com.zaxxer.hikari.HikariConfig;

public class DuckDBTelemetryConnector extends TelemetryFileConnector {

	protected DuckDBTelemetryConnector(int dstIndex) {
		super(dstIndex);
	}
	
	@Override
    protected HikariConfig getConfig(String connURL) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(connURL);
		config.setMaximumPoolSize(ConfLoader.getInstance().getNumDeviceProcessors() + 1);
		return config;
    }

}
