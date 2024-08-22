package com.synclite.consolidator.connector;

import com.zaxxer.hikari.HikariConfig;

public class MySQLTelemetryConnector extends TelemetryFileConnector {

	protected MySQLTelemetryConnector(int dstIndex) {
		super(dstIndex);
	}
	
	@Override
    protected HikariConfig getConfig(String connURL) {
		HikariConfig hc = super.getConfig(connURL);
		hc.addDataSourceProperty("rewriteBatchedStatements", true);
		return hc;
    }
}
