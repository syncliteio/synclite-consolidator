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