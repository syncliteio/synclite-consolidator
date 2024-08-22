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

import java.io.StringReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;

public class SparkConnector extends JDBCConnector{

	private Builder sparkBuilder;
	//private SparkSession spark;
	protected SparkConnector(int dstIndex) {
		super(dstIndex);
		String sparkConfiguration = ConfLoader.getInstance().getDstSparkConfigurations(dstIndex);

		Properties props = new Properties();
		try {
			props.load(new StringReader(sparkConfiguration.replace("\\n", "\n")));
		} catch (Exception e) {
			throw new RuntimeException("Failed to load specified spark configuration : " + e.getMessage(), e);
		}		//Iterate on each line

		if (System.getProperty("os.name").startsWith("Windows")) {
			//Set hadoop home if windows			
			Path hadoopHome = ConfLoader.getInstance().getDeviceDataRoot().resolve("hadoopHome");
			System.setProperty("hadoop.home.dir", hadoopHome.toString());
		}

		try {
			sparkBuilder = SparkSession.builder()
					.appName("SyncLiteConsolidator")
					.master("local[*]");  // Run Spark in local mode

			for (String key : props.stringPropertyNames()) {
				String value = props.getProperty(key);
				sparkBuilder.config(key , value);
			}

			//this.spark = sparkBuilder.getOrCreate();
		} catch(Exception e) {
			throw new RuntimeException("Failed to create a spark session : " + e.getMessage(), e);
		}
	}

	public Connection connect() throws DstExecutionException {
		//throw new UnsupportedOperationException("connect not implemented for MongoDB");
		return null;
	}

	public SparkSession getSession() throws DstExecutionException {
		return sparkBuilder.getOrCreate();
		//return this.spark;
	}

}
