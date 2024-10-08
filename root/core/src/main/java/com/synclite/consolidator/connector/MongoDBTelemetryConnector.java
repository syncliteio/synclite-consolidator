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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;

public class MongoDBTelemetryConnector extends TelemetryFileConnector {
	protected MongoDBTelemetryConnector(int dstIndex) {
		super(dstIndex);
		
        String connectionString = ConfLoader.getInstance().getDstConnStr(dstIndex);
		/*
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();
        
		this.client = MongoClients.create(settings);
		*/
		this.client = MongoClients.create(connectionString);
	}
    
    protected MongoClient client;

    public Connection connect() throws DstExecutionException {
    	//throw new UnsupportedOperationException("connect not implemented for MongoDB");
    	client.startSession();
    	return null;
    }

    public MongoClient getClient() throws DstExecutionException {
    	return client;
    }
}
