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
