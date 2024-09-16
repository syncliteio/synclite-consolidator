package com.synclite.consolidator.connector;

import com.mongodb.client.MongoClients;
import com.synclite.consolidator.global.ConfLoader;

public class FerretDBConnector extends MongoDBConnector {

	protected FerretDBConnector(int dstIndex) {
		super(dstIndex);
        String connectionString = ConfLoader.getInstance().getDstConnStr(dstIndex);
		this.client = MongoClients.create(connectionString);
	}

}
