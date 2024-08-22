package com.synclite.consolidator.processor;

import java.util.Collections;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.oper.CreateDatabase;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;

public class DSTInitializer {

	public static void initialize(Logger tracer, int dstIndex) throws SyncLiteException{
		tracer.info("Initializing destination");
		//Create a mock table object
		Table mockTbl = new Table();

		mockTbl.id = TableID.from(null, null, 1, ConfLoader.getInstance().getDstDatabase(dstIndex), ConfLoader.getInstance().getDstSchema(dstIndex), "mock");
		try (SQLExecutor dstExecutor = SQLExecutor.getInstance(null, dstIndex, tracer)){
			if ( !dstExecutor.databaseExists(mockTbl)) {
				if (dstExecutor.canCreateDatabase()) {
					dstExecutor.createDatabase(new CreateDatabase(mockTbl));
				} else {
					throw new SyncLiteException("Specified database " + mockTbl.id.database + " does not exist");
				}
			}
		}

		try (SQLExecutor dstExecutor = SQLExecutor.getInstance(null, dstIndex, tracer)){
			//If syncMode is RPELICATION then do not create this single schema. 
			//Each device creates its own schema for REPLICATION
			if (ConfLoader.getInstance().getDstSyncMode() != DstSyncMode.REPLICATION) {
				dstExecutor.beginTran();
				dstExecutor.execute(Collections.singletonList(new CreateSchema(mockTbl)));
				dstExecutor.commitTran();
			}
			dstExecutor.initSetup();
		}

		tracer.info("Initialized destination");

	}
}
