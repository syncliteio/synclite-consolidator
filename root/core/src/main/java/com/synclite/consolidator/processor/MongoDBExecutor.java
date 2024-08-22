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

package com.synclite.consolidator.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.connector.MongoDBConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.log.CDCLogPosition;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CopyColumn;
import com.synclite.consolidator.oper.CopyTable;
import com.synclite.consolidator.oper.CreateDatabase;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.NativeOper;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.Table;

public class MongoDBExecutor extends JDBCExecutor {

	MongoClient client;
	MongoDatabase db;
	MongoCollection<Document> currentCollection;
	String currentCollectionName;
	ClientSession session;
	Document currentSetDocument = new Document();
	Document currentWhereDocument = new Document();
	boolean currentCollectionHasFilterMapper = false;
    UpdateOptions upsertOptions = new UpdateOptions().upsert(true); // Perform upsert
    ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true); // Perform upsert
    List<InsertOneModel<Document>> insertModels = new ArrayList<>();
    List<WriteModel<Document>> updateModels = new ArrayList<>();
    List<WriteModel<Document>> replaceModels = new ArrayList<>();
    List<DeleteOneModel<Document>> deleteModels = new ArrayList<>();
    //List<ReplaceOneModel<Document>> replaceModels = new ArrayList<>();
    
	public MongoDBExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	protected final Connection connectToDst() throws DstExecutionException {
		MongoDBConnector conn = (MongoDBConnector) JDBCConnector.getInstance(dstIndex);
		client = conn.getClient();
		db = client.getDatabase(ConfLoader.getInstance().getDstDatabase(dstIndex));
		return null;
	}
	
	@Override
	protected void bindPrepared(PreparedStatement pstmt, int i, Object o, Column c, boolean isConditionArg) throws SQLException {
		try {
			if ((isConditionArg) && (!c.isSystemGeneratedTSColumn)) {
				if (c.type.dbNativeDataType.equalsIgnoreCase("ObjectId")) {
					ObjectId d = new ObjectId(o.toString());
					currentWhereDocument.append(c.column, d);
				} else {
					currentWhereDocument.append(c.column, o);
				}
			} else {
				if (c.type.dbNativeDataType.equalsIgnoreCase("BSON")) {
					//Unwrap json into individual key values
					//Check for filter mapper and if present create a new JSON document
					//Loop over all fields and append to set document.
					//
			        Document bsonObject = Document.parse(o.toString());
			        if (currentCollectionHasFilterMapper) {
			        	Document mappedDoc = new Document();
			        	for (Map.Entry<String, Object> entry : bsonObject.entrySet()) {
			        		String colName = entry.getKey();
			        		Object colVal = entry.getValue();
			        		String mappedColName = colName;
			        		Object mappedColVal = colVal;
							if (ConfLoader.getInstance().isAllowedColumn(dstIndex, currentCollectionName, colName)) {
								mappedColName = ConfLoader.getInstance().getMappedColumnName(dstIndex, currentCollectionName, colName);

								String valueToCheck = "null";
								if (colVal != null) {
									valueToCheck = colVal.toString();
								}
								mappedColVal = ConfLoader.getInstance().getMappedValue(dstIndex, currentCollectionName, colName, valueToCheck);
								if (mappedColVal == null) {
									mappedColVal = colVal;
								}
								mappedDoc.append(mappedColName, mappedColVal);
							}
			        	}
			        	currentSetDocument = mappedDoc;
					} else {
						currentSetDocument = bsonObject; 
					}
				} else {
					currentSetDocument.append(c.column, o);
				}
			}
		}catch (Exception e) {
			throw new SQLException("Failed to bind argument : " + e.getMessage(), e);
		}
	}
	
	@Override
    protected void addToInsertBatch() throws DstExecutionException {
		insertModels.add(new InsertOneModel<>(new Document(currentSetDocument)));
		currentSetDocument.clear();
	}

	@Override
	protected void bindUpsertArgs(Upsert oper) throws SQLException {
		int i = 1;
		for (Object o : oper.afterValues) {
			try {
				if (oper.tbl.columns.get(i-1).pkIndex > 0) {
					bindPrepared(null, i, o, oper.tbl.columns.get(i-1), true);
				} else {
					bindPrepared(null, i, o, oper.tbl.columns.get(i-1), false);
				}
			} catch (Exception e) {
				this.tracer.error("Failed to bind upsert arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for UPSERT for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index "+ i + " for UPSERT for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}
	}	

	@Override
	protected void bindReplaceArgs(Replace oper) throws SQLException {
		int i = 1;
		for (Object o : oper.afterValues) {
			try {
				if (oper.tbl.columns.get(i-1).pkIndex > 0) {
					bindPrepared(null, i, o, oper.tbl.columns.get(i-1), true);
				} else {
					bindPrepared(null, i, o, oper.tbl.columns.get(i-1), false);
				}
			} catch (Exception e) {
				this.tracer.error("Failed to bind replace arguments with exception : " + e.getMessage(), e); 
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for REPLACE for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index "+ i + " for REPLACE for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}
	}	

	@Override
    protected void addToUpsertBatch() throws DstExecutionException {
		if (currentWhereDocument.isEmpty()) {
			updateModels.add(new InsertOneModel<Document>(new Document(currentSetDocument)));
		} else {
			updateModels.add(new UpdateOneModel<Document>(new Document(currentWhereDocument), new Document("$set", new Document(currentSetDocument)), upsertOptions));
		}
		currentSetDocument.clear();
		currentWhereDocument.clear();
    }

    protected void addToReplaceBatch() throws DstExecutionException {		
		replaceModels.add(new ReplaceOneModel<Document>(new Document(currentWhereDocument), new Document(currentSetDocument), replaceOptions));		
		currentSetDocument.clear();
		currentWhereDocument.clear();
    }

	@Override
    protected void addToUpdateBatch() throws DstExecutionException {
		updateModels.add(new UpdateOneModel<Document>(new Document(currentWhereDocument), new Document("$set", new Document(currentSetDocument))));
		currentSetDocument.clear();
		currentWhereDocument.clear();
	}

	@Override
    protected void addToDeleteBatch() throws DstExecutionException {
		deleteModels.add(new DeleteOneModel<Document>(new Document(currentWhereDocument)));
		currentWhereDocument.clear();
	}

	@Override
    protected boolean isOperPrepared(Oper oper) throws DstExecutionException {
		switch (oper.operType) {
		case INSERT:
			return (!insertModels.isEmpty());
		case UPDATE:
			return (!updateModels.isEmpty());
		case DELETE:
			return (!deleteModels.isEmpty());
		case UPSERT:
			return (!updateModels.isEmpty());
		case REPLACE:
			return (!replaceModels.isEmpty());
		case DELETEINSERT:
			return ((!deleteModels.isEmpty()) && (!insertModels.isEmpty()));			
		default:
			return false;
		}
    }
    
	
	@Override
    protected final void resetBatch() throws DstExecutionException {
		this.insertModels.clear();
		this.updateModels.clear();
		this.deleteModels.clear();
		this.replaceModels.clear();
		this.batchOperCount = 0;
    }

	@Override
	protected void executeInsertBatch() throws DstExecutionException {
		try {
			BulkWriteResult result = currentCollection.bulkWrite(this.session, insertModels);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to bulk write insert batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeDeleteInsertBatch() throws DstExecutionException {
		try {
			try {
				BulkWriteResult result = currentCollection.bulkWrite(this.session, deleteModels);			
			} catch (Exception e) {
				throw new DstExecutionException("Failed to bulk write delete subbatch of deleteinsert batch : " + e.getMessage(), e);
			}
			try {
				BulkWriteResult result = currentCollection.bulkWrite(this.session, insertModels);
			} catch (Exception e) {
				throw new DstExecutionException("Failed to bulk write insert subbatch of deleteinsert batch : " + e.getMessage(), e);
			}
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute deleteinsert batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeUpsertBatch() throws DstExecutionException {
		try {
			BulkWriteResult result = currentCollection.bulkWrite(this.session, updateModels);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to bulk write upsert batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeReplaceBatch() throws DstExecutionException {
		try {
			BulkWriteResult result = currentCollection.bulkWrite(this.session, replaceModels);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to bulk write replace batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeUpdateBatch() throws DstExecutionException {
		try {
			BulkWriteResult result = currentCollection.bulkWrite(this.session, updateModels);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to bulk write update batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeDeleteBatch() throws DstExecutionException {
		try {
			BulkWriteResult result = currentCollection.bulkWrite(this.session, deleteModels);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to bulk write delete batch : " + e.getMessage(), e);
		}
	}

	protected final void prepareStatement(Table tbl) throws DstExecutionException {
		currentSetDocument.clear();
		currentWhereDocument.clear();
		this.currentCollectionName = tbl.id.table;
		this.currentCollection = db.getCollection(tbl.id.table);
		
		this.currentCollectionHasFilterMapper = false;
		String srcTable = ConfLoader.getInstance().getSrcTableFromDstTable(dstIndex, tbl.id.table);
		if (ConfLoader.getInstance().getDstEnableFilterMapperRules(dstIndex) || ConfLoader.getInstance().getDstEnableValueMapper(dstIndex)) {
			if (ConfLoader.getInstance().tableHasFilterMapperRules(dstIndex, srcTable) || ConfLoader.getInstance().tableHasValueMappings(dstIndex, srcTable)) {
				currentCollectionHasFilterMapper = true;
			}
		}
	}

	protected final void prepareInsertStatement(Insert oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	protected final void prepareDeleteInsertStatement(DeleteInsert oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	protected final void prepareUpsertStatement(Upsert oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	protected final void prepareReplaceStatement(Replace oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	protected final void prepareUpdateStatement(Update oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	protected final void prepareDeleteStatement(Delete oper) throws DstExecutionException {
		prepareStatement(oper.tbl);
	}

	@Override
	public boolean databaseExists(Table tbl) throws DstExecutionException {
		return true;
	}

	@Override
	public boolean schemaExists(Table tbl) throws DstExecutionException {
		return true;
	}

	@Override
	public boolean tableExists(Table tbl) throws DstExecutionException {
		return (db.getCollection(tbl.id.table) != null);
	}

	@Override
	public boolean columnExists(Table tbl, Column c) throws DstExecutionException {
		return true;
	}

	@Override
	public void close() throws DstExecutionException {
		//no op
	}

	@Override
	public void truncate(TruncateTable oper) throws DstExecutionException {
		try {
			MongoCollection<Document> collection = db.getCollection(oper.tbl.id.table);
	        collection.deleteMany(this.session, new Document());
		} catch (Exception e) {
			throw new DstExecutionException("Failed to truncate collection : " + e.getMessage(), e);
		}
	}

	@Override
	public void copyTable(CopyTable oper) throws DstExecutionException {
		try {
		      // Get source and destination collections
	        MongoCollection<Document> sourceCollection = db.getCollection(oper.copyFromTable.id.table);
	        MongoCollection<Document> destinationCollection = db.getCollection(oper.tbl.id.table);

	        // Copy documents from source to destination collection
	        MongoCursor<Document> cursor = sourceCollection.find().iterator();
	        try {
	            while (cursor.hasNext()) {
	                Document document = cursor.next();
	                destinationCollection.insertOne(this.session, document);
	            }
	        } finally {
	            cursor.close();
	        }		
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute copy table : " + e.getMessage(), e);
		}
	}

	@Override
	public void copyColumn(CopyColumn oper) throws DstExecutionException {
		try {
	        MongoCollection<Document> sourceCollection = db.getCollection(oper.tbl.id.table);
	        MongoCollection<Document> destinationCollection = db.getCollection(oper.tbl.id.table);

            // Iterate over documents in the source collection
            for (Document doc : sourceCollection.find()) {
                // Get the value of the desired attribute from the source document
                Object attributeValue = doc.get(oper.srcCol.column);

                // Create a filter for finding documents with the same attribute value
                Document filter = new Document(oper.dstCol.column, attributeValue);

                // Create a new document with only the desired attribute
                Document update = new Document("$set", new Document(oper.dstCol.column, attributeValue));

                // Perform an upsert operation
                destinationCollection.updateOne(this.session, filter, update, new UpdateOptions().upsert(true));
            }
	    } catch (Exception e) {
			throw new DstExecutionException("Failed to execute partial update : " + e.getMessage(), e);
		}
	}

	
	@Override
	public void beginTran() throws DstExecutionException {
		try {
			ClientSessionOptions options = ClientSessionOptions.builder().causallyConsistent(true).build();
			this.session = this.client.startSession(options);
			if (ConfLoader.getInstance().getDstMongoDBUseTransactions(dstIndex)) {
				TransactionOptions txnOptions = TransactionOptions.builder()
					    .readConcern(ReadConcern.SNAPSHOT)
					    .writeConcern(WriteConcern.MAJORITY)
					    .build();
				this.session.startTransaction(txnOptions);
			}
		} catch (Exception e) {
			throw new DstExecutionException("Failed to start a mongoclient session transaction : " + e.getMessage(), e);
		}
	}

	@Override
	protected boolean isOpenInsertBatch() {
		return (!insertModels.isEmpty());
	}

	@Override
	protected boolean isOpenUpdateBatch() {
		return (!updateModels.isEmpty());
	}

	@Override
	protected boolean isOpenDeleteBatch() {
		return (!deleteModels.isEmpty());
	}

	@Override
	protected boolean isOpenUpsertBatch() {
		return (!updateModels.isEmpty());
	}

	@Override
	protected boolean isOpenReplaceBatch() {
		return (!replaceModels.isEmpty());
	}

	@Override
	protected void doCommit() throws DstExecutionException {
		try {
			if (this.session != null) {
				if (ConfLoader.getInstance().getDstMongoDBUseTransactions(dstIndex)) {
					this.session.commitTransaction();
				}
			}
		} catch (Exception e) {
			throw new DstExecutionException("Failed to commit a mongoclient session transaction : " + e.getMessage(), e);
		}
		closeConn();
	}

	private final void closeConn() throws DstExecutionException {
		if (this.session != null) {
			this.session.close();
			this.session = null;
		}
	}

	@Override
	public void rollbackTran() throws DstExecutionException {
		try {
			if (this.session != null) {
				if (ConfLoader.getInstance().getDstMongoDBUseTransactions(dstIndex)) {
					tracer.debug("Rollback Transaction");
					this.session.abortTransaction();
				}
			}
		} catch (Exception e) {
			throw new DstExecutionException("Failed to rollback a mongoclient session transaction : " + e.getMessage(), e);
		}
		closeConn();
	}

	@Override
	public void createDatabase(CreateDatabase oper) throws DstExecutionException {
		try {
			client.getDatabase(oper.tbl.id.database);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to create and get database : " + e.getMessage(), e);
		}
	}

	@Override
	public void createSchema(CreateSchema oper) throws DstExecutionException {
		//No Op
	}

	@Override
	protected CDCLogPosition readCDCLogPosition(String deviceUUID, String deviceName, ConsolidatorDstTable dstControlTable) throws DstExecutionException {
		try {
			// Define the filter criteria
			Document filter = new Document("synclite_device_id", deviceUUID)
					.append("synclite_device_name", deviceName);

			// Execute the find operation with the filter
			MongoCollection<Document> collection = db.getCollection(dstControlTable.id.table);
			FindIterable<Document> documents = collection.find(filter);

			// Iterate over the matched documents
			for (Document doc : documents) {
				CDCLogPosition logPos = new CDCLogPosition(0, -1, -1, 0, 0);
				logPos.commitId = Long.valueOf(doc.get("commit_id").toString());
				logPos.changeNumber = Long.valueOf(doc.get("cdc_change_number").toString());
				logPos.txnChangeNumber = Long.valueOf(doc.get("cdc_txn_change_number").toString());
				logPos.logSegmentSequenceNumber = Long.valueOf(doc.get("cdc_log_segment_sequence_number").toString());
				logPos.txnCount = Long.valueOf(doc.get("txn_count").toString());
				return logPos;
			}
			throw new DstExecutionException("No checkpoint log position found in the destination");

		} catch (Exception e) {
			throw new DstExecutionException("Failed to read log positions from destination : " + e.getMessage(), e);
		}
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		if (e.getMessage().contains("E11000") &&  e.getMessage().contains("duplicate key error collection")) {
			return true;
		}
		return false;		
	}

	@Override
	public void executeSQL(DeleteIfPredicate sqlStmt) throws DstExecutionException {
		try {
			
			String[] tokens = sqlStmt.predicate.split("=");
			String colName = tokens[0].strip();
			String colVal = tokens[1].strip();
			//Remove quotes from the value if present
	        String regex = "^['\"]|['\"]$";
	        // Remove quotes using regex
	        colVal = colVal.replaceAll(regex, "");
	        
	        // Get a handle to the collection
            MongoCollection<Document> collection = db.getCollection(sqlStmt.tbl.id.table);

            // Define the filter to match documents where the attribute equals the given value
            Document filter = new Document(colName, colVal);

            // Perform the delete operation
            collection.deleteMany(this.session, filter);
			
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute deleteIfPredicate dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void executeSQL(NativeOper sqlStmt) throws DstExecutionException {		
		 // Parse the command string into a BSON Document
        Document command;
        String cmdTxt = sqlStmt.getSQL(sqlGenerator);
        try {
            command = Document.parse(cmdTxt);
        } catch (JsonParseException e) {
            throw new DstExecutionException("Invalid JSON command: " + cmdTxt + " : " + e.getMessage(), e);
        }

        // Execute the command
        try {
        	Document result = db.runCommand(command);
        } catch(Exception e) {
        	throw new DstExecutionException("Failed to excecute command : " + cmdTxt + " : " + e.getMessage(), e);
        }        
	}

	@Override
	public void executeSQL(Minus sqlStmt) throws DstExecutionException {		
		try {
			
			//TODO:
			//Tried hard to make this pipeline code work.
			//
			/*
			// Get handles to the collections
			MongoCollection<Document> collection1 = db.getCollection(sqlStmt.tbl.id.table);
			MongoCollection<Document> collection2 = db.getCollection(sqlStmt.rhsTable.id.table);

			// Define the compound key for the $lookup stage
			List<Document> compoundKey = new ArrayList<>();
			for (Column c : sqlStmt.rhsTable.columns) {
			    Document key = new Document();
			    key.append("localField", "$" + c.column);
			    key.append("foreignField", "$" + c.column);
			    compoundKey.add(key);
			}
			
            Bson lookupStage = new Document("$lookup",
                    new Document("from", sqlStmt.rhsTable.id.table)
                            .append("let", new Document("compoundKey", new Document("$concatArrays", compoundKey)))
                            .append("pipeline", Arrays.asList(
                                    new Document("$match", new Document("$expr", new Document("$eq", Arrays.asList("$compoundKey", "$$compoundKey"))))
                            ))
                            .append("as", "joined_docs")
            );

            // Define the $unwind stage to destructure the joined_docs array
            Bson unwindStage = new Document("$unwind", "$joined_docs");

            // Define the $match stage to filter documents where no matches were found in collection2
            Bson matchStage = new Document("$match", new Document("joined_docs", new Document("$exists", false)));

            // Define the $out stage to overwrite the existing collection1 with the filtered documents
            Bson outStage = new Document("$out", "collection1");

            // Execute the aggregation pipeline
			AggregateIterable<Document> result = collection1.aggregate(this.session, Arrays.asList(lookupStage, unwindStage, matchStage, outStage));

			// Iterate over the result to trigger the execution
			for (Document doc : result) {
			    // This loop is necessary to actually execute the aggregation pipeline
			}

			// Now, the filtered documents have been directly overwritten in collection1
			  
			*/
			
			// Get handles to the collections
			MongoCollection<Document> collection1 = db.getCollection(sqlStmt.tbl.id.table);
			MongoCollection<Document> collection2 = db.getCollection(sqlStmt.rhsTable.id.table);

			// Build the Document for $nin operator
			Document ninCriteria = new Document();
			for (Column c: sqlStmt.rhsTable.columns) {
				List<String> distinctValues = collection2.distinct(c.column, String.class).into(new ArrayList<>());
				ninCriteria.append(c.column, new Document("$nin", distinctValues));
			}

			// Delete the unmatched documents (be cautious!)
			DeleteResult deleteResult = collection1.deleteMany(this.session, ninCriteria);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute deleteIfPredicate dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void createTable(CreateTable oper) throws DstExecutionException {
		try {
            if (!db.listCollectionNames().into(new ArrayList<>()).contains(oper.tbl.id.table)) {
                //db.createCollection(oper.tbl.id.table);
                
                db.createCollection(this.session, oper.tbl.id.table);
                MongoCollection<Document> collection = db.getCollection(oper.tbl.id.table);
                //If PK exists then create a unique index 
                
                if (oper.tbl.hasPrimaryKey()) {
                    // Define the attributes on which to create the compound index
                	Document indexKeys = new Document();
                	for (Column c : oper.tbl.columns) {
	                    if (c.pkIndex > 0) {
	                    	indexKeys.append(c.column, 1);
	                    }
                	}
                	try {
	                    // Define options for the index (optional)
	                    IndexOptions indexOptions = new IndexOptions();
	                    indexOptions.unique(true); // Example: Make the index unique
	                    // Create the compound index
	                    collection.createIndex(this.session, indexKeys, indexOptions);
                	} catch (Exception e) {
                		//Ignore                		
                	}
                }                              
            }
		} catch (Exception e) {
			//Even after checking table exits , if we got a table already exists error then skip and move on
			if (isTableAlreadyExistsException(e)) {
				this.tracer.info("Table : " + oper.tbl.id + " found already existing");
				return;
			}				
			throw new DstExecutionException("Failed to execute createTable dst oper : " + e.getMessage(), e);
		}
	}

	protected boolean isTableAlreadyExistsException(Exception e) {
		return false;
	}


	@Override
	public void dropTable(DropTable oper) throws DstExecutionException {
		try {
            if (db.listCollectionNames().into(new ArrayList<>()).contains(oper.tbl.id.table)) {
            	MongoCollection<Document> collection =  db.getCollection(oper.tbl.id.table);
            	collection.drop(this.session);
            }
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute dropTable dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void renameTable(RenameTable oper) throws DstExecutionException {
		try {
            if (db.listCollectionNames().into(new ArrayList<>()).contains(oper.tbl.id.table)) {
                // Rename the collection
                db.runCommand(this.session, new Document("renameCollection", oper.oldTable.id.table)
                                         .append("to", oper.newTable.id.table));
                }
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute renameTable dst oper : " + e.getMessage(), e);
		}
	}

	@Override
	public void addColumn(AddColumn oper) throws DstExecutionException {
  /*      try {
			MongoCollection<Document> collection = db.getCollection(oper.tbl.id.table);
	
	        // Define the update operation to add the attribute to all documents
	        Document update = new Document("$set", new Document(oper.columns.get(0).column, ""));
	
	        // Perform the update operation
	        collection.updateMany(this.session, new Document(), update);
        } catch (Exception e) {
        	throw new DstExecutionException("Failed to execute addColumn dst oper : " + e.getMessage(), e);
        }*/
	}

	@Override
	public void alterColumn(AlterColumn oper) throws DstExecutionException {
		//No op
	}

	@Override
	public void dropColumn(DropColumn oper) throws DstExecutionException {
		try {
			MongoCollection<Document> collection = db.getCollection(oper.tbl.id.table);
		
			// Remove attribute from all documents
			Document update = new Document("$unset", new Document(oper.columns.get(0).column, ""));
			collection.updateMany(this.session, new Document(), update);
		} catch(Exception e) {
			throw new DstExecutionException("Failed to execute dropColumn dst oper : " + e.getMessage(), e);		
		}
	}

	@Override
	public void renameColumn(RenameColumn oper) throws DstExecutionException {
		try {
	        MongoCollection<Document> collection = db.getCollection(oper.tbl.id.table);

	        // Define the update operation to rename the attribute
	        Document query = new Document();
	        Document update = new Document("$rename", new Document(oper.oldName, oper.newName));

	        // Perform the update operation
	        collection.updateMany(this.session, query, update);
	    } catch (Exception e) {
			throw new DstExecutionException("Failed to execute renameColumn dst oper : " + e.getMessage(), e);
		}
	}

}
