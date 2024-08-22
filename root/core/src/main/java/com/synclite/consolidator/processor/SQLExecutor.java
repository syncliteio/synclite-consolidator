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

import java.util.List;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
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
import com.synclite.consolidator.oper.FinishBatch;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
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
public abstract class SQLExecutor implements AutoCloseable{

    public static SQLExecutor getInstance(Device device, int dstIndex, Logger tracer) throws SyncLiteException {
        switch (ConfLoader.getInstance().getDstType(dstIndex)) {
        case APACHE_ICEBERG:
        	return new ApacheIcebergExecutor(device, dstIndex, tracer);
    	case SQLITE:
    		return new SQLiteExecutor(device, dstIndex, tracer);
        case POSTGRESQL:
        	return new PGExecutor(device, dstIndex, tracer);
        case MONGODB:
            return new MongoDBExecutor(device, dstIndex, tracer);
        case DUCKDB:
        	return new DuckDBExecutor(device, dstIndex, tracer);
        case MYSQL:
        	return new MySQLExecutor(device, dstIndex, tracer);
        default:
            throw new SyncLiteException("Unsupported dst type : " + ConfLoader.getInstance().getDstType(dstIndex));
        }
    }

    public abstract void insert(Insert oper) throws DstExecutionException;
	public abstract void deleteInsert(DeleteInsert deleteInsert) throws DstExecutionException;
    public abstract void upsert(Upsert oper) throws DstExecutionException;
	public abstract void replace(Replace replace) throws DstExecutionException;
    public abstract void update(Update oper) throws DstExecutionException;
    public abstract void delete(Delete oper) throws DstExecutionException;
    public abstract void truncate(TruncateTable oper) throws DstExecutionException;
	public abstract void loadFile(LoadFile oper) throws DstExecutionException;
    public abstract void createTable(CreateTable oper) throws DstExecutionException;
    public abstract void dropTable(DropTable oper) throws DstExecutionException;
    public abstract void renameTable(RenameTable oper) throws DstExecutionException;
	public abstract void alterColumn(AlterColumn alterColumn) throws DstExecutionException;
    public abstract void copyTable(CopyTable oper) throws DstExecutionException;
    public abstract void addColumn(AddColumn oper) throws DstExecutionException;
    public abstract void dropColumn(DropColumn oper) throws DstExecutionException;
    public abstract void renameColumn(RenameColumn oper) throws DstExecutionException;
    public abstract void copyColumn(CopyColumn oper) throws DstExecutionException;
    public abstract void createDatabase(CreateDatabase oper) throws DstExecutionException;
    public abstract void createSchema(CreateSchema createSchema) throws DstExecutionException;
	public abstract void executeSQL(DeleteIfPredicate sqlStmt) throws DstExecutionException;
	public abstract void executeSQL(Minus minusStmt) throws DstExecutionException;
	public abstract void executeSQL(FinishBatch finishBatch) throws DstExecutionException;
	public abstract void executeSQL(NativeOper nativeOper) throws DstExecutionException;	
	public abstract boolean databaseExists(Table tbl) throws DstExecutionException;
    public abstract boolean schemaExists(Table tbl) throws DstExecutionException;
    public abstract boolean tableExists(Table tbl) throws DstExecutionException;
    public abstract boolean columnExists(Table tbl, Column c) throws DstExecutionException;
    public abstract void beginTran() throws DstExecutionException;
    public abstract void commitTran() throws DstExecutionException;
    public abstract void rollbackTran() throws DstExecutionException;
    public abstract boolean isDuplicateKeyException(Exception e);
    public abstract boolean canCreateDatabase();
/*    public void closeTelemetryFile(Path telemetryFilePath) throws DstExecutionException {
    	throw new DstExecutionException("Destination type : " + ConfLoader.getInstance().getDstType() + " does not support Telemetry file destination");
    }
  */  
    public void execute(List<Oper> opers) throws DstExecutionException {
        for (Oper oper : opers) {
            oper.execute(this);
        }
    }

    public void execute(Oper oper) throws DstExecutionException {
       oper.execute(this);
    }

    protected abstract CDCLogPosition readCDCLogPosition(String deviceUUID, String deviceName, ConsolidatorDstTable dstCheckpointTable) throws DstExecutionException;


    @Override
    public void close() throws DstExecutionException {
    }
    
	protected void initSetup() throws DstExecutionException {
		//Nothing to be done in default implementation
	}


}
