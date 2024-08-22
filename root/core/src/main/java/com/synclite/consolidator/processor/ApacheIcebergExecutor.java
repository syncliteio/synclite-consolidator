package com.synclite.consolidator.processor;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.connector.SparkConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.log.CDCLogPosition;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;

public class ApacheIcebergExecutor extends JDBCExecutor {

	SparkSession spark;
	Table currentTable;
	List<Row> deleteRows = new ArrayList<Row>();
	List<Row> insertRows = new ArrayList<Row>();
	List<Row> updateRows = new ArrayList<Row>();
	StructType currentTableSchema = null;	

	public ApacheIcebergExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	protected final Connection connectToDst() throws DstExecutionException {
		SparkConnector conn = (SparkConnector) JDBCConnector.getInstance(dstIndex);
		spark = conn.getSession();
		return null;
	}

	@Override
	protected void bindPrepared(PreparedStatement pstmt, int i, Object o, Column c, boolean isConditionArg) throws SQLException {
		//NoOp
	}

	@Override
	protected void addToInsertBatch() throws DstExecutionException {
		//NoOp
	}

	@Override
	protected void bindInsertArgs(Insert oper) throws SQLException {
		int i = 1;
		Object[] icebergVals = new Object[oper.afterValues.size()];
		for (Object o : oper.afterValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for INSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index " + i + " for INSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}		
		Row insertRow = RowFactory.create(icebergVals);
		insertRows.add(insertRow);
	}	

	@Override
	protected void bindUpdateArgs(Update oper) throws SQLException {
		int i = 1;
		Object[] icebergVals = new Object[oper.beforeValues.size() + oper.afterValues.size()];
		for (Object o : oper.beforeValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind before argument with value " + o + " at index " + i + " for UPDATE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind before argument with value " + o + " at index " + i + " for UPDATE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}	

		int j = 1;
		for (Object o : oper.beforeValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(j-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind after argument with value " + o + " at index " + j + " for UPDATE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind after argument with value " + o + " at index " + j + " for UPDATE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++j;
			++i;
		}	
				
		Row updateRow = RowFactory.create(icebergVals);
		updateRows.add(updateRow);
	}
	
	@Override
	protected void bindDeleteArgs(Delete oper) throws SQLException {
		int i = 1;
		Object[] icebergVals = new Object[oper.beforeValues.size()];
		for (Object o : oper.beforeValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for DELETE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index " + i + " for DELETE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}		
		Row deleteRow = RowFactory.create(icebergVals);
		deleteRows.add(deleteRow);
	}

	@Override
	protected void bindUpsertArgs(Upsert oper) throws SQLException {
		int i = 1;
		Object[] icebergVals = new Object[oper.afterValues.size()];
		for (Object o : oper.afterValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for UPSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index " + i + " for UPSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}		
		Row insertRow = RowFactory.create(icebergVals);
		insertRows.add(insertRow);
	}	

	@Override
	protected void bindReplaceArgs(Replace oper) throws SQLException {
		int i = 1;
		Object[] icebergVals = new Object[oper.afterValues.size()];
		for (Object o : oper.afterValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for REPLACE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new SQLException("Failed to bind argument with value " + o + " at index " + i + " for REPLACE operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}		
		Row insertRow = RowFactory.create(icebergVals);
		insertRows.add(insertRow);
	}	

	@Override
	protected void bindDeleteInsertArgs(DeleteInsert oper) throws DstExecutionException {
		int i = 1;
		Object[] icebergVals = new Object[oper.afterValues.size()];
		for (Object o : oper.afterValues) {
			try {
				Object icebergVal = getIcebergValue(o, oper.tbl.columns.get(i-1).type);
				icebergVals[i-1] = icebergVal;
			} catch (Exception e) {
				this.tracer.error("Failed to bind argument with value " + o + " at index " + i + " for DELETEINSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
				throw new DstExecutionException("Failed to bind argument with value " + o + " at index " + i + " for DELETEINSERT operation for table : " + oper.tbl + " for column : " + oper.tbl.columns.get(i-1).column + "(" + oper.tbl.columns.get(i-1).type + "). Failed record dump : " + getRecordDump(oper.beforeValues, oper.afterValues), e);
			}
			++i;
		}		
		Row insertRow = RowFactory.create(icebergVals);
		insertRows.add(insertRow);
	}
	
	@Override
	protected void addToUpsertBatch() throws DstExecutionException {
		//NoOp
	}

	protected void addToReplaceBatch() throws DstExecutionException {		
		//NoOp
	}

	@Override
	protected void addToUpdateBatch() throws DstExecutionException {
		//NoOp
	}

	@Override
	protected void addToDeleteBatch() throws DstExecutionException {
		//NoOp
	}

	@Override
	protected boolean isOperPrepared(Oper oper) throws DstExecutionException {
		switch (oper.operType) {
		case INSERT:
			return (!insertRows.isEmpty());
		case UPDATE:
			return (!updateRows.isEmpty());			
		case DELETE:
			return (!deleteRows.isEmpty());
		case UPSERT:
			return (!insertRows.isEmpty());
		case REPLACE:
			return (!insertRows.isEmpty());
		case DELETEINSERT:
			return ((!deleteRows.isEmpty()) && (!insertRows.isEmpty()));			
		default:
			return false;
		}
	}


	@Override
	protected final void resetBatch() throws DstExecutionException {
		this.deleteRows.clear();
		this.insertRows.clear();
		this.updateRows.clear();
		this.currentTableSchema = null;
		this.currentTable = null;
	}

	@Override
	protected void executeInsertBatch() throws DstExecutionException {
		try {
			Dataset<Row> insertDataSet = spark.createDataFrame(insertRows, currentTableSchema);
			insertDataSet.write().format("iceberg").mode("append").insertInto(sqlGenerator.getTableNameSQL(currentTable.id));
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute insert batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeDeleteInsertBatch() throws DstExecutionException {
		try {
			try {
				Dataset<Row> insertDataSet = spark.createDataFrame(insertRows, currentTableSchema);
				TableID tempTableID = TableID.from(currentTable.id.deviceUUID, currentTable.id.deviceName, dstIndex, null, null, currentTable.id.table + "_tmp");
				insertDataSet.createOrReplaceTempView(tempTableID.table);
				insertDataSet.write().format("iceberg").mode("append").insertInto(sqlGenerator.getTableNameSQL(tempTableID));
				String deleteQuery = sqlGenerator.getDeleteFromTempTableSql(currentTable, tempTableID);
				tracer.debug(currentTable + " SQL : " + deleteQuery);
			    spark.sql(deleteQuery);
			} catch (Exception e) {
				throw new DstExecutionException("Failed to execute delete subbatch of deleteinsert batch : " + e.getMessage(), e);
			}

			try {
				Dataset<Row> insertDataSet = spark.createDataFrame(insertRows, currentTableSchema);
				insertDataSet.write().format("iceberg").mode("append").insertInto(sqlGenerator.getTableNameSQL(currentTable.id));
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
			Dataset<Row> insertDataSet = spark.createDataFrame(insertRows, currentTableSchema);
			TableID tempTableID = TableID.from(currentTable.id.deviceUUID, currentTable.id.deviceName, dstIndex, null, null, currentTable.id.table + "_tmp");
			insertDataSet.createOrReplaceTempView(tempTableID.table);
			//insertDataSet.write().format("iceberg").mode("append").insertInto(sqlGenerator.getTableNameSQL(tempTableID));
			String mergeQuery = sqlGenerator.getMergeFromTempTableSql(currentTable, tempTableID);
			tracer.debug(currentTable + " SQL : " + mergeQuery);
			spark.sql(mergeQuery);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute upsert batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeReplaceBatch() throws DstExecutionException {
		try {
			executeDeleteInsertBatch();
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute replace batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeUpdateBatch() throws DstExecutionException {
		try {			
			//For update we create a temp table with all columns repeated twice
			StructField[] fields = new StructField[currentTableSchema.fields().length + currentTableSchema.fields().length];
			System.arraycopy(getBeforeFields(), 0, fields, 0, currentTableSchema.fields().length);
			System.arraycopy(getAfterFields(), 0, fields, currentTableSchema.fields().length, currentTableSchema.fields().length);
			
			StructType tempTableSchema = DataTypes.createStructType(fields);
			
			Dataset<Row> updateDataSet = spark.createDataFrame(updateRows, tempTableSchema);
			TableID tempTableID = TableID.from(currentTable.id.deviceUUID, currentTable.id.deviceName, dstIndex, null, null, currentTable.id.table + "_tmp");
			updateDataSet.createOrReplaceTempView(tempTableID.table);
			String updateQuery = sqlGenerator.getUpdateFromTempTableSql(currentTable, tempTableID);
			tracer.debug(currentTable + " SQL : " + updateQuery);
			spark.sql(updateQuery);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute update batch : " + e.getMessage(), e);
		}
	}

	@Override
	protected void executeDeleteBatch() throws DstExecutionException {
		try {
			Dataset<Row> deleteDataSet = spark.createDataFrame(deleteRows, currentTableSchema);
			TableID tempTableID = TableID.from(currentTable.id.deviceUUID, currentTable.id.deviceName, dstIndex, null, null, currentTable.id.table + "_tmp");
			deleteDataSet.createOrReplaceTempView(tempTableID.table);
			String deleteQuery = sqlGenerator.getDeleteFromTempTableSql(currentTable, tempTableID);
			tracer.debug(currentTable + " SQL : " + deleteQuery);
			spark.sql(deleteQuery);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute delete batch : " + e.getMessage(), e);
		}
	}

	protected final void prepareStatement(Table tbl) throws DstExecutionException {
		this.deleteRows.clear();
		this.insertRows.clear();
		this.updateRows.clear();
		List<StructField> fields = new ArrayList<>();
		for (Column c : tbl.columns) {
			fields.add(DataTypes.createStructField(c.column, getIcebergDataType(c.type), true)); // Adjust data type as needed
		}
		currentTableSchema = DataTypes.createStructType(fields);
		currentTable = tbl;
	}

	private final StructField[] getBeforeFields() {
		StructField[] fields = new StructField[currentTable.columns.size()];
		int idx = 0;
		for (Column c : currentTable.columns) {
			fields[idx] = DataTypes.createStructField(c.column + "b", getIcebergDataType(c.type), true);
			++idx;
		}
		return fields;
	}

	private final StructField[] getAfterFields() {
		StructField[] fields = new StructField[currentTable.columns.size()];
		int idx = 0;
		for (Column c : currentTable.columns) {
			fields[idx] = DataTypes.createStructField(c.column + "a", getIcebergDataType(c.type), true);
			++idx;
		}
		return fields;
	}
	

	private Object getIcebergValue(Object o, com.synclite.consolidator.schema.DataType type) {
		if (o == null) {
			return null;
		}
		switch (type.javaSQLType) {
		case SMALLINT:
		case TINYINT:
		case INTEGER:
			return Integer.valueOf(o.toString());
		case BIGINT:
			return Long.valueOf(o.toString());
		case CLOB:
		case VARCHAR:
		case CHAR:
			return o.toString();
		case DECIMAL:
		case DOUBLE:
		case REAL:
		case NUMERIC:
			return Double.valueOf(o.toString());
		case FLOAT:
			return Float.valueOf(o.toString());
		case BINARY:			
		case VARBINARY:
		case LONGVARBINARY:
		case BLOB:
			if (o instanceof Byte[]) {
				return 0;
			} else {
				return o.toString().getBytes();
			}
		case BIT:
		case BOOLEAN:
			switch(o.toString().toLowerCase().strip()) {
			case "1":
			case "y":
			case "yes":
			case "t":
			case "true":
			case "on":
				return true;
			default:
				return false;
			}
		case DATE:
			return Date.valueOf(o.toString());
		case TIME:
		case TIME_WITH_TIMEZONE:
		case TIMESTAMP:
		case TIMESTAMP_WITH_TIMEZONE:
			return Timestamp.valueOf(o.toString());
		default:
			return o.toString();
		}
	}
	
	private DataType getIcebergDataType(com.synclite.consolidator.schema.DataType type) {
		switch (type.javaSQLType) {
		case SMALLINT:
		case TINYINT:
		case INTEGER:
			return DataTypes.IntegerType;
		case BIGINT:
			return DataTypes.LongType;
		case CLOB:
		case VARCHAR:
		case CHAR:
			return DataTypes.StringType;
		case DECIMAL:
		case DOUBLE:
		case REAL:
		case NUMERIC:
			return DataTypes.DoubleType;
		case FLOAT:
			return DataTypes.FloatType;
		case BINARY:			
		case VARBINARY:
		case LONGVARBINARY:
		case BLOB:
			return DataTypes.BinaryType;
		case BIT:
		case BOOLEAN:
			return DataTypes.BooleanType;
		case DATE:
			return DataTypes.DateType;
		case TIME:
		case TIME_WITH_TIMEZONE:
		case TIMESTAMP:
		case TIMESTAMP_WITH_TIMEZONE:
			return DataTypes.TimestampType;
		default:
			return DataTypes.StringType;
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
        boolean databaseExists = spark.catalog().databaseExists(tbl.id.database);
        return databaseExists;
	}

	@Override
	public boolean schemaExists(Table tbl) throws DstExecutionException {
		return true;
	}

	@Override
	public boolean tableExists(Table tbl) throws DstExecutionException {
		try {
			spark.sql("DESCRIBE TABLE " + sqlGenerator.getTableNameSQL(tbl.id));
		} catch (Exception e) {	
			if (e.getMessage().contains("Table or view not found")) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean columnExists(Table tbl, Column c) throws DstExecutionException {
        StructType schema = spark.table(sqlGenerator.getTableNameSQL(tbl.id)).schema();
        // Check if the column already exists
        boolean columnExists = false;
        for (StructField field : schema.fields()) {
            if (field.name().equals(c.column)) {
                columnExists = true;
                break;
            }
        }
		return columnExists;
	}

	@Override
	public void close() throws DstExecutionException {
		//no op
	}
	
	@Override
	public void beginTran() throws DstExecutionException {
		//NoOp
	}

	@Override
	protected boolean isOpenInsertBatch() {
		return (!insertRows.isEmpty());
	}

	@Override
	protected boolean isOpenUpdateBatch() {
		return (!updateRows.isEmpty());
	}

	@Override
	protected boolean isOpenDeleteBatch() {
		return (!deleteRows.isEmpty());
	}

	@Override
	protected boolean isOpenUpsertBatch() {
		return (!insertRows.isEmpty());
	}

	@Override
	protected boolean isOpenReplaceBatch() {
		return (!insertRows.isEmpty());
	}

	@Override
	protected void doCommit() throws DstExecutionException {
		//NoOp
	}

	private final void closeConn() throws DstExecutionException {
		//NoOp
	}

	@Override
	public void rollbackTran() throws DstExecutionException {
		//NoOp
	}

	@Override
	public void createSchema(CreateSchema oper) throws DstExecutionException {
		//No Op
	}

	@Override
	protected CDCLogPosition readCDCLogPosition(String deviceUUID, String deviceName, ConsolidatorDstTable dstControlTable) throws DstExecutionException {
		try {
			Dataset<Row> rs = spark.sql(sqlGenerator.getCheckpointTableSelectSql(deviceUUID, deviceName, dstControlTable));
			for (Row row : rs.collectAsList()) {
				CDCLogPosition logPos = new CDCLogPosition(0, -1, -1, 0, 0);
				logPos.commitId = row.getLong(0);
				logPos.changeNumber = row.getLong(1);
				logPos.txnChangeNumber = row.getLong(2);
				logPos.logSegmentSequenceNumber = row.getLong(3);
				logPos.txnCount = row.getLong(4);
				return logPos;
			}
			throw new DstExecutionException("No checkpoint log position found in the destination");
		} catch (Exception e) {
			throw new DstExecutionException("Failed to read checkpoint log position from destination : " + e.getMessage(), e);
		}
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		return false;		
	}


	protected boolean isTableAlreadyExistsException(Exception e) {
		return false;
	}

	@Override
	protected final void executeUnbatchedSql(Oper oper) throws DstExecutionException {
		String sql = oper.getSQL(sqlGenerator);
		this.currentSql = sql;
		tracer.debug(oper.tbl + " : SQL : " + sql);
		try {
			spark.sql(sql);
		} catch (Exception e) {
			throw new DstExecutionException("Failed to execute SQL : " + sql + " : " + e.getMessage(), e);
		}
	}

	@Override
    public boolean canCreateDatabase() {
    	return true;
    }

}
