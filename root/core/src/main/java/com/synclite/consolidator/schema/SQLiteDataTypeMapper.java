package com.synclite.consolidator.schema;

import java.sql.JDBCType;

public class SQLiteDataTypeMapper extends DataTypeMapper {

	protected SQLiteDataTypeMapper(int dstIndex) {
		super(dstIndex);
	}

	@Override
	protected DataType doMapTypeConservative(DataType type) {
		if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
			return new DataType("blob", getJavaSqlType("blob"), getStorageClass("blob"));
		} else {
			return new DataType("text", getJavaSqlType("text"),  getStorageClass("text"));
		}
	}

	@Override
	protected DataType doMapTypeBestEffort(DataType type) {
		return type;
	}

	@Override
	protected DataType getBestEffortDateTimeDataType() {
		return new DataType("timestamp", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortDateDataType() {
		return new DataType("timestamp", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortBooleanDataType() {
		return new DataType("boolean", JDBCType.BOOLEAN, StorageClass.NUMERIC);	
	}

	@Override
	protected DataType getBestEffortBlobDataType() {
		return new DataType("blob", JDBCType.BLOB, StorageClass.BLOB);	
	}

	@Override
	protected DataType getBestEffortTextDataType() {
		return new DataType("text", JDBCType.CLOB, StorageClass.CLOB);	
	}

	@Override
	protected DataType getBestEffortIntegerDataType() {
		return new DataType("bigint", JDBCType.BIGINT, StorageClass.NUMERIC);	
	}

	@Override
	protected DataType getBestEffortRealDataType() {
		return new DataType("real", JDBCType.REAL, StorageClass.REAL);	
	}

	@Override
	protected DataType getBestEffortClobDataType() {
		return new DataType("text", JDBCType.CLOB, StorageClass.TEXT);	
	}
}
