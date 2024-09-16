package com.synclite.consolidator.schema;

import java.sql.JDBCType;

public class ClickHouseDataTypeMapper extends DataTypeMapper {

	protected ClickHouseDataTypeMapper(int dstIndex) {
		super(dstIndex);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DataType doMapTypeConservative(DataType type) {
        if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
            return new DataType("Blob", JDBCType.BLOB, getStorageClass("Blob"));
        } if (type.dbNativeDataType.equalsIgnoreCase("clob")) {
            return new DataType("Clob", JDBCType.CLOB, getStorageClass("Clob"));
        } else {
        	String dt = "String";
        	return new DataType(dt, JDBCType.VARCHAR, getStorageClass(dt));
        }
	}

	@Override
	protected DataType getBestEffortDateTimeDataType() {
		return new DataType("DateTime", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortDateDataType() {
		return new DataType("DateTime", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortBooleanDataType() {
		return new DataType("Boolean", JDBCType.BOOLEAN, StorageClass.NUMERIC);	
	}

	@Override
	protected DataType getBestEffortBlobDataType() {
		return new DataType("Blob", JDBCType.BLOB, StorageClass.BLOB);	
	}

	@Override
	protected DataType getBestEffortClobDataType() {
		return new DataType("Clob", JDBCType.CLOB, StorageClass.CLOB);	
	}

	@Override
	protected DataType getBestEffortTextDataType() {
		return new DataType("String", JDBCType.VARCHAR, StorageClass.TEXT);	
	}

	@Override
	protected DataType getBestEffortIntegerDataType() {
		return new DataType("Int64", JDBCType.BIGINT, StorageClass.NUMERIC);	
	}

	@Override
	protected DataType getBestEffortRealDataType() {
		return new DataType("Float64", JDBCType.DOUBLE, StorageClass.REAL);	
	}

}
