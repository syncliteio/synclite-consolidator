package com.synclite.consolidator.schema;

import java.sql.JDBCType;

import com.synclite.consolidator.global.ConfLoader;

public class PGDataTypeMapper extends DataTypeMapper {

    protected PGDataTypeMapper(int dstIndex) {
        super(dstIndex);
    }

    @Override
    protected DataType doMapTypeConservative(DataType type) {
        if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
            return new DataType("BYTEA", JDBCType.BLOB, getStorageClass("BYTEA"));
        } else if ((type.javaSQLType == JDBCType.ARRAY) && (type.dbNativeDataType.equalsIgnoreCase("FLOAT[]") || type.dbNativeDataType.equalsIgnoreCase("DOUBLE[]"))) {
        	if (ConfLoader.getInstance().getDstPGVectorExtensionEnabled(dstIndex)) {
        		return new DataType("VECTOR", JDBCType.ARRAY, getStorageClass("TEXT"));
        	} else {
        		return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
        	}
        } else {
            return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
        }
    }

	@Override
	protected DataType getBestEffortArrayDataType(DataType t) {
        String typeToCheck = t.dbNativeDataType.toLowerCase().trim().split("[\\s(]+")[0];
        
        if (ConfLoader.getInstance().getDstPGVectorExtensionEnabled(dstIndex)) {
        	if (typeToCheck.equals("float[]") || typeToCheck.equals("vector")) {
        		return new DataType("VECTOR", JDBCType.ARRAY, getStorageClass("TEXT"));
        	} else {
        		return new DataType("text[]", JDBCType.ARRAY, getStorageClass("TEXT"));
        	}
        } else {
    		return new DataType("text[]", JDBCType.ARRAY, getStorageClass("TEXT"));
        }
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
        return new DataType("bytea", JDBCType.BLOB, StorageClass.BLOB);
	}

	@Override
	protected DataType getBestEffortTextDataType() {
        return new DataType("text", JDBCType.CLOB, StorageClass.TEXT);
	}

	@Override
	protected DataType getBestEffortIntegerDataType() {
        return new DataType("bigint", JDBCType.BIGINT, StorageClass.NUMERIC);
	}

	@Override
	protected DataType getBestEffortRealDataType() {
        return new DataType("double precision", JDBCType.DOUBLE, StorageClass.REAL);
	}

	@Override
	protected DataType getBestEffortClobDataType() {
        return new DataType("text", JDBCType.CLOB, StorageClass.TEXT);
	}

}
