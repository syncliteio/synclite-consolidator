package com.synclite.consolidator.schema;

import java.sql.JDBCType;

import com.synclite.consolidator.global.ConfLoader;

public class MSSQLDataTypeMapper extends DataTypeMapper {

	protected MSSQLDataTypeMapper(int dstIndex) {
		super(dstIndex);
	}

	@Override
	protected DataType doMapTypeConservative(DataType type) {
		if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
			return new DataType("VARBINARY(MAX)", JDBCType.BLOB, getStorageClass("VARBINARY(MAX)"));
		} else if (type.dbNativeDataType.startsWith("NCHAR")){
			return new DataType("NVARCHAR(MAX)", JDBCType.NVARCHAR , getStorageClass("NVARCHAR(MAX)"));          
		} else if (type.dbNativeDataType.startsWith("NATIVE")){
			return new DataType("NVARCHAR(MAX)", JDBCType.NVARCHAR , getStorageClass("NVARCHAR(MAX)"));
		} else if (type.dbNativeDataType.startsWith("NVARCHAR")){
			return new DataType("NVARCHAR(MAX)", JDBCType.NVARCHAR, getStorageClass("NVARCHAR(MAX)"));
		}		
		return new DataType("VARCHAR(MAX)", JDBCType.VARCHAR, getStorageClass("VARCHAR(MAX)"));
	}

	@Override
	protected DataType getBestEffortArrayDataType(DataType t) {
        String typeToCheck = t.dbNativeDataType.toLowerCase().trim().split("[\\s(]+")[0];       
        if (ConfLoader.getInstance().getDstVectorExtensionEnabled(dstIndex)) {
        	if (typeToCheck.startsWith("float") || typeToCheck.startsWith("vector")) {
				String s = typeToCheck;
				try {
					int subscript = Integer.parseInt(s.substring(s.indexOf('[') + 1, s.indexOf(']')));
	        		return new DataType("VECTOR(" + subscript + ")", JDBCType.ARRAY, getStorageClass("TEXT"));
				} catch (NumberFormatException e) {
	        		return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
				}
        	} else {
        		return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
        	}
        } else {
    		return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
        }
	}

	@Override
	protected DataType getBestEffortDateTimeDataType() {
		return new DataType("datetime", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);
	}

	@Override
	protected DataType getBestEffortDateDataType() {
		return new DataType("datetime", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);
	}

	@Override
	protected DataType getBestEffortBooleanDataType() {
		return new DataType("bit", JDBCType.BIT, StorageClass.NUMERIC);
	}

	@Override
	protected DataType getBestEffortBlobDataType() {
		return new DataType("varbinary(max)", JDBCType.BLOB, StorageClass.BLOB);
	}

	@Override
	protected DataType getBestEffortTextDataType() {
		return new DataType("nvarchar(max)", JDBCType.CLOB, StorageClass.TEXT);
	}

	
	@Override
	protected DataType getBestEffortIntegerDataType() {
		return new DataType("bigint", JDBCType.BIGINT, StorageClass.NUMERIC);
	}

	@Override
	protected DataType getBestEffortRealDataType() {
		return new DataType("float", JDBCType.FLOAT, StorageClass.NUMERIC);
	}
	
	@Override
    protected DataType doMapTypeForSystemColumn(DataType srcType) {
        if (srcType.dbNativeDataType.toLowerCase().startsWith("char")) {
			return srcType;
		} 
        return doMapTypeBestEffort(srcType);
	}

	@Override
	protected DataType getBestEffortClobDataType() {
		return new DataType("nvarchar(max)", JDBCType.CLOB, StorageClass.CLOB);
	}

}
