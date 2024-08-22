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
