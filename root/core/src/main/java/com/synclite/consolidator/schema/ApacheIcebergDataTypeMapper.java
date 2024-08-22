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

public class ApacheIcebergDataTypeMapper extends DataTypeMapper {

	protected ApacheIcebergDataTypeMapper(int dstIndex) {
		super(dstIndex);
	}

	@Override
	protected DataType doMapTypeConservative(DataType type) {
        if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
            return new DataType("BINARY", JDBCType.BLOB, getStorageClass("BINARY"));
        } if (type.dbNativeDataType.equalsIgnoreCase("clob")) {
            return new DataType("STRING", JDBCType.CLOB, getStorageClass("Clob"));
        } else {
        	String dt = "STRING";
        	return new DataType(dt, JDBCType.VARCHAR, getStorageClass(dt));
        }
	}

	@Override
	protected DataType getBestEffortDateTimeDataType() {
		return new DataType("TIMESTAMP", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortDateDataType() {
		return new DataType("TIMESTAMP", JDBCType.TIMESTAMP, StorageClass.TIMESTAMP);	
	}

	@Override
	protected DataType getBestEffortBooleanDataType() {
		return new DataType("BOOLEAN", JDBCType.BOOLEAN, StorageClass.NUMERIC);
	}

	
	@Override
	protected DataType getBestEffortBlobDataType() {
		return new DataType("BINARY", JDBCType.VARCHAR, StorageClass.TEXT);	
	}

	@Override
	protected DataType getBestEffortTextDataType() {
		return new DataType("STRING", JDBCType.VARCHAR, StorageClass.TEXT);	
	}

	@Override
	protected DataType getBestEffortClobDataType() {
		return new DataType("STRING", JDBCType.VARCHAR, StorageClass.TEXT);	
	}

	@Override
	protected DataType getBestEffortIntegerDataType() {
		return new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC);	
	}

	@Override
	protected DataType getBestEffortRealDataType() {
		return new DataType("DOUBLE", JDBCType.DOUBLE, StorageClass.REAL);	
	}

}
