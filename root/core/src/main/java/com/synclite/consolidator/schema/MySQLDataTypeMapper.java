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

public class MySQLDataTypeMapper extends DataTypeMapper {

    protected MySQLDataTypeMapper(int dstIndex) {
        super(dstIndex);
    }

    @Override
    protected DataType doMapTypeConservative(DataType type) {
        if (type.dbNativeDataType.equalsIgnoreCase("blob")) {
            return new DataType("BLOB", JDBCType.BLOB, getStorageClass("BLOB"));
        } else {
            return new DataType("TEXT", JDBCType.VARCHAR, getStorageClass("TEXT"));
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
        return new DataType("longblob", JDBCType.BLOB, StorageClass.BLOB);
	}

	@Override
	protected DataType getBestEffortTextDataType() {
        return new DataType("text", JDBCType.VARCHAR, StorageClass.TEXT);
	}

	@Override
	protected DataType getBestEffortClobDataType() {
        return new DataType("longtext", JDBCType.VARCHAR, StorageClass.TEXT);
	}

	@Override
	protected DataType getBestEffortIntegerDataType() {
        return new DataType("bigint", JDBCType.BIGINT, StorageClass.INTEGER);
	}

	@Override
	protected DataType getBestEffortRealDataType() {
        return new DataType("double", JDBCType.DOUBLE, StorageClass.REAL);
	}

	@Override
    protected DataType doMapTypeForSystemColumn(DataType srcType) {
		//
		//We do this because MySQL does not support text column as PK , hence need to maint the char(n) columns like that only
		//
        if (srcType.dbNativeDataType.toLowerCase().startsWith("char")) {
			return srcType;
		} 
        return doMapTypeBestEffort(srcType);
	}


}
