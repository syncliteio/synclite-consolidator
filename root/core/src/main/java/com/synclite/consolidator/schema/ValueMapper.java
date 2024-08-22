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

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;

public class ValueMapper {

	private int dstIndex;
    protected ValueMapper(int dstIndex) {
    	this.dstIndex = dstIndex;
    }

    public static ValueMapper getInstance(int dstIndex) {
    	return new ValueMapper(dstIndex);
    }

    /*
	@Override
	public List<Object> mapValues(Table tbl, List<Object> values) throws SyncLiteException {
		//Iterate on columns and check if value mapping is specified then return mapped value else delegate to system value mapper
	
		if (! ConfLoader.getInstance().tableHasValueMappings(dstIndex, tbl.id.table)) {
			return systemValueMapper.mapValues(tbl, values);
		}

		int idx = 0;
		List<Object> outList = new ArrayList<Object>(values.size());
		for (Column c : tbl.columns) {
			Object srcValue = values.get(idx);
			outList.add(mapValue(tbl.id, c, srcValue));
			++idx;
		}		
		return outList;
	}
*/
    
	public Object mapValue(TableID tableID, Column c, Object value) throws SyncLiteException {
		String valueToCheck;
		if (value == null) {
			valueToCheck = "null";
		} else {
			valueToCheck = value.toString();
		}
		Object mappedValue = ConfLoader.getInstance().getMappedValue(dstIndex, tableID.table, c.column, valueToCheck.toString());	
		if (mappedValue == null) {
			//If no mapping found then delegate to system mappper 
			return value;
		} else {
			return mappedValue;
		}

	}

}
