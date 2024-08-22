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

package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.schema.Table;

public abstract class DML extends Oper {

    public List<Object> beforeValues;
    public List<Object> afterValues;

    public DML(
        Table tbl,
        List<Object> beforeValues,
        List<Object> afterValues
    ) {
        super(tbl);
        this.beforeValues = beforeValues;
        this.afterValues = afterValues;
    }
    
	public void replaceArgs(List<Object> beforeValues, List<Object> afterValues) {
		this.beforeValues= beforeValues;
		this.afterValues= afterValues;
	}

}
