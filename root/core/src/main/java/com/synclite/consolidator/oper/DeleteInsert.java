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

import java.nio.file.Path;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class DeleteInsert extends Insert {

	private Delete delete;
	private Insert insert;
    public DeleteInsert(Table tbl, List<Object> beforeValues, List<Object> afterValues) {
        super(tbl, afterValues, true);        
        this.delete = new Delete(tbl, beforeValues);
        this.insert = new Insert(tbl, afterValues, false);
        this.operType = OperType.DELETEINSERT;        
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.deleteInsert(this);
    }

    @Override
    public boolean isBatchable() {
        return true;
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getInsertSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }
    
    @Override
    public String getFileLoaderSQL(SQLGenerator generator, Path csvFilePath) {
    	return generator.getFileLoaderInsertSQL(this, csvFilePath);
    }

	public Delete getDeleteOper() {
		return delete;
	}
	
	public Insert getInsertOper() {
		return insert;
	}

	@Override
	public void replaceArgs(List<Object> beforeValues, List<Object> afterValues) {
		this.delete.beforeValues = beforeValues;
		this.insert.afterValues = afterValues;
		this.afterValues= afterValues;
	}

}
