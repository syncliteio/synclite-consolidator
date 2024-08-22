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

public class Update extends DML {
	private boolean applyIdempotently;
    public Update(Table tbl, List<Object> beforeValues, List<Object> afterValues) {
        super(tbl, beforeValues, afterValues);
        this.operType = OperType.UPDATE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.update(this);
    }

    @Override
    public boolean isBatchable() {
        return true;
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getUpdateSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }

    public boolean getApplyIdempotently() {
    	return applyIdempotently;
    }

    public String getFileLoaderSQL(SQLGenerator generator, Path csvFilePath) {
    	return generator.getFileLoaderUpdateSQL(this, csvFilePath);
    }

}

