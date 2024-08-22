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

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class TruncateTable extends DML {

    public TruncateTable(Table tbl) {
        super(tbl, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        this.operType = OperType.TRUNCATE;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.truncate(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getTruncateSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}