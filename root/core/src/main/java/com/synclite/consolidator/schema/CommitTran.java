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

import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.oper.DDL;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.processor.SQLExecutor;

public class CommitTran extends DDL {

    public CommitTran() {
        super(null, Collections.EMPTY_LIST);
        this.operType = OperType.COMMITTRAN;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.commitTran();
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getCommitTranSQL();
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) {
        return tableMapper.mapOper(this);
    }

}
