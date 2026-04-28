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

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.ConsolidatorDstTable;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class UpdateIfPredicate extends Oper {

    public String sql;
    public String setClause;
    public String predicate;

    public UpdateIfPredicate(Table tbl, String sql) {
        super(tbl);
        this.sql = sql;
        parseSetAndPredicate();
        this.operType = OperType.UPDATE_IF_PREDICATE;
    }

    public UpdateIfPredicate(ConsolidatorDstTable tbl, String sql, String setClause, String mappedPredicate) {
        super(tbl);
        this.sql = sql;
        this.setClause = setClause;
        this.predicate = mappedPredicate;
        this.operType = OperType.UPDATE_IF_PREDICATE;
    }

    private void parseSetAndPredicate() {
        String sqlUpper = this.sql.toUpperCase();
        int setIndex = sqlUpper.indexOf(" SET ");
        int whereIndex = sqlUpper.indexOf(" WHERE ");

        if (setIndex != -1 && whereIndex != -1) {
            this.setClause = sql.substring(setIndex + 5, whereIndex).trim();
            this.predicate = sql.substring(whereIndex + 7).trim().replaceAll(";$", "");
        } else if (setIndex != -1) {
            this.setClause = sql.substring(setIndex + 5).trim().replaceAll(";$", "");
            // No WHERE clause — be safe, restrict to impossible predicate to avoid full-table update
            this.predicate = "1 = 0";
        } else {
            this.setClause = "";
            this.predicate = "1 = 0";
        }
    }

    @Override
    public boolean isBatchable() {
        return false;
    }

    @Override
    public void execute(SQLExecutor executor) throws DstExecutionException {
        executor.executeSQL(this);
    }

    @Override
    public String getSQL(SQLGenerator generator) {
        return generator.getUpdateIfPredicateSQL(this);
    }

    @Override
    public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
        return tableMapper.mapOper(this);
    }
}
