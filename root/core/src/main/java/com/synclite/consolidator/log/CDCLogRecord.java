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

package com.synclite.consolidator.log;

import java.util.List;

import com.synclite.consolidator.oper.OperType;

public class CDCLogRecord {

    public long commitId;
    public String database;
    public String schema;
    public String table;
    public String oldTable;
    public OperType opType;
    public String sql;
    public long changeNumber;
    public List<CDCColumnValues> values;
    public CDCLogSchema colSchemas;

    public CDCLogRecord(
            long commitId,
            String database,
            String schema,
            String table,
            String oldTable,
            OperType opType,
            String sql,
            long changeNumber,
            List<CDCColumnValues> values,
            CDCLogSchema colSchemas
    ) {

        this.commitId = commitId;
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.oldTable = oldTable;
        this.opType = opType;
        this.sql = sql;
        this.changeNumber = changeNumber;
        this.values = values;
        this.colSchemas= colSchemas;
    }

    @Override
    public String toString() {
        return "CDC Log Record : commit : " + commitId + ", Table : " + database + "." + schema + "." + table + ", OP :" + opType + ", SQL : " + sql + ", change number : " + changeNumber;
    }
};


