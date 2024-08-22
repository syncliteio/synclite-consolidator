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
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.schema.TableMapper;

public class Minus extends Oper {

	public String sql;
	public Table rhsTable;

    public Minus(Table tbl, String sql) {
        super(tbl);
        this.sql = sql;
        parseRhs();
        this.operType = OperType.MINUS;
    }

    private final void parseRhs() {
    	String[] tokens = sql.split("\\s");
    	String[] tabTokens = tokens[2].split("\\.");
    	if (tabTokens.length == 1) {
    		String databaseName = "main";
    		String tableName = tabTokens[0];
    		this.rhsTable = ConsolidatorSrcTable.from(TableID.from(this.tbl.id.deviceUUID, this.tbl.id.deviceName, 1, databaseName, null, tableName));
    	} else if (tabTokens.length == 2) {
    		String databaseName = tabTokens[0];
    		String tableName = tabTokens[1];
    		this.rhsTable = ConsolidatorSrcTable.from(TableID.from(this.tbl.id.deviceUUID, this.tbl.id.deviceName, 1, databaseName, null, tableName));
    	}
    }

	public Minus(ConsolidatorDstTable lhs, ConsolidatorDstTable rhs) {
        super(lhs);
        this.rhsTable = rhs;
        this.operType = OperType.MINUS;
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
		return generator.getMinusSQL(this);
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) throws SyncLiteException {
		return tableMapper.mapOper(this);
	}

}
