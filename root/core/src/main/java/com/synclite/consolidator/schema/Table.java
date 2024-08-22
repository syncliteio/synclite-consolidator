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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Table {
    public TableID id;
    private boolean isSystemTable;
    private boolean hasPrimaryKey;
    private boolean hasBLOBColumn;
    private boolean hasCLOBColumn;
    public final List<Column> columns = new ArrayList<Column>();
    public final HashMap<String, Column> colMap = new HashMap<String, Column>();
    public String sql;

    public Table() {
        this.isSystemTable = false;
        this.hasPrimaryKey = false;
        this.hasBLOBColumn = false;
        this.hasCLOBColumn = false;
    }

    public final boolean hasPrimaryKey() {
        return this.hasPrimaryKey;
    }

    public final boolean hasBLOBColumn() {
        return this.hasBLOBColumn;
    }

    public final boolean hasCLOBColumn() {
        return this.hasCLOBColumn;
    }

    public final boolean hasLOBColumn() {
        return this.hasCLOBColumn || this.hasBLOBColumn;
    }

    public void addColumn(Column c) {
        columns.add(c);
        colMap.put(c.column, c);
        if (c.pkIndex != 0) {
            this.hasPrimaryKey = true;
        }
        if (c.isBLOB()) {
        	this.hasBLOBColumn = true;
        }

        if (c.isCLOB()) {
        	this.hasCLOBColumn = true;
        }        
    }

    public void replaceColumn(Column c) {
    	Column existingCol = colMap.get(c.column);    	
    	columns.remove(existingCol);
        colMap.put(c.column, c);
        if (c.pkIndex != 0) {
            this.hasPrimaryKey = true;
        }
        if (c.isBLOB()) {
        	this.hasBLOBColumn = true;
        }

        if (c.isCLOB()) {
        	this.hasCLOBColumn = true;
        }        
    }

    public void dropColumn(Column c) {
        columns.remove(c);
        colMap.remove(c);
        checkPrimaryKey();
    }

    private final void checkPrimaryKey() {
        this.hasPrimaryKey = false;
        for (Column c : columns) {
            if (c.pkIndex != 0) {
                this.hasPrimaryKey = true;
            }
        }
    }

    public void dropColumnIfExists(Column c) {
        if (colMap.containsKey(c)) {
            dropColumn(c);
        }
    }

    public void addColumnIfNotExists(Column c) {
        if (!colMap.containsKey(c.column)) {
            addColumn(c);
        }
    }

    public void replaceColumnIfExists(Column c) {
        if (colMap.containsKey(c.column)) {
            replaceColumn(c);
        }
    }

    public void clearColumns() {
        columns.clear();
        colMap.clear();
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Column getColumnByName(String name) {
        return colMap.get(name);
    }

    public boolean hasColumn(String colName) {
        return colMap.containsKey(colName);
    }

    public void setIsSystemTable() {
        this.isSystemTable = true;
    }

    public boolean getIsSystemTable() {
        return this.isSystemTable;
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public void copyFrom(Table copyFrom) {
        this.sql = copyFrom.sql;
        this.clearColumns();
        for (Column col : copyFrom.columns) {
            Column newCol = new Column(col.cid, col.column, col.type, col.isNotNull, col.defaultValue, col.pkIndex, col.isAutoIncrement);
            addColumn(newCol);
        }
    }

    public final void renameColumn(Column col, String newName) {
        col.column = newName;
        colMap.remove(col.column);
        col.column = newName;
        colMap.put(col.column, col);
    }
}
