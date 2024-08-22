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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;

public class ConsolidatorSrcTable extends Table {
    private static final ConcurrentHashMap<TableID, ConsolidatorSrcTable> consolidatorSrcTables = new ConcurrentHashMap<TableID, ConsolidatorSrcTable>();

    private ConsolidatorSrcTable(TableID id) {
        this.id = id;
    }

    public static ConsolidatorSrcTable from(TableID id) {
        return consolidatorSrcTables.computeIfAbsent(id, s -> new ConsolidatorSrcTable(s));
    }

    public void refreshColumns(TableMapper tableMapper, List<Column> newSchemaCols) {
        //Clear off all columns of this table object in case the object is lying around
        //Clear off the table entry from the mapper in case its there from old instances of same table name
        tableMapper.remove(this);
        clearColumns();
        for (Column c : newSchemaCols) {
            addColumn(c);
        }
    }

    public Oper generateCreateTableOper(TableMapper tableMapper, List<Column> newSchemaCols) {
    	refreshColumns(tableMapper, newSchemaCols);
    	return new CreateTable(this);
    }

    public Oper generateAddColumnOper(List<Column> newSchemaCols) {
        //Add columns
        for (Column c : newSchemaCols) {
            if (!colMap.containsKey(c.column)) {
                addColumn(c);
                return new AddColumn(this, c);
            }
        }
        return null;
    }

    public Oper generateAlterColumnOper(List<Column> alteredSchemaCols) {
        //Add columns
        for (Column c : alteredSchemaCols) {
            if (colMap.containsKey(c.column)) {
            	Column existingCol = colMap.get(c.column);
            	if (! existingCol.type.dbNativeDataType.equals(c.type.dbNativeDataType)) {
            		replaceColumn(c);
            		return new AlterColumn(this, c);
            	} else if (existingCol.isNotNull != c.isNotNull) {
            		replaceColumn(c);
            		return new AlterColumn(this, c);           		
            	} else if (existingCol.pkIndex != c.pkIndex) {
            		replaceColumn(c);
            		return new AlterColumn(this, c);
            	}
            }
        }
        return null;
    }

    public Oper generateDropColumnOper(List<Column> newSchemaCols) {
        HashSet<String> newSchemaColSet = new HashSet<String>();
        for (Column c : newSchemaCols) {
            newSchemaColSet.add(c.column);
        }
        Column colToRemove = null;
        List<Oper> addColumns = new ArrayList<Oper>();
        for (Column c : columns) {
            if (!newSchemaColSet.contains(c.column)) {
                colToRemove = c;
                break;
            }
        }
        
        if (colToRemove != null) {
	        dropColumn(colToRemove);
	        return new DropColumn(this, colToRemove);
        }
        return null;
    }

    public Oper generateRenameColumnOper(String oldColName, String newColName) {
        Column renamedCol = colMap.get(oldColName);
        renameColumn(renamedCol, newColName);
        return new RenameColumn(this, renamedCol, oldColName, newColName);
    }

    public Oper generateRenameTableOper(TableMapper tableMapper, String oldTableName, String newTableName) {
        TableID oldTableID = TableID.from(this.id.deviceUUID, this.id.deviceName, 1, this.id.database, this.id.schema, oldTableName);
        ConsolidatorSrcTable oldTable = ConsolidatorSrcTable.from(oldTableID);

        TableID newTableID = TableID.from(this.id.deviceUUID, this.id.deviceName, 1, this.id.database, this.id.schema, newTableName);
        ConsolidatorSrcTable newTable = ConsolidatorSrcTable.from(newTableID);
        newTable.copyFrom(oldTable);
        return new RenameTable(this, oldTable, newTable);
    }

    public Oper generateDropTableOper(TableMapper tableMapper) {
        return new DropTable(this);
    }

    public static void remove(TableID id) {
        consolidatorSrcTables.remove(id);
    }

    public static int getCount() {
        return consolidatorSrcTables.size();
    }

    public static void resetAll() {
        consolidatorSrcTables.clear();
    }

    public static Collection<ConsolidatorSrcTable> getTables() {
        return consolidatorSrcTables.values();
    }

}
