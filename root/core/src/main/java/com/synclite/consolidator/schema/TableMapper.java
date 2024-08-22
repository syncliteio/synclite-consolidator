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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.BeginTran;
import com.synclite.consolidator.oper.CopyColumn;
import com.synclite.consolidator.oper.CopyTable;
import com.synclite.consolidator.oper.CreateDatabase;
import com.synclite.consolidator.oper.CreateSchema;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.FinishBatch;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Minus;
import com.synclite.consolidator.oper.NoOp;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;
import com.synclite.consolidator.oper.Replace;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;

public abstract class TableMapper {
		
	private static final class InstanceHolder {
		private static final TableMapper USER_TM_INSTANCES[] = createUserTableMappers();
		private static final TableMapper SYSTEM_TM_INSTANCES[] = createSystemTableMappers();

	    private static final TableMapper[] createUserTableMappers() {    	
	    	TableMapper INSTANCES[] = new TableMapper[ConfLoader.getInstance().getNumDestinations() + 1];
	    	
	    	for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
	            switch(ConfLoader.getInstance().getDstSyncMode()) {
	            case REPLICATION:
	                INSTANCES[dstIndex] = new ReplicationTableMapper(dstIndex);
	                break;
	            case CONSOLIDATION:
	            	INSTANCES[dstIndex] = new ConsolidationTableMapper(dstIndex);
	            	break;
	            case EVENT_STREAMING:
	            	INSTANCES[dstIndex] = new EventStreamingTableMapper(dstIndex);
	            	break;
	            }
	    	}
	    	return INSTANCES;
		}

	    private static final TableMapper[] createSystemTableMappers() {    	
	    	TableMapper INSTANCES[] = new TableMapper[ConfLoader.getInstance().getNumDestinations() + 1];	    	
	    	for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
	    		INSTANCES[dstIndex] = new ConsolidationTableMapper(dstIndex);
	    	}
	    	return INSTANCES;
		}
	}

    protected DataTypeMapper dataTypeMapper;
    protected final ConcurrentHashMap<ConsolidatorSrcTable, ConsolidatorDstTable> tableMap = new ConcurrentHashMap<ConsolidatorSrcTable, ConsolidatorDstTable>();
    protected final SQLGenerator sqlGenerator;
    protected final ValueMapper valueMapper;
    protected int dstIndex;
    protected TableMapper(int dstIndex) {
        this.dstIndex = dstIndex;
        this.dataTypeMapper = DataTypeMapper.getInstance(dstIndex);
        this.sqlGenerator = SQLGenerator.getInstance(dstIndex);
        //if (ConfLoader.getInstance().getDstEnableValueMapper(dstIndex)) {
        	this.valueMapper = ValueMapper.getInstance(dstIndex);
        //} else {
        	//this.valueMapper = SystemValueMapper.getInstance(dstIndex);
        //}
    }

    public ValueMapper getValueMapper() {
    	return valueMapper;
    }
    
    public static TableMapper getUserTableMapperInstance(int dstIndex) throws SyncLiteException {
    	return InstanceHolder.USER_TM_INSTANCES[dstIndex];
    }

    public static TableMapper getSystemTableMapperInstance(int dstIndex) throws SyncLiteException {
    	return InstanceHolder.SYSTEM_TM_INSTANCES[dstIndex];
    }

    public void remove(ConsolidatorSrcTable srcTable) {
        ConsolidatorDstTable dstTable = tableMap.get(srcTable);
        if (dstTable != null) {
            ConsolidatorDstTable.remove(dstTable.id);
            tableMap.remove(srcTable);
        }
    }

    public ConsolidatorDstTable mapTable(ConsolidatorSrcTable srcTable) {
        ConsolidatorDstTable dstTable = tableMap.get(srcTable);
        if (dstTable != null) {
            return dstTable;
        }
        TableID dstTableID = mapTableID(srcTable.id);
        dstTable = ConsolidatorDstTable.from(dstTableID);
        
        dstTable.clearColumns();
        for (Column srcColumn : srcTable.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, srcTable.id.table, srcColumn.column)) {
        		continue;
        	}
        	Column dstColumn;
        	if (srcTable.getIsSystemTable()) {
                dstColumn = mapSystemColumn(srcColumn);
            } else {
                dstColumn = mapColumn(srcTable.id, srcColumn);
            }
            dstTable.addColumn(dstColumn);
        }
        
        if (srcTable.getIsSystemTable()) {
        	dstTable.setIsSystemTable();
        }
        
        tableMap.put(srcTable, dstTable);
        return dstTable;
    }

    protected final Column mapSystemColumn(Column srcColumn) {  	
        Column dstColumn = new Column(
                srcColumn.cid,
                srcColumn.column,
                mapTypeForSystemColumn(srcColumn.type),
                srcColumn.isNotNull,
                srcColumn.defaultValue,
                srcColumn.pkIndex,
                srcColumn.isAutoIncrement
            );
        return dstColumn;
    }

    public Column mapColumn(TableID srcTableID, Column srcColumn) {
		String mappedColumnName = ConfLoader.getInstance().getMappedColumnName(dstIndex, srcTableID.table, srcColumn.column);
    	Column dstColumn = new Column(
                srcColumn.cid,
                mappedColumnName,
                mapType(srcColumn.type),
                srcColumn.isNotNull,
                srcColumn.defaultValue,
                srcColumn.pkIndex,
                srcColumn.isAutoIncrement
            );
        return dstColumn;
    }

    protected abstract TableID mapTableID(TableID srcTableID);

    protected DataType mapType(DataType type) {
        return this.dataTypeMapper.mapType(type);
    }

    protected DataType mapTypeForSystemColumn(DataType type) {
        return this.dataTypeMapper.mapTypeForSystemColumn(type);
    }

    public abstract Insert mapInsert(Insert insert) throws SyncLiteException;

    public abstract void bindMappedInsert(DML mappedInsert, List<Object> afterValues) throws SyncLiteException;
	public abstract void bindMappedUpdate(DML mappedUpdate, List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException;
	public abstract void bindMappedDelete(DML mappedDelete, List<Object> beforeValues) throws SyncLiteException;
	
    public abstract List<Oper> mapOper(Insert insert) throws SyncLiteException;

    public abstract List<Oper> mapOper(Update update) throws SyncLiteException;

    public abstract List<Oper> mapOper(Delete delete) throws SyncLiteException;

	public abstract List<Oper> mapOper(LoadFile loadFile);

    public List<Oper> mapOper(TruncateTable truncate) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) truncate.tbl);
        return Collections.singletonList(new TruncateTable(dstTable));
    }


    public List<Oper> mapOper(CreateTable create) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) create.tbl);
        return Collections.singletonList(new CreateTable(dstTable));
    }

    public List<Oper> mapOper(DropTable drop) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) drop.tbl);
        return Collections.singletonList(new DropTable(dstTable));
    }

    public List<Oper> mapOper(RenameTable renameTableOper) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) renameTableOper.tbl);
        return Collections.singletonList(new RenameTable(dstTable, renameTableOper.oldTable, renameTableOper.newTable));
    }

    public List<Oper> mapOper(CreateDatabase createDatabase) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) createDatabase.tbl);
        return Collections.singletonList(new CreateDatabase(dstTable));
    }

    public List<Oper> mapOper(CreateSchema createSchema) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) createSchema.tbl);
        return Collections.singletonList(new CreateSchema(dstTable));
    }

    public List<Oper> mapOper(NoOp noOp) {
        return Collections.singletonList(noOp);
    }

    public List<Oper> mapOper(AddColumn addColumn) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) addColumn.tbl);
        List<Column> dstColumnsToAdd = new ArrayList<Column>(addColumn.columns.size());
        for (Column c : addColumn.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, addColumn.tbl.id.table, c.column)) {
        		continue;
        	}
            dstColumnsToAdd.add(mapColumn(addColumn.tbl.id, c));
        }
        if (dstColumnsToAdd.isEmpty()) {
        	return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(new AddColumn(dstTable, dstColumnsToAdd.get(0)));
    }

    public List<Oper> mapOper(AlterColumn alterColumn) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) alterColumn.tbl);
        List<Column> dstColumnsToAlter = new ArrayList<Column>(alterColumn.columns.size());
        for (Column c : alterColumn.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, alterColumn.tbl.id.table, c.column)) {
        		continue;
        	}
            dstColumnsToAlter.add(mapColumn(alterColumn.tbl.id, c));
        }
        if (dstColumnsToAlter.isEmpty()) {
        	return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(new AlterColumn(dstTable, dstColumnsToAlter.get(0)));
    }

    public List<Oper> mapOper(DropColumn dropColumn) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) dropColumn.tbl);
        List<Column> dstColumnsToDrop = new ArrayList<Column>(dropColumn.columns.size());
        for (Column c : dropColumn.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, dropColumn.tbl.id.table, c.column)) {
        		continue;
        	}
        	dstColumnsToDrop.add(mapColumn(dropColumn.tbl.id, c));
        }
        if (dstColumnsToDrop.isEmpty()) {
        	return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(new DropColumn(dstTable, dstColumnsToDrop.get(0)));
    }

    public List<Oper> mapOper(RenameColumn renameColumn) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) renameColumn.tbl);
        List<Column> dstColumnsToRename = new ArrayList<Column>(renameColumn.columns.size());
        for (Column c : renameColumn.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, renameColumn.tbl.id.table, c.column)) {
        		continue;
        	}
            dstColumnsToRename.add(mapColumn(renameColumn.tbl.id, c));
        }
        if (dstColumnsToRename.isEmpty()) {
        	return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(new RenameColumn(dstTable, dstColumnsToRename.get(0), renameColumn.oldName, renameColumn.newName));
    }

    public List<Oper> mapOper(BeginTran beginTran) {
        return Collections.singletonList(beginTran);
    }

    public List<Oper> mapOper(CommitTran commitTran) {
        return Collections.singletonList(commitTran);
    }

    public List<Oper> mapOper(CopyColumn copyColumn) {
        ConsolidatorDstTable tbl = mapTable((ConsolidatorSrcTable) copyColumn.tbl);
        Column srcCol = mapColumn(copyColumn.tbl.id, copyColumn.srcCol);
        Column dstCol = mapColumn(copyColumn.tbl.id, copyColumn.dstCol);
        return Collections.singletonList(new CopyColumn(tbl, srcCol, dstCol));
    }

    public List<Oper> mapOper(CopyTable copyTable) {
        ConsolidatorDstTable tbl = mapTable((ConsolidatorSrcTable) copyTable.tbl);
        ConsolidatorDstTable copyFromTable = mapTable((ConsolidatorSrcTable) copyTable.copyFromTable);
        return Collections.singletonList(new CopyTable(tbl, copyFromTable));
    }

    public final void resetAll() {
        tableMap.clear();
    }

	public List<Oper> mapOper(DeleteIfPredicate sqlStmt) {
		ConsolidatorDstTable tbl = null;
		if (sqlStmt.tbl != null) {
			tbl = mapTable((ConsolidatorSrcTable) sqlStmt.tbl);
		}
		return Collections.singletonList(new DeleteIfPredicate(tbl, sqlStmt.sql));
	}

	public List<Oper> mapOper(Minus minus) {
		ConsolidatorDstTable lhs = mapTable((ConsolidatorSrcTable) minus.tbl);
		ConsolidatorDstTable rhs = mapTable((ConsolidatorSrcTable) minus.rhsTable);
		return Collections.singletonList(new Minus(lhs, rhs));		
	}

	public List<Oper> mapOper(FinishBatch finishBatch) {
		return Collections.singletonList(new FinishBatch(finishBatch.tbl));		
	}

    protected Insert getMappedInsertOper(ConsolidatorDstTable dstTable, List<Object> dstValues, boolean applyInsertIdempotently) {
        if (applyInsertIdempotently) {
            //Idempotent Inserts are only possible if the table has a key      
        	if (dstTable.hasPrimaryKey()) {
        		switch(ConfLoader.getInstance().getDstIdempotentDataIngestionMethod(dstIndex)) {
        		case NATIVE_UPSERT:
        			if (this.sqlGenerator.supportsUpsert()) {
        				return new Upsert(dstTable, dstValues);
        			} else {
            			return new DeleteInsert(dstTable, dstValues, dstValues);
        			}
        			
        		case NATIVE_REPLACE:
        			if (this.sqlGenerator.supportsReplace()) {
            			return new Replace(dstTable, dstValues);
        			} else {
            			return new DeleteInsert(dstTable, dstValues, dstValues);
        			}
        			
        		case DELETE_INSERT:
        			return new DeleteInsert(dstTable, dstValues, dstValues);
        		}
        	}
        }
        return new Insert(dstTable, dstValues, false);
    }
    
}
