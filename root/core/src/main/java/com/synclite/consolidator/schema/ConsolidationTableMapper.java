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

import java.sql.JDBCType;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstObjectInitMode;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.oper.AddColumn;
import com.synclite.consolidator.oper.AlterColumn;
import com.synclite.consolidator.oper.CopyColumn;
import com.synclite.consolidator.oper.CopyTable;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteIfPredicate;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.DropColumn;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;

public class ConsolidationTableMapper extends TableMapper {

	private DateTimeFormatter currentDateTimeFormatter;

    protected ConsolidationTableMapper(int dstIndex) {
        super(dstIndex);
        // TODO Auto-generated constructor stub
        currentDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }
    
    @Override
    public ConsolidatorDstTable mapTable(ConsolidatorSrcTable srcTable) {
        ConsolidatorDstTable dstTable = tableMap.get(srcTable);
        if (dstTable != null) {
            return dstTable;
        }

        TableID dstTableID = mapTableID(srcTable.id);
        dstTable = ConsolidatorDstTable.from(dstTableID);
        dstTable.clearColumns();
        
        Column deviceIDCol = new Column(dstTable.columns.size(), "synclite_device_id", mapTypeForSystemColumn(new DataType("char(36)", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumn(deviceIDCol);

        Column deviceNameCol = new Column(dstTable.columns.size() + 1, "synclite_device_name", mapTypeForSystemColumn(new DataType("char(255)", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumn(deviceNameCol);
        
        Column insertTSCol = new Column(dstTable.columns.size() + 2, "synclite_update_timestamp", mapTypeForSystemColumn(new DataType("char(32)", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumn(insertTSCol);

        boolean hasPK = false;
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
            if (dstColumn.pkIndex > 0) {
                hasPK = true;
            }
        }
        
        if (hasPK) {
            deviceIDCol.pkIndex = 1;
            deviceNameCol.pkIndex = 1;
        }

        if (srcTable.getIsSystemTable()) {
        	dstTable.setIsSystemTable();
        }

        tableMap.put(srcTable, dstTable);
        return dstTable;
    }


    @Override
    protected TableID mapTableID(TableID srcTableID) {
		String mappedTableName = ConfLoader.getInstance().getMappedTableName(dstIndex, srcTableID.table);
        return TableID.from(srcTableID.deviceUUID, srcTableID.deviceName, this.dstIndex, ConfLoader.getInstance().getDstDatabase(dstIndex), ConfLoader.getInstance().getDstSchema(dstIndex), mappedTableName);
    }

    @Override
    public Column mapColumn(TableID srcTableID, Column srcColumn) {
  		String mappedColumnName = ConfLoader.getInstance().getMappedColumnName(dstIndex, srcTableID.table, srcColumn.column);
        Column dstColumn = new Column(
                srcColumn.cid,
                mappedColumnName,
                mapType(srcColumn.type),
                ((srcColumn.pkIndex == 1) ? 1 : 0),
                srcColumn.defaultValue,
                srcColumn.pkIndex,
                0
        );        
        return dstColumn;
    }


    @Override
    public Insert mapInsert(Insert insert) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) insert.tbl);
        
        List<Object> dstValues = new ArrayList<Object>(insert.afterValues.size() + 3);
        dstValues.add(dstTable.id.deviceUUID);
        dstValues.add(dstTable.id.deviceName);
        dstValues.add(getCurrentTS());
        dstValues.addAll(insert.afterValues);
        
        return getMappedInsertOper(dstTable, dstValues, insert.applyInsertIdempotently());
       
    }

	@Override
    public void bindMappedInsert(DML mappedOper, List<Object> afterValues) throws SyncLiteException {
        ConsolidatorDstTable dstTable = (ConsolidatorDstTable) mappedOper.tbl;       
        mappedOper.afterValues.clear();
        mappedOper.afterValues.add(dstTable.id.deviceUUID);
        mappedOper.afterValues.add(dstTable.id.deviceName);
        mappedOper.afterValues.add(getCurrentTS());
        mappedOper.afterValues.addAll(afterValues);        
    }

	@Override
	public void bindMappedUpdate(DML mappedOper, List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException {
        ConsolidatorDstTable dstTable = (ConsolidatorDstTable) mappedOper.tbl;
        
        mappedOper.beforeValues.clear();
        mappedOper.beforeValues.add(dstTable.id.deviceUUID);
        mappedOper.beforeValues.add(dstTable.id.deviceName);
        //Add null for synclite_update_timestamp
        mappedOper.beforeValues.add(null);
        mappedOper.beforeValues.addAll(beforeValues);
		
		mappedOper.afterValues.clear();
		mappedOper.afterValues.add(dstTable.id.deviceUUID);
		mappedOper.afterValues.add(dstTable.id.deviceName);
		mappedOper.afterValues.add(getCurrentTS());
		mappedOper.afterValues.addAll(afterValues);
	}

	@Override
	public void bindMappedDelete(DML mappedOper, List<Object> beforeValues) throws SyncLiteException {
        ConsolidatorDstTable dstTable = (ConsolidatorDstTable) mappedOper.tbl;
		mappedOper.beforeValues.clear();
		mappedOper.beforeValues.add(dstTable.id.deviceUUID);
		mappedOper.beforeValues.add(dstTable.id.deviceName);
		mappedOper.beforeValues.add(null);
		mappedOper.beforeValues.addAll(beforeValues);
	}

    private final String getCurrentTS() {
    	LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        String formattedDateTime = now.format(currentDateTimeFormatter);
        return formattedDateTime;
    }

    @Override
    public List<Oper> mapOper(Insert insert) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) insert.tbl);
        List<Object> dstValues = new ArrayList<Object>(insert.afterValues.size() + 3);
        dstValues.add(dstTable.id.deviceUUID);
        dstValues.add(dstTable.id.deviceName);
        dstValues.add(getCurrentTS());
        
        dstValues.addAll(insert.afterValues);
        
        return Collections.singletonList(getMappedInsertOper(dstTable, dstValues, insert.applyInsertIdempotently()));
    }

    public List<Oper> mapOper(LoadFile loadFile) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) loadFile.tbl);

        Map<String, String> defaultValues = new HashMap<String, String>(3);
        defaultValues.put("synclite_device_id", dstTable.id.deviceUUID);
        defaultValues.put("synclite_device_name", dstTable.id.deviceName);
       	defaultValues.put("synclite_update_timestamp", getCurrentTS());       

        return Collections.singletonList(new LoadFile(dstTable, defaultValues, loadFile.dataFilePath, loadFile.format, loadFile.delimiter, loadFile.nullString, loadFile.header, loadFile.quote, loadFile.escape));
    }

    @Override
    public List<Oper> mapOper(Update update) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) update.tbl);

        List<Object> dstBeforeValues = new ArrayList<Object>(update.beforeValues.size() + 3);
        dstBeforeValues.add(dstTable.id.deviceUUID);
        dstBeforeValues.add(dstTable.id.deviceName);
        //Add null for synclite_update_timestamp
        dstBeforeValues.add(null);
        dstBeforeValues.addAll(update.beforeValues);

        List<Object> dstAfterValues = new ArrayList<Object>(update.afterValues.size() + 3);
        dstAfterValues.add(dstTable.id.deviceUUID);
        dstAfterValues.add(dstTable.id.deviceName);
        dstAfterValues.add(getCurrentTS());
        dstAfterValues.addAll(update.afterValues);

		if (this.sqlGenerator.isUpdateAllowed()) {
	        return Collections.singletonList(new Update(dstTable, dstBeforeValues, dstAfterValues));
		} else {
			//TODO
	        return Collections.singletonList(new DeleteInsert(dstTable, dstBeforeValues, dstAfterValues));
		}
    }

    @Override
    public List<Oper> mapOper(Delete delete) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) delete.tbl);

        List<Object> dstBeforeValues = new ArrayList<Object>(delete.beforeValues.size() + 2);
        dstBeforeValues.add(dstTable.id.deviceUUID);
        dstBeforeValues.add(dstTable.id.deviceName);
        dstBeforeValues.add(null);
        dstBeforeValues.addAll(delete.beforeValues);
        
        return Collections.singletonList(new Delete(dstTable, dstBeforeValues));
    }

    @Override
    public List<Oper> mapOper(CreateTable createTableOper) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) createTableOper.tbl);

		Table checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(createTableOper.tbl.id.deviceUUID, createTableOper.tbl.id.deviceName, this.dstIndex));				
		DstObjectInitMode objectInitMode = ConfLoader.getInstance().getDstObjectInitMode(dstIndex);
		if (createTableOper.tbl == checkpointTable) {
			objectInitMode = DstObjectInitMode.TRY_CREATE_DELETE_DATA;
		}
		List<Oper> opers = new ArrayList<Oper>();
		switch (objectInitMode) {
		case OVERWRITE_OBJECT:
		case TRY_CREATE_DELETE_DATA:
		case TRY_CREATE_APPEND_DATA:
	        //return Collections.singletonList(new CreateTable(dstTable));
	        opers.add(new CreateTable(dstTable));
	        //Add column opers for each column
	        for (Column c : dstTable.columns) {
	            if (!c.isSystemColumn()) {
	                opers.add(new AddColumn(dstTable, c));
	            }
	        }
	        break;
		default:			
		}
		
		switch (objectInitMode) {
		case OVERWRITE_OBJECT:
		case TRY_CREATE_DELETE_DATA:
		case DELETE_DATA:
			opers.add(new TruncateTable(dstTable));
			break;
		default:	
		}
        return opers;
    }

    @Override
    public List<Oper> mapOper(DropTable dropTableOper) {
        return mapOper(new TruncateTable(dropTableOper.tbl));
    }


    @Override
    public List<Oper> mapOper(RenameColumn renameColumnOper) {
        List<Oper> opers = new ArrayList<Oper>();
        Column colToRename = renameColumnOper.columns.get(0);
        if (!ConfLoader.getInstance().isAllowedColumn(dstIndex, renameColumnOper.tbl.id.table, renameColumnOper.columns.get(0).column)) {
        	return Collections.EMPTY_LIST;
        }
        Column newCol = new Column(colToRename.cid, colToRename.column, colToRename.type, colToRename.isNotNull, colToRename.defaultValue, colToRename.pkIndex, colToRename.isAutoIncrement);
        opers.addAll(mapOper(new AddColumn(renameColumnOper.tbl, newCol)));
        opers.addAll(mapOper(new CopyColumn(renameColumnOper.tbl, colToRename, newCol)));
        return opers;
    }

    @Override
    public List<Oper> mapOper(RenameTable renameTableOper) {
        List<Oper> opers = new ArrayList<Oper>();
        opers.addAll(mapOper(new CreateTable(renameTableOper.newTable)));
        opers.addAll(mapOper(new CopyTable(renameTableOper.newTable, renameTableOper.oldTable)));
        return opers;
    }


    @Override
    public List<Oper> mapOper(DropColumn dropColumnOper) throws SyncLiteException {
    	//
    	//Do nothing for drop column in consolidation mode as other devices may still be using the column.
   		//
    	return Collections.EMPTY_LIST;
    }

	public List<Oper> mapOper(DeleteIfPredicate sqlStmt) {
		ConsolidatorDstTable tbl = null;
		if (sqlStmt.tbl != null) {
			tbl = mapTable((ConsolidatorSrcTable) sqlStmt.tbl);
		}
		
		//add additional predicates 
		
		String mappedPredicate = sqlStmt.predicate + " AND synclite_device_name = '" + tbl.id.deviceName + "' AND synclite_device_id = '" + tbl.id.deviceUUID + "'";
		
		return Collections.singletonList(new DeleteIfPredicate(tbl, sqlStmt.sql, mappedPredicate));
	}
   
    public List<Oper> mapOper(AlterColumn alterColumn) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) alterColumn.tbl);
        List<Column> dstColumnsToAlter = new ArrayList<Column>(alterColumn.columns.size());
        for (Column c : alterColumn.columns) {
        	if (! ConfLoader.getInstance().isAllowedColumn(dstIndex, alterColumn.tbl.id.table, c.column)) {
        		continue;
        	}
        	//
        	//Map to best effort text data type
        	//We keep it conservative for consolidation usecase as data is coming from multiple devices
        	//Hence, we change the data type to text on any attempt to change the data type.        	
        	//
        	Column mappedCol = mapColumn(alterColumn.tbl.id, c);
        	c.type = dataTypeMapper.getBestEffortTextDataType();
            dstColumnsToAlter.add(mappedCol);
        }
        if (dstColumnsToAlter.isEmpty()) {
        	return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(new AlterColumn(dstTable, dstColumnsToAlter.get(0)));
    }
}
