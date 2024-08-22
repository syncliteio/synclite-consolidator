package com.synclite.consolidator.schema;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;

public class EventStreamingTableMapper extends TableMapper {

    protected EventStreamingTableMapper(int dstIndex) {
        super(dstIndex);
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
        Column deviceIDCol = new Column(dstTable.columns.size(), "synclite_device_id", mapType(new DataType("text", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumnIfNotExists(deviceIDCol);

        Column deviceNameCol = new Column(dstTable.columns.size()+1, "synclite_device_name", mapType(new DataType("text", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumnIfNotExists(deviceNameCol);

        Column eventTypeCol = new Column(dstTable.columns.size()+2, "synclite_event_type", mapType(new DataType("text", JDBCType.VARCHAR, StorageClass.TEXT)), 1, null, 0, 0);
        dstTable.addColumnIfNotExists(eventTypeCol);

        for (Column srcColumn : srcTable.columns) {
            Column dstColumn = mapColumn(srcTable.id, srcColumn);
            dstTable.addColumn(dstColumn);
        }
        
        tableMap.put(srcTable, dstTable);
        return dstTable;
    }

    @Override
    protected TableID mapTableID(TableID srcTableID) {
        return TableID.from(srcTableID.deviceUUID, srcTableID.deviceName, this.dstIndex, ConfLoader.getInstance().getDstDatabase(dstIndex), ConfLoader.getInstance().getDstSchema(dstIndex), srcTableID.table);
    }

    @Override
    public Column mapColumn(TableID tableID, Column srcColumn) {
        Column dstColumn = new Column(
                srcColumn.cid,
                srcColumn.column,
                mapType(srcColumn.type),
                0,
                null,
                0,
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
        dstValues.add("INSERT");
        dstValues.addAll(insert.afterValues);

		return getMappedInsertOper(dstTable, dstValues, insert.applyInsertIdempotently());
    }

    @Override
    public void bindMappedInsert(DML mappedOper, List<Object> args) throws SyncLiteException {
        ConsolidatorDstTable dstTable = (ConsolidatorDstTable) mappedOper.tbl;
        
        List<Object> dstValues = mappedOper.afterValues;
        dstValues.clear();        
        dstValues.add(dstTable.id.deviceUUID);
        dstValues.add(dstTable.id.deviceName);
        dstValues.add("INSERT");
        dstValues.addAll(mappedOper.afterValues);
        
        mappedOper.afterValues = dstValues;
    }

	@Override
	public void bindMappedUpdate(DML mappedOper, List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException {
		throw new IllegalStateException("Not Implemented");
	}

	@Override
	public void bindMappedDelete(DML mappedOper, List<Object> beforeValues) throws SyncLiteException {
		throw new IllegalStateException("Not Implemented");
	}

    @Override
    public List<Oper> mapOper(Insert insert) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) insert.tbl);

        List<Object> dstValues = new ArrayList<Object>(insert.afterValues.size() + 3);
        dstValues.add(dstTable.id.deviceUUID);
        dstValues.add(dstTable.id.deviceName);
        dstValues.add("INSERT");
        dstValues.addAll(insert.afterValues);

        return Collections.singletonList(getMappedInsertOper(dstTable, dstValues, insert.applyInsertIdempotently()));
    }


    @Override
    public List<Oper> mapOper(LoadFile loadFile) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) loadFile.tbl);

        Map<String, String> defaultValues = new HashMap<String, String>(3);
        defaultValues.put("synclite_device_id", dstTable.id.deviceUUID);
        defaultValues.put("synclite_device_name", dstTable.id.deviceName);
        defaultValues.put("synclite_event_type", "INSERT");

        return Collections.singletonList(new LoadFile(dstTable, defaultValues, loadFile.dataFilePath, loadFile.format, loadFile.delimiter, loadFile.nullString, loadFile.header, loadFile.quote, loadFile.escape));
    }

    @Override
    public List<Oper> mapOper(Update update) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) update.tbl);
        List<Object> dstAfterValues = new ArrayList<Object>(update.afterValues.size() + 3);

        dstAfterValues.add(dstTable.id.deviceUUID);
        dstAfterValues.add(dstTable.id.deviceName);
        dstAfterValues.add("UPDATE");
        dstAfterValues.addAll(update.afterValues);
        
        return Collections.singletonList(new Insert(dstTable, dstAfterValues, false));
    }

    @Override
    public List<Oper> mapOper(Delete delete) throws SyncLiteException {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) delete.tbl);

        List<Object> dstBeforeValues = new ArrayList<Object>(delete.beforeValues.size() + 3);
        dstBeforeValues.add(dstTable.id.deviceUUID);
        dstBeforeValues.add(dstTable.id.deviceName);
        dstBeforeValues.add("DELETE");
        dstBeforeValues.addAll(delete.beforeValues);

        return Collections.singletonList(new Insert(dstTable, dstBeforeValues, false));
    }

    @Override
    public List<Oper> mapOper(TruncateTable truncate) {
        ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) truncate.tbl);
        List<Object> values = new ArrayList<Object>(truncate.tbl.columns.size() + 3);
        values.add(dstTable.id.deviceUUID);
        values.add(dstTable.id.deviceName);
        values.add("TRUNCATE");

        for (int i=0; i < values.size(); ++i) {
            values.add(null);
        }
        return Collections.singletonList(new Insert(dstTable, values, false));
    }
}
