package com.synclite.consolidator.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstObjectInitMode;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.DML;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.DropTable;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.LoadFile;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.TruncateTable;
import com.synclite.consolidator.oper.Update;

public class ReplicationTableMapper extends TableMapper {

	protected ReplicationTableMapper(int dstIndex) {
		super(dstIndex);
	}

	@Override
	protected TableID mapTableID(TableID srcTableID) {
		String schemaName;
		switch (ConfLoader.getInstance().getDstDeviceSchemaNamePolicy(dstIndex)) {
		case SYNCLITE_DEVICE_ID:
			schemaName = "synclite_" + uuidRemoveSpecialChars(srcTableID.deviceUUID);
			break;
		case SYNCLITE_DEVICE_NAME:
			if (!srcTableID.deviceName.isEmpty()) {
				schemaName = "synclite_" + srcTableID.deviceName;
			} else {
				schemaName = "synclite_" + uuidRemoveSpecialChars(srcTableID.deviceUUID);
			}
			break;
		case SYNCLITE_DEVICE_ID_AND_NAME:
			if (!srcTableID.deviceName.isEmpty()) {
				schemaName = "synclite_"  + uuidRemoveSpecialChars(srcTableID.deviceUUID) + "_" + srcTableID.deviceName; 
			} else {
				schemaName = "synclite_"  + uuidRemoveSpecialChars(srcTableID.deviceUUID);
			}
			break;
		case SPECIFIED_DST_CATALOG_SCHEMA:
			schemaName = ConfLoader.getInstance().getDstSchema(dstIndex);
			break;
		default:
			schemaName = "synclite_" + uuidRemoveSpecialChars(srcTableID.deviceUUID);
			break;
		}
		
		String mappedTableName = ConfLoader.getInstance().getMappedTableName(dstIndex, srcTableID.table);
		return TableID.from(srcTableID.deviceUUID, srcTableID.deviceName, this.dstIndex, ConfLoader.getInstance().getDstDatabase(dstIndex), schemaName, mappedTableName);    
	}

	private final String uuidRemoveSpecialChars(String uuid) {
		return uuid.replace("-", "");
	}


	@Override
	public Insert mapInsert(Insert insert) throws SyncLiteException {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) insert.tbl);		
		return getMappedInsertOper(dstTable, insert.afterValues, insert.applyInsertIdempotently());
	}

	@Override
	public void bindMappedInsert(DML mappedOper, List<Object> args) throws SyncLiteException {
		//We are doing this through a virtual method as in case of DeleteInsert operator ( that extends Insert operation)
		//We need to replace the args in the contained delete and insert operators. 
		mappedOper.replaceArgs(args, args);
	}

	@Override
	public void bindMappedUpdate(DML mappedOper, List<Object> beforeValues, List<Object> afterValues) throws SyncLiteException {
		//We are doing this through a virtual method as in case of DeleteInsert operator ( that extends Insert operation)
		//We need to replace the args in the contained delete and insert operators. 
		mappedOper.replaceArgs(beforeValues, afterValues);
	}

	@Override
	public void bindMappedDelete(DML mappedOper, List<Object> args) throws SyncLiteException {
		mappedOper.replaceArgs(args, args);
	}

	
	@Override
	public List<Oper> mapOper(Insert insert) throws SyncLiteException {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) insert.tbl);
        return Collections.singletonList(getMappedInsertOper(dstTable, insert.afterValues, insert.applyInsertIdempotently()));
	}

	@Override
	public List<Oper> mapOper(LoadFile loadFile) {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) loadFile.tbl);        
		return Collections.singletonList(new LoadFile(dstTable, Collections.EMPTY_MAP, loadFile.dataFilePath, loadFile.format, loadFile.delimiter, loadFile.nullString, loadFile.header, loadFile.quote, loadFile.escape));
	}

	@Override
	public List<Oper> mapOper(Update update) throws SyncLiteException {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) update.tbl);
		if (this.sqlGenerator.isUpdateAllowed()) {
			return Collections.singletonList(new Update(dstTable, update.beforeValues, update.afterValues));
		} else {
			return Collections.singletonList(new DeleteInsert(dstTable, update.beforeValues, update.afterValues));
		}
	}

	@Override
	public List<Oper> mapOper(Delete delete) {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) delete.tbl);
		List<Object> dstBeforeValues = delete.beforeValues;
		return Collections.singletonList( new Delete(dstTable, dstBeforeValues));
	}

	@Override
	public List<Oper> mapOper(CreateTable create) {
		ConsolidatorDstTable dstTable = mapTable((ConsolidatorSrcTable) create.tbl);
		
		Table checkpointTable = ConsolidatorSrcTable.from(SyncLiteConsolidatorInfo.getCheckpointTableID(create.tbl.id.deviceUUID, create.tbl.id.deviceName, this.dstIndex));				
		DstObjectInitMode objectInitMode = ConfLoader.getInstance().getDstObjectInitMode(dstIndex);
		if (create.tbl == checkpointTable) {
			objectInitMode = DstObjectInitMode.DELETE_DATA;
		}

		switch (objectInitMode) {
		case OVERWRITE_OBJECT:
			List<Oper> mappedOpers = new ArrayList<Oper>();
			mappedOpers.add(new DropTable(dstTable));
			mappedOpers.add(new CreateTable(dstTable));
			return mappedOpers;
		case TRY_CREATE_DELETE_DATA:
			mappedOpers = new ArrayList<Oper>();
			mappedOpers.add(new CreateTable(dstTable));
			mappedOpers.add(new TruncateTable(dstTable));
			return mappedOpers;
		case DELETE_DATA:
			mappedOpers = new ArrayList<Oper>();
			mappedOpers.add(new TruncateTable(dstTable));
			return mappedOpers;
		case TRY_CREATE_APPEND_DATA:
			return Collections.singletonList(new CreateTable(dstTable));
		case APPEND_DATA:
			return Collections.EMPTY_LIST;

		}
		return Collections.EMPTY_LIST;
	}

}
