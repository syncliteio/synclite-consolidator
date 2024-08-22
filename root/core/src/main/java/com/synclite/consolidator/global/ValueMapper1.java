package com.synclite.consolidator.global;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.TableID;

public interface ValueMapper1 {
	public Object mapValue(TableID tableID, Column col, Object value) throws SyncLiteException;

}
