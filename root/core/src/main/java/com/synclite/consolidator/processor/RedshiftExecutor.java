package com.synclite.consolidator.processor;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class RedshiftExecutor extends JDBCExecutor {

	public RedshiftExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		//duplicate key value violates unique constraint
		String errorMsg = e.getMessage();
		if (errorMsg.contains("duplicate key value violates unique constraint")) {
			return true;
		}
		return false;
	}
	
	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("relation") && e.getMessage().contains("already exists")) {
			return true;
		}
		return false;
	}
}
