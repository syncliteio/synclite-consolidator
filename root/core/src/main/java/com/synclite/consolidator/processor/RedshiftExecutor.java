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
