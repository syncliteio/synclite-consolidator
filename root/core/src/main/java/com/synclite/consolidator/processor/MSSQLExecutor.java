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

public class MSSQLExecutor extends JDBCExecutor {

	public MSSQLExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
    	String errorMsg = e.getMessage();
		if (errorMsg.contains("Msg 2627")) {
    		if (errorMsg.contains("Violation of PRIMARY KEY constraint")) {
    			return true;
    		}
    	}
		return false;
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("2714") && e.getMessage().contains("There is already an object named") && e.getMessage().contains("in the database")) {
			return true;
		}
		return false;
	}

}
