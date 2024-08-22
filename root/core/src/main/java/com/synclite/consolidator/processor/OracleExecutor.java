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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;

public class OracleExecutor extends JDBCExecutor {

	public OracleExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);
	}

	@Override
	public boolean isDuplicateKeyException(Exception e) {
		if (e instanceof java.sql.SQLIntegrityConstraintViolationException) {
			// Check the SQL state and error message to identify the duplicate key violation
			String sqlState = ((java.sql.SQLIntegrityConstraintViolationException) e).getSQLState();
			String errorMessage = e.getMessage();

			if ("23000".equals(sqlState) && errorMessage.contains("unique constraint")) {
				return true;
			}
		}
		return false;	
	}

	@Override
	protected boolean isTableAlreadyExistsException(Exception e) {
		if (e.getMessage().contains("ORA-00955") && e.getMessage().contains("name is already used by an existing object")) {
			return true;
		}
		return false;
	}

	//For oracle date comes as a timestamp. Try to set a timestamp.
	@Override
	protected void setDate(PreparedStatement pstmt, int i, Object o) throws SQLException {
		super.setTimestamp(pstmt, i, o);
	}
}

