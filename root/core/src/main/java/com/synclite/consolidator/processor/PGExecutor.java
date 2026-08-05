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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.schema.Column;

public class PGExecutor extends JDBCExecutor {

	public PGExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
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
		String msg = e.getMessage();
		if (msg == null) {
			return false;
		}
		if (msg.contains("relation") && msg.contains("already exists")) {
			return true;
		}
		//Concurrent CREATE TABLE IF NOT EXISTS into the same schema can lose the
		//pg_type catalog race and surface as a unique-violation on
		//pg_type_typname_nsp_index rather than "relation already exists".
		if (msg.contains("pg_type_typname_nsp_index")) {
			return true;
		}
		return false;
	}

	@Override
	protected boolean abortsTxnOnStatementError() {
		//PostgreSQL aborts the entire transaction on any statement error (25P02).
		return true;
	}

	@Override
	public boolean canCreateDatabase() {
		return true;
	}

	/**
	 * PostgreSQL {@code json} / {@code jsonb} / {@code uuid} columns reject text bindings —
	 * the driver sends {@code character varying} and the server refuses with
	 * "column ... is of type json/jsonb/uuid but expression is of type character varying".
	 * These types map to {@link java.sql.JDBCType#VARCHAR} in the type mapper (there is no
	 * dedicated JDBCType for them), so the default {@code bindPrepared} would call
	 * {@code setString}. Bind them as an unspecified-type object instead so PostgreSQL
	 * implicitly casts the text value to the target type.
	 */
	@Override
	protected void bindPrepared(PreparedStatement pstmt, int i, Object o, Column c, boolean isConditionArg) throws SQLException {
		if (isPostgresByteaType(c)) {
			if (o == null) {
				pstmt.setNull(i, Types.BINARY);
			} else {
				pstmt.setBytes(i, coerceBinaryValue(o));
			}
			return;
		}
		if (needsUnspecifiedTypeBinding(c)) {
			if (o == null) {
				pstmt.setNull(i, Types.OTHER);
			} else {
				pstmt.setObject(i, o.toString(), Types.OTHER);
			}
			return;
		}
		super.bindPrepared(pstmt, i, o, c, isConditionArg);
	}

	private static boolean needsUnspecifiedTypeBinding(Column c) {
		if (c == null || c.type == null || c.type.dbNativeDataType == null) {
			return false;
		}
		String nativeType = c.type.dbNativeDataType.trim().toLowerCase();
		return nativeType.equals("json") || nativeType.equals("jsonb") || nativeType.equals("uuid");
	}

	private static boolean isPostgresByteaType(Column c) {
		if (c == null || c.type == null || c.type.dbNativeDataType == null) {
			return false;
		}
		return c.type.dbNativeDataType.trim().equalsIgnoreCase("bytea");
	}

	protected void setDate(PreparedStatement pstmt, int i, Object o) throws SQLException {
		try {
			pstmt.setDate(i, Date.valueOf(o.toString()));
		} catch(Exception e) {
			try {
				//Try parsing as a timestamp first.
				//
				Timestamp ts = Timestamp.valueOf(o.toString());
				java.sql.Date sqlDate = new java.sql.Date(ts.getTime());
				pstmt.setDate(i, sqlDate);
			} catch (Exception e1) {
				try {
					pstmt.setString(i, o.toString() + "::date");
				} catch (Exception e2) {
					pstmt.setObject(i, o);
				}
			}
		}
	}

	protected void setTime(PreparedStatement pstmt, int i, Object o) throws SQLException {
		try {
			pstmt.setTime(i, java.sql.Time.valueOf(o.toString()));
		} catch (Exception e) {
			try {
				//Handle fractional-second time values (e.g. 10:30:00.123456)
				//that java.sql.Time.valueOf rejects.
				//
				Timestamp ts = Timestamp.valueOf("1970-01-01 " + o.toString());
				pstmt.setTime(i, new java.sql.Time(ts.getTime()));
			} catch (Exception e1) {
				//Let PostgreSQL implicitly cast the text to time instead of
				//binding it as character varying.
				//
				pstmt.setObject(i, o.toString(), Types.OTHER);
			}
		}
	}


}
