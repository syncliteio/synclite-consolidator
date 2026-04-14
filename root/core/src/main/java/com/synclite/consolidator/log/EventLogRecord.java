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

package com.synclite.consolidator.log;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.oper.OperType;

public class EventLogRecord extends CommandLogRecord {

    // Define the regular expression pattern for the allowed INSERT syntaxes
    private static final String INSERT_WITH_COLLIST_PATTERN_STRING = "INSERT\\s+INTO\\s+(\\w+\\.)?\\w+\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)";

    // Compile the pattern only once
    private static final Pattern INSERT_WITH_COLLIST_PATTERN = Pattern.compile(INSERT_WITH_COLLIST_PATTERN_STRING, Pattern.CASE_INSENSITIVE);

	public String databaseName;
	public String tableName;
	public OperType opType;
	public HashMap<String, Integer> colMap;
	public List<String> whereColNames;
	public List<String> setColNames;
	
    public EventLogRecord(long changeNumber, long txnChangeNumber, long commitId, String sql, long argCnt, List<Object> argValues) throws SyncLiteException {
    	super(changeNumber, txnChangeNumber, commitId, sql, argCnt, argValues);
    	this.sql = sql.strip();
    	if ((this.opType == OperType.INSERT) && (argCnt == 0)) {    		
			//
			//DISCLAIMER: THIS IS ONLY TESTING PATH. WE OFFICIALLY SUPPORT ONLY PREPARED STATEMENT BASED INSERTS for DBLOGGER DEVICES.
			//
			//This is an unprepared INSERT.
			//Try to parse arguments from sql.
    		//
			parseArgs(sql, argValues);
			this.argCnt = argValues.size();
    	}
    }

    public EventLogRecord(long changeNumber, long txnChangeNumber, long commitId, EventLogRecord rec, long argCnt, List<Object> argValues) throws SyncLiteException {
    	super(changeNumber, txnChangeNumber, commitId, rec, argCnt, argValues);
    	this.databaseName = rec.databaseName;
    	this.tableName = rec.tableName;
    	this.opType = rec.opType;
    	this.colMap = rec.colMap;
    	this.whereColNames = rec.whereColNames;
    	this.setColNames = rec.setColNames;
    }

    private final void parseArgs(String sql, List<Object> argValues) {
    	try {
	    	int firstIndex = sql.indexOf("(");
	    	int lastIndex = sql.lastIndexOf(")");
	    	
	    	String argsStr = sql.substring(firstIndex+1, lastIndex);
	    	
			try (Reader in = new StringReader(argsStr)) {
				Iterable<CSVRecord> records = CSVFormat.DEFAULT
						.withDelimiter(',')
						.withNullString("null")
						//.withEscape('\'')
						.withQuote('\'')
						.parse(in);
				
				for (CSVRecord record : records) {
					for (int i = 0; i < record.size(); ++i) {
						argValues.add(record.get(i));
					}
					break;
				}
			}
			
    	} catch(Exception e) {    		
    		//Ignore this and move on with empty args.
    	}

	}

	/**
	 * Extracts database and table name from a potentially qualified table token (e.g., "main.t1" or "t1").
	 * For INSERT, the token may contain "(" (e.g., "t1(col1,col2)"), which is stripped first.
	 * Returns true if a valid table name was extracted, false otherwise.
	 */
	private boolean parseTableName(String tableToken, boolean stripParenthesis) {
		if (stripParenthesis) {
			String[] parts = tableToken.split("\\(", 2);
			tableToken = parts[0];
		}
		String[] tabTokens = tableToken.split("\\.");
		if (tabTokens.length == 1) {
			this.databaseName = "main";
			this.tableName = tabTokens[0];
			return true;
		} else if (tabTokens.length == 2) {
			this.databaseName = tabTokens[0];
			this.tableName = tabTokens[1];
			return true;
		}
		return false;
	}

	private boolean parseDML(String[] tokens) throws SyncLiteException {
        try {
            if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
            	if (!parseTableName(tokens[2], true)) {
            		return false;
            	}
            	this.opType = OperType.INSERT;

            	// Check if INSERT has an explicit column list
            	Matcher matcher = INSERT_WITH_COLLIST_PATTERN.matcher(sql.strip());
            	if (matcher.matches()) {
            		String columnNames = matcher.group(2).replaceAll("\\s+", "");
            		String[] columns = columnNames.split(",");
            		this.colMap = new HashMap<String, Integer>();
            		for (int idx = 0; idx < columns.length; ++idx) {
            			this.colMap.put(columns[idx].toLowerCase(), idx);
            		}
            	}
            	return true;
            } else if (tokens[0].equalsIgnoreCase("UPDATE")) { 
            	if (!parseTableName(tokens[1], false)) {
            		return false;
            	}
            	this.opType = OperType.UPDATE;
            	this.setColNames = parseSetColumns(this.sql);
            	this.whereColNames = parseWhereColumns(this.sql);
            	return true;
            } else if (tokens[0].equalsIgnoreCase("DELETE") && tokens[1].equalsIgnoreCase("FROM")) { 
            	if (!parseTableName(tokens[2], false)) {
            		return false;
            	}
            	if (this.argCnt > 0) {
            		this.opType = OperType.DELETE;
            		this.whereColNames = parseWhereColumns(this.sql);
            	} else {
            		this.opType = OperType.DELETE_IF_PREDICATE;
            	}
            	return true;
            } else if (tokens[0].equalsIgnoreCase("COPY")) {
            	if (!parseTableName(tokens[1], false)) {
            		return false;
            	}
            	this.opType = OperType.LOAD;
            	return true;
            } else if(tokens[0].equalsIgnoreCase("MINUS")) {
            	if (!parseTableName(tokens[1], false)) {
            		return false;
            	}
            	this.opType = OperType.MINUS;
            	return true;
            } else if(tokens[0].equalsIgnoreCase("FINISHBATCH")) {
            	if (!parseTableName(tokens[1], false)) {
            		return false;
            	}
            	this.opType = OperType.FINISHBATCH;
            	return true;
            }
        } catch (NullPointerException e) {
            throw new SyncLiteException("Failed to parse SQL statement " + this.sql, e);
        }
        return false;
    }

    private List<String> parseWhereColumns(String sqlStr) {
    	try {
	    	String upperSql = sqlStr.toUpperCase();
	    	int whereIdx = upperSql.indexOf(" WHERE ");
	    	if (whereIdx < 0) {
	    		return null;
	    	}
	    	String whereClause = sqlStr.substring(whereIdx + 7);
	    	String[] parts = whereClause.split("(?i)\\s+AND\\s+");
	    	List<String> cols = new ArrayList<>();
	    	for (String part : parts) {
	    		part = part.trim();
	    		int eqIdx = part.indexOf('=');
	    		if (eqIdx > 0) {
	    			String colPart = part.substring(0, eqIdx).trim();
	    			colPart = colPart.replaceAll("[\"'`\\[\\]]", "");
	    			int dotIdx = colPart.lastIndexOf('.');
	    			if (dotIdx >= 0) {
	    				colPart = colPart.substring(dotIdx + 1);
	    			}
	    			cols.add(colPart.toLowerCase());
	    		}
	    	}
	    	return cols.isEmpty() ? null : cols;
    	} catch (Exception e) {
    		return null;
    	}
    }

    private List<String> parseSetColumns(String sqlStr) {
    	try {
	    	String upperSql = sqlStr.toUpperCase();
	    	int setIdx = upperSql.indexOf(" SET ");
	    	int whereIdx = upperSql.indexOf(" WHERE ");
	    	if (setIdx < 0) {
	    		return null;
	    	}
	    	String setClause = (whereIdx > 0) ? sqlStr.substring(setIdx + 5, whereIdx) : sqlStr.substring(setIdx + 5);
	    	String[] parts = setClause.split(",");
	    	List<String> cols = new ArrayList<>();
	    	for (String part : parts) {
	    		part = part.trim();
	    		int eqIdx = part.indexOf('=');
	    		if (eqIdx > 0) {
	    			String colPart = part.substring(0, eqIdx).trim();
	    			colPart = colPart.replaceAll("[\"'`\\[\\]]", "");
	    			int dotIdx = colPart.lastIndexOf('.');
	    			if (dotIdx >= 0) {
	    				colPart = colPart.substring(dotIdx + 1);
	    			}
	    			cols.add(colPart.toLowerCase());
	    		}
	    	}
	    	return cols.isEmpty() ? null : cols;
    	} catch (Exception e) {
    		return null;
    	}
    }
    
	@Override
    protected void parse() throws SyncLiteException {
        if (sql == null) {
            return;
        }
        String sqlToParse = sql.trim().replace("\r\n", " ");
        String[] tokens = sqlToParse.split("\\s+");
        if (tokens.length < 2) {
        	//Check for control commands
        	if (tokens[0].equalsIgnoreCase("SHUTDOWN")) {
        		this.databaseName = "main";
        		this.tableName = "dummy";
        		this.opType = OperType.SHUTDOWN;
        	}
            return;
        }
        boolean dmlFound = parseDML(tokens);
        if (!dmlFound) {
        	parseDDL(tokens);
        	if (ddlInfo != null) {
        		this.databaseName = ddlInfo.databaseName;
        		this.tableName = ddlInfo.tableName;        		
        		this.opType = ddlInfo.ddlType;
        	} else {
        		//throw new SyncLiteException("")
        		//Ignore unwanted logs
        		this.opType = OperType.NOOP;
        	}
        }
    }

}
