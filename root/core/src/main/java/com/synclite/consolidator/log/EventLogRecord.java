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
	
    public EventLogRecord(long changeNumber, long txnChangeNumber, long commitId, String sql, long argCnt, List<Object> argValues) throws SyncLiteException {
    	super(changeNumber, txnChangeNumber, commitId, sql, argCnt, argValues);
    	this.sql = sql.strip();
    	if ((this.opType == OperType.INSERT) && (argCnt == 0)) {    		
			//
			//DISCLAIMER: THIS IS ONLY TESTING PATH. WE OFFICIALLY SUPPORT ONLY PREPARED STATEMENT BASED INSERTS for TELEMETRY DEVICES.
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

	private boolean parseDML(String[] tokens) throws SyncLiteException {
        try {
            if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
            	String[] tabTokens;
            	String[] parts = tokens[2].split("\\(", 2);
            	if (parts.length > 1) {
            	    // If "(" is present, take content before "(" and split it using dot as the delimiter
            	    String contentBeforeParenthesis = parts[0];
            	    tabTokens = contentBeforeParenthesis.split("\\.");
            	} else {
            	    // If "(" is not present, use the entire string
            	    tabTokens = tokens[2].split("\\.");
            	}
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		this.opType = OperType.INSERT;
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		this.opType = OperType.INSERT;
            	}
            	
            	if (opType == OperType.INSERT) {
            	    // Create a Matcher object using the compiled pattern
                    Matcher matcher = INSERT_WITH_COLLIST_PATTERN.matcher(sql.strip());
                    // Check if the input string matches the pattern
                    if (matcher.matches()) {
                        // Extract the content inside the brackets after the table name
                        String columnNames = matcher.group(2);

                        // Remove whitespaces from the column names
        				columnNames = columnNames.replaceAll("\\s+", "");

        				String[] columns = columnNames.split(",");
        				Integer idx = 0;
        				this.colMap = new HashMap<String, Integer>();
        				for (String column : columns) {
        					//Add to argMap
        					this.colMap.put(column.toLowerCase(), idx);
        					++idx;
        				}
                    }
                    /*
            		if (! tokens[3].equalsIgnoreCase("VALUES")) {
            			//
            			//If this token is not VALUES then column list is specified in the INSERT. We need to parse it and 
            			//populate colMap.
            			//
            			//Get the column list between first opening bracket and first closing bracket            		
            			int openBracketIndex = sql.indexOf('(');
            			// Find the index of the first closing bracket after the opening bracket
            			int closeBracketIndex = sql.indexOf(')', openBracketIndex);

            			// Check if both opening and closing brackets are found
            			if (openBracketIndex != -1 && closeBracketIndex != -1) {
            				// Extract content between the first opening bracket and the first closing bracket
            				String columnNames = sql.substring(openBracketIndex + 1, closeBracketIndex);

            				// Remove whitespaces from the column names
            				columnNames = columnNames.replaceAll("\\s+", "");

            				String[] columns = columnNames.split(",");
            				Integer idx = 0;
            				this.colMap = new HashMap<String, Integer>();
            				for (String column : columns) {
            					//Add to argMap
            					this.colMap.put(column.toLowerCase(), idx);
            					++idx;
            				}
            			}
            		}*/
            		return true;
            	}
            } else if (tokens[0].equalsIgnoreCase("UPDATE")) { 
            	String[] tabTokens = tokens[1].split("\\.");
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		this.opType = OperType.UPDATE;
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		this.opType = OperType.UPDATE;
            	}
            } else if (tokens[0].equalsIgnoreCase("DELETE") && tokens[1].equalsIgnoreCase("FROM")) { 
            	String[] tabTokens = tokens[2].split("\\.");
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		if (this.argCnt > 0) {
            			this.opType = OperType.DELETE;
            		} else {
            			this.opType = OperType.DELETE_IF_PREDICATE;
            		}
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		if (this.argCnt > 0) {
            			this.opType = OperType.DELETE;
            		} else {
            			this.opType = OperType.DELETE_IF_PREDICATE;
            		}
            	}
            } else if (tokens[0].equalsIgnoreCase("COPY")) {
            	String[] tabTokens = tokens[1].split("\\.");
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		this.opType = OperType.LOAD;            		
            		return true;
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		this.opType = OperType.LOAD;
            		return true;
            	}            	
            } else if(tokens[0].equalsIgnoreCase("MINUS")) {
            	String[] tabTokens = tokens[1].split("\\.");
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		this.opType = OperType.MINUS;	
            		return true;
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		this.opType = OperType.MINUS;
            		return true;
            	}            	
            } else if(tokens[0].equalsIgnoreCase("FINISHBATCH")) {
            	String[] tabTokens = tokens[1].split("\\.");
            	if (tabTokens.length == 1) {
            		this.databaseName = "main";
            		this.tableName = tabTokens[0];
            		this.opType = OperType.FINISHBATCH;	
            		return true;
            	} else if (tabTokens.length == 2) {
            		this.databaseName = tabTokens[0];
            		this.tableName = tabTokens[1];
            		this.opType = OperType.FINISHBATCH;
            		return true;
            	}
            }
        } catch (NullPointerException e) {
            throw new SyncLiteException("Failed to parse SQL statement " + this.sql, e);
        }
        return false;
    }
    
	@Override
    protected void parse() throws SyncLiteException {
        if (sql == null) {
            return;
        }
        String sqlToParse = sql.trim().replace("\r\n", " ");
        String[] tokens = sqlToParse.split("[\\s+]");
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
