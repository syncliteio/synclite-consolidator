package com.synclite.consolidator.log;

import java.util.List;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.oper.OperType;

public class CommandLogRecord {

    public class DDLInfo {
        public OperType ddlType;
        public String databaseName;
        public String tableName;
        public String columnName;
        public String oldTableName;
        public String oldColumnName;
        public String colDef;
        
        public DDLInfo(OperType ddlType, String databaseName, String tableName, String oldTableName, String columnName, String oldColumnName, String colDef) {
            this.ddlType = ddlType;
            this.databaseName = databaseName;
            this.tableName= tableName;
            this.columnName = columnName;
            this.oldTableName = oldTableName;
            this.oldColumnName = oldColumnName;
            this.colDef = colDef;
        }

        @Override
        public String toString() {
            return "DDL Type : " + ddlType + ", database : " + databaseName + ", table : " + tableName + ", column : " + columnName + ", oldTableName : " + oldTableName + ", oldColumnName : " + oldColumnName;
        }
    }

    public long changeNumber;
    public long txnChangeNumber;
    public long commitId;
    public String sql;
    public long argCnt;
    public List<Object> argValues;
    public DDLInfo ddlInfo;

    private CommandLogRecord(long changeNumber, long txnChangeNumber, long commitId, String sql, long argCnt, List<Object> argValues, DDLInfo ddlInfo) {
        this.changeNumber = changeNumber;
        this.txnChangeNumber = txnChangeNumber;
        this.commitId = commitId;
        this.sql = sql;
        this.argCnt = argCnt;
        this.argValues = argValues;
        this.ddlInfo = ddlInfo;
    }

    public CommandLogRecord(long changeNumber, long txnChangeNumber, long commitId, String sql, long argCnt, List<Object> argValues) throws SyncLiteException {
    	this(changeNumber, txnChangeNumber, commitId, sql, argCnt, argValues, null);
    	parse();
    }

	public CommandLogRecord(long changeNumber, long txnChangeNumber, long commitId, CommandLogRecord rec, long argCnt, List<Object> argValues) {
    	this(changeNumber, txnChangeNumber, commitId, rec.sql, argCnt, argValues, rec.ddlInfo);
    }
    
    public final boolean isDDL() {
        return (this.ddlInfo != null);
    }
    
    public final boolean isNoOp() {
    	if ((this.sql != null) && (this.sql.isBlank()) && (this.argCnt == 0)) {
    		return true;
    	}
    	return false;
    }
    protected void parseDDL(String[] tokens) throws SyncLiteException {
        try {
            if (tokens[0].equalsIgnoreCase("CREATE") && tokens[1].equalsIgnoreCase("TABLE")) {
                //create table
                if (tokens[2].equalsIgnoreCase("IF") && tokens[3].equalsIgnoreCase("NOT") && tokens[4].equalsIgnoreCase("EXISTS")) {
                	//CREATE TABLE IF NOT EXISTS main.t1
                	//CREATE TABLE IF NOT EXISTS t1
                	String[] fullTableName = getFullTableName(tokens[5]);
                	ddlInfo = new DDLInfo(OperType.CREATETABLE, fullTableName[0], fullTableName[1], null, null, null, null);
                } else {
                	//
                	//Check if table name contains .
                	//CREATE TABLE main.t1
                	//CREATE TABLE t1
                	//
                	String[] fullTableName = getFullTableName(tokens[2]);
                	ddlInfo = new DDLInfo(OperType.CREATETABLE, fullTableName[0], fullTableName[1], null, null, null, null);
                }                
            } else if (tokens[0].equalsIgnoreCase("ALTER") && tokens[1].equalsIgnoreCase("TABLE")) {
            	String[] fullTableName = getFullTableName(tokens[2]);

            	if (tokens[3].equalsIgnoreCase("ADD") && tokens[4].equalsIgnoreCase("COLUMN")) {
                    //ALTER TABLE main.t1 ADD COLUMN col1
                    //ALTER TABLE t1 ADD COLUMN col1
                	StringBuilder colDef = new StringBuilder();
                	for (int i = 6; i < tokens.length; ++i) {
                		colDef.append(" ");
                		colDef.append(tokens[i]);
                	}
                	ddlInfo = new DDLInfo(OperType.ADDCOLUMN, fullTableName[0], fullTableName[1], null, tokens[5], null, colDef.toString());
                } else if (tokens[3].equalsIgnoreCase("ADD")) {
                    //ALTER TABLE main.t1 ADD col1
                    //ALTER TABLE t1 ADD col1
                	StringBuilder colDef = new StringBuilder();
                	for (int i = 5; i < tokens.length; ++i) {
                		colDef.append(" ");
                		colDef.append(tokens[i]);
                	}
                	ddlInfo = new DDLInfo(OperType.ADDCOLUMN, fullTableName[0], fullTableName[1], null, tokens[4], null, colDef.toString());
                } else if (tokens[3].equalsIgnoreCase("DROP") && tokens[4].equalsIgnoreCase("COLUMN")) {
                    //ALTER TABLE main.t1 DROP COLUMN col1
                    //ALTER TABLE t1 DROP COLUMN col1
                	ddlInfo = new DDLInfo(OperType.DROPCOLUMN, fullTableName[0], fullTableName[1], null, tokens[5], null, null);                	
                } else if (tokens[3].equalsIgnoreCase("DROP")) {
                    //ALTER TABLE main.t1 DROP COLUMN col1
                    //ALTER TABLE t1 DROP COLUMN col1
                	ddlInfo = new DDLInfo(OperType.DROPCOLUMN, fullTableName[0], fullTableName[1], null, tokens[4], null, null);                	
                } else if (tokens[3].equalsIgnoreCase("RENAME") && tokens[4].equalsIgnoreCase("COLUMN") && tokens[6].equalsIgnoreCase("TO")) {
                    //ALTER TABLE main.t1 RENAME COLUMN col1 TO col2
                    ddlInfo = new DDLInfo(OperType.RENAMECOLUMN, fullTableName[0], fullTableName[1], null, tokens[7], tokens[5], null);
                } else if (tokens[3].equalsIgnoreCase("RENAME") && tokens[5].equalsIgnoreCase("TO")) {
                    //ALTER TABLE main.t1 RENAME col1 TO col2
                    ddlInfo = new DDLInfo(OperType.RENAMECOLUMN, fullTableName[0], fullTableName[1], null, tokens[6], tokens[4], null);
                } else if (tokens[3].equalsIgnoreCase("RENAME") && tokens[4].equalsIgnoreCase("TO")) {
                    //ALTER TABLE main.t1 RENAME TO t2
                    ddlInfo = new DDLInfo(OperType.RENAMETABLE, fullTableName[0], tokens[5] , fullTableName[1], null, null, null);
                } else if (tokens[3].equalsIgnoreCase("ALTER") && tokens[4].equalsIgnoreCase("COLUMN")) {
                    //ALTER TABLE main.t1 ALTER COLUMN col1 <NEW COLUMN DEF>
                	StringBuilder colDef = new StringBuilder();
                	for (int i = 6; i < tokens.length; ++i) {
                		colDef.append(" ");
                		colDef.append(tokens[i]);
                	}
                    ddlInfo = new DDLInfo(OperType.ALTERCOLUMN, fullTableName[0], fullTableName[1], null, tokens[5], null, colDef.toString());
                } else if (tokens[5].equalsIgnoreCase("ALTER")) {
                    //ALTER TABLE main.t1 ALTER col1 <NEW COLUMN DEF>
                	StringBuilder colDef = new StringBuilder();
                	for (int i = 5; i < tokens.length; ++i) {
                		colDef.append(" ");
                		colDef.append(tokens[i]);
                	}
                    ddlInfo = new DDLInfo(OperType.ALTERCOLUMN, fullTableName[0], fullTableName[1], null, tokens[4], null, colDef.toString());
                }
            } else if (tokens[0].equalsIgnoreCase("DROP") && tokens[1].equalsIgnoreCase("TABLE")) {
                //drop table
                if (tokens[2].equalsIgnoreCase("IF") && tokens[3].equalsIgnoreCase("EXISTS")) {
                	//DROP TABLE IF EXISTS main.t1
                	//DROP TABLE IF EXISTS t1
                	String[] fullTableName = getFullTableName(tokens[4]);
                	ddlInfo = new DDLInfo(OperType.DROPTABLE, fullTableName[0], fullTableName[1], null, null, null, null);
                } else {
                	//
                	//Check if table name contains .
                	//DROP TABLE main.t1
                	//CREATE TABLE t1
                	//
                	String[] fullTableName = getFullTableName(tokens[2]);
                	ddlInfo = new DDLInfo(OperType.DROPTABLE, fullTableName[0], fullTableName[1], null, null, null, null);
                }
            } else if (tokens[0].equalsIgnoreCase("REFRESH") && tokens[1].equalsIgnoreCase("TABLE")) {
            	String[] fullTableName = getFullTableName(tokens[2]);
            	ddlInfo = new DDLInfo(OperType.REFRESHTABLE, fullTableName[0], fullTableName[1], null, null, null, null);
            } else if (tokens[0].equalsIgnoreCase("PUBLISH") && tokens[1].equalsIgnoreCase("COLUMN") && tokens[2].equalsIgnoreCase("LIST")) {
            	String[] fullTableName = getFullTableName(tokens[2]);
            	ddlInfo = new DDLInfo(OperType.PUBLISHCOLUMNLIST, fullTableName[0], fullTableName[1], null, tokens[4], null, null);
            }
        } catch (NullPointerException e) {
            throw new SyncLiteException("Failed to parse a DDL statement : " + this.sql, e);
        }
    }

    private String[] getFullTableName(String name) {
    	String[] tbl = new String[2];
    	tbl[0] = "main";
    	tbl[1] = "";
    	
    	String[] tokens = name.split("\\.");
    	if (tokens.length == 2) {
    		tbl[0] = tokens[0];
    		tbl[1] = tokens[1];	
    	} else {
    		tbl[1] = name;
    	}
    	
    	if (tbl[1].contains("(")) {
			tbl[1] = tbl[1].substring(0, tbl[1].indexOf("("));
		} 
    	
    	return tbl;
    }
    protected void parse() throws SyncLiteException {
        if (sql == null) {
            return;
        }
        String sqlToParse = sql.trim().replace("\r\n", " ");
        //
        //Split on white spaces.
        //
        String[] tokens = sqlToParse.split("\\s+");

        if (tokens.length < 2) {
            return;
        }
        parseDDL(tokens);
    }

    public final boolean isBegin() {
        return sql.equalsIgnoreCase("BEGIN");
    }

    public final boolean isCommit() {
        if (sql == null) {
            return false;
        }
        return sql.equalsIgnoreCase("COMMIT");
    }

    public final boolean isRollback() {
        if (sql == null) {
            return false;
        }
        return sql.equalsIgnoreCase("ROLLBACK");
    }

    @Override
    public String toString() {
        return "commit id : " + commitId + ", change number : " + changeNumber + ", sql : " + sql;
    }
}
