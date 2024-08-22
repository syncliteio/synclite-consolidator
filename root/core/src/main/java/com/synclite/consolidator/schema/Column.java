package com.synclite.consolidator.schema;

public class Column {
    public long cid;
    public String column;
    public DataType type;
    public int isNotNull;
    public String defaultValue;
    public int pkIndex;
    public int isAutoIncrement;
    public boolean isSystemColumn;
    public boolean isSystemGeneratedTSColumn;
    //public Pair<Table, Column> references;

    public Column(long cid, String column, DataType type, int isNotNull, String defaultValue, int pkIndex, int isAutoIncrement) {
        this.cid = cid;
        this.column = column;
        this.type = type;
        this.isNotNull = isNotNull;
        this.defaultValue = defaultValue;
        this.pkIndex = pkIndex;
        this.isAutoIncrement = isAutoIncrement;
        this.isSystemColumn = checkSystemColumn(column);
        this.isSystemGeneratedTSColumn = checkSystemGeneratedTSColumn(column);
    }

    private static boolean checkSystemColumn(String c) {
        return (c.equals("synclite_device_id") || c.equals("synclite_device_name") || (c.equals("synclite_update_timestamp")));
	}

    private static boolean checkSystemGeneratedTSColumn(String c) {
        return c.equals("synclite_update_timestamp");
	}

	@Override
    public String toString() {
        return "cid : " + cid +
               ", column : " + column +
               ", type : " + type +
               ", isNotNull : " + isNotNull +
               ", default value : " + defaultValue +
               ", isPrimaryKey : " + pkIndex +
               ", isAutoIncrement : " + isAutoIncrement;
    }

    public boolean isBLOB() {
        return type.dbNativeDataType.equalsIgnoreCase("blob");
    }

    public boolean isCLOB() {
    	return type.dbNativeDataType.equalsIgnoreCase("clob");
    }
    
    public boolean isSystemColumn() {
    	return this.isSystemColumn;
    }
   
    
}
