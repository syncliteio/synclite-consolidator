package com.synclite.consolidator.schema;

import java.util.concurrent.ConcurrentHashMap;

public class TableID {
    public String deviceUUID;
    public String deviceName;
    public int dstIndex;
    public String database;
    public String schema;
    public String table;

    private static final ConcurrentHashMap<String, TableID> tableIDs = new ConcurrentHashMap<String, TableID>();
    private TableID(String device, String deviceName, int dstIndex, String database, String schema, String table) {
        this.deviceUUID = device;
        this.deviceName = deviceName;
        this.dstIndex = dstIndex;
        this.database = database;
        this.schema = schema;
        this.table = table;
    }

    public static String getResolvedName(String deviceUUID, String deviceName, int dstIndex, String database, String schema, String table) {
    	//
    	//TODO support case sensitivity in table names ?
    	//
    	StringBuilder resolvedName = new StringBuilder();
        resolvedName.append(deviceUUID);
        resolvedName.append(".");
        resolvedName.append(deviceName);
        resolvedName.append(".");
        resolvedName.append(dstIndex);
        resolvedName.append(".");       
        resolvedName.append((database != null) ? database.toLowerCase() : database);
        resolvedName.append(".");
        resolvedName.append((schema != null) ? schema.toLowerCase() : schema);
        resolvedName.append(".");
        resolvedName.append((table != null) ? table.toLowerCase() : table);
        return resolvedName.toString();
    }

    public static TableID from(String deviceUUID, String deviceName, int dstIndex, String database, String schema, String table) {
        String resolvedName = getResolvedName(deviceUUID, deviceName, dstIndex, database, schema, table);
        return tableIDs.computeIfAbsent(resolvedName, s -> new TableID(deviceUUID, deviceName, dstIndex, database, schema, table));
    }

    @Override
    public String toString() {
        return database + "." + schema + "." + table;
    }
}
