package com.synclite.consolidator.schema;

import java.util.concurrent.ConcurrentHashMap;

public class ReplicatorTable extends Table{
    private static final ConcurrentHashMap<TableID, ReplicatorTable> replicationTables = new ConcurrentHashMap<TableID, ReplicatorTable>();

    private ReplicatorTable(TableID id) {
        this.id = id;
    }

    public static ReplicatorTable from(TableID id) {
        return replicationTables.computeIfAbsent(id, s -> new ReplicatorTable(s));
    }

    public static void remove(TableID id) {
        replicationTables.remove(id);
    }
}
