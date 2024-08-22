package com.synclite.consolidator.schema;

import java.util.concurrent.ConcurrentHashMap;

public class ConsolidatorDstTable extends Table {
    private static final ConcurrentHashMap<TableID, ConsolidatorDstTable> consolidatorDstTables = new ConcurrentHashMap<TableID, ConsolidatorDstTable>();

    private ConsolidatorDstTable(TableID id) {
        this.id = id;
    }

    public static ConsolidatorDstTable from(TableID id) {
        return consolidatorDstTables.computeIfAbsent(id, s -> new ConsolidatorDstTable(s));
    }

    public static void remove(TableID id) {
        consolidatorDstTables.remove(id);
    }

    public static int getCount() {
        return consolidatorDstTables.size();
    }

    public static void resetAll() {
        consolidatorDstTables.clear();
    }

}
