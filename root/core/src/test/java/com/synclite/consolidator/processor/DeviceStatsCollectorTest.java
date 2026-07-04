package com.synclite.consolidator.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.schema.TableID;

class DeviceStatsCollectorTest {

    @Test
    void renameTableStatsUseOriginalTableIdentity() {
        TableID currentTableId = TableID.from("device-uuid", "device-name", 1, "db", "schema", "new_table");

        TableID statsTableId = DeviceStatsCollector.resolveStatsTableId(currentTableId, OperType.RENAMETABLE, "old_table");

        assertEquals("old_table", statsTableId.table);
        assertEquals("schema", statsTableId.schema);
        assertEquals("db", statsTableId.database);
    }
}
