package com.synclite.consolidator.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.oper.OperType;

class CommandLogRecordTest {

    @Test
    void renameColumnParsesQuotedPostgresIdentifiers() throws SyncLiteException {
        CommandLogRecord record = new CommandLogRecord(
                1,
                1,
                1,
                "ALTER TABLE \"main\".\"t1\" RENAME COLUMN \"a\" TO \"b\"",
                0,
                null);

        assertNotNull(record.ddlInfo);
        assertEquals(OperType.RENAMECOLUMN, record.ddlInfo.ddlType);
        assertEquals("t1", record.ddlInfo.tableName);
        assertEquals("a", record.ddlInfo.oldColumnName);
        assertEquals("b", record.ddlInfo.columnName);
    }
}
