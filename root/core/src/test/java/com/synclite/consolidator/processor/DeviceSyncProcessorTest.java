package com.synclite.consolidator.processor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DeviceSyncProcessorTest {

    @Test
    void recognizesSqlServerInvalidColumnNameAsMissingColumn() {
        assertTrue(DeviceSyncProcessor.isMissingColumnError(
                "com.microsoft.sqlserver.jdbc.SQLServerException: Invalid column name 'command_log_change_number'."));
    }

    @Test
    void ignoresOtherDatabaseErrors() {
        assertFalse(DeviceSyncProcessor.isMissingColumnError("Permission denied for table synclite_checkpoint"));
    }
}
