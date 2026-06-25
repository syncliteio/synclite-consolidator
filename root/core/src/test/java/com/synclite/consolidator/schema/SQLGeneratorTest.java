package com.synclite.consolidator.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.oper.RenameTable;

class SQLGeneratorTest {

    @BeforeEach
    void configureConfLoader() throws Exception {
        ConfLoader confLoader = ConfLoader.getInstance();
        setBooleanArray(confLoader, "dstQuoteObjectNames", new Boolean[] { false, false });
        setBooleanArray(confLoader, "dstQuoteColumnNames", new Boolean[] { false, false });
        setBooleanArray(confLoader, "dstUseCatalogScopeResolution", new Boolean[] { false, true });
        setBooleanArray(confLoader, "dstUseSchemaScopeResolution", new Boolean[] { false, true });
    }

    @Test
    void renameTableUsesSimpleNewNameForPostgres() {
        PGSQLGenerator generator = new PGSQLGenerator(1);
        Table oldTable = new Table();
        oldTable.id = TableID.from("device-uuid", "device-name", 1, "newdb", "newschema", "t1");

        Table newTable = new Table();
        newTable.id = TableID.from("device-uuid", "device-name", 1, "newdb", "newschema", "t2");

        RenameTable renameTable = new RenameTable(new Table(), oldTable, newTable);

        assertEquals("ALTER TABLE newdb.newschema.t1 RENAME TO t2", generator.getRenameTableSQL(renameTable));
    }

    private static void setBooleanArray(ConfLoader confLoader, String fieldName, Boolean[] values) throws Exception {
        Field field = ConfLoader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(confLoader, values);
    }
}
