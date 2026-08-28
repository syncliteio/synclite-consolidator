package com.synclite.consolidator.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.JDBCType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstDataTypeMapping;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.RenameColumn;
import com.synclite.consolidator.oper.RenameTable;

class SQLGeneratorTest {

    @BeforeEach
    void configureConfLoader() throws Exception {
        ConfLoader confLoader = ConfLoader.getInstance();
        setBooleanArray(confLoader, "dstQuoteObjectNames", new Boolean[] { false, false });
        setBooleanArray(confLoader, "dstQuoteColumnNames", new Boolean[] { false, false });
        setBooleanArray(confLoader, "dstUseCatalogScopeResolution", new Boolean[] { false, true });
        setBooleanArray(confLoader, "dstUseSchemaScopeResolution", new Boolean[] { false, true });
        setStringArray(confLoader, "dstCreateTableSuffix", new String[] { "", "" });
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

    @Test
    void schemaQualifiedObjectNamesUseDestinationSpecificQuoting() throws Exception {
        ConfLoader confLoader = ConfLoader.getInstance();
        setBooleanArray(confLoader, "dstQuoteObjectNames", new Boolean[] { false, true });

        MySQLSQLGenerator mysqlGenerator = new MySQLSQLGenerator(1);
        PGSQLGenerator postgresGenerator = new PGSQLGenerator(1);

        assertEquals("`myschema`.`synclite_consolidator_metadata`",
                mysqlGenerator.getSchemaQualifiedObjectName("myschema", "synclite_consolidator_metadata"));
        assertEquals("\"myschema\".\"synclite_consolidator_metadata\"",
                postgresGenerator.getSchemaQualifiedObjectName("myschema", "synclite_consolidator_metadata"));
    }

    @Test
    void postgresCreateTableKeepsAutoincrementPrimaryKeyConstraint() {
        PGSQLGenerator generator = new PGSQLGenerator(1);
        Table table = new Table();
        table.id = TableID.from("device-uuid", "device-name", 1, "newdb", "newschema", "items");
        table.addColumn(new Column(1, "id", new DataType("INTEGER", JDBCType.INTEGER, StorageClass.INTEGER), 0, null, 1, 1));
        table.addColumn(new Column(2, "name", new DataType("VARCHAR", JDBCType.VARCHAR, StorageClass.TEXT), 1, null, 0, 0));

        CreateTable createTable = new CreateTable(table);
        String sql = generator.getCreateTableSQL(createTable);

        assertTrue(sql.contains("PRIMARY KEY(id)"));
        assertTrue(sql.contains("id INTEGER"));
    }

    @Test
    void mssqlPreservesExplicitStringLengthsForSystemTableKeyColumns() {
        MSSQLDataTypeMapper mapper = new MSSQLDataTypeMapper(1);
        DataType propKeyType = new DataType("varchar(255)", JDBCType.VARCHAR, StorageClass.TEXT);

        DataType mappedType = mapper.mapTypeForSystemColumn(propKeyType);

        assertEquals("varchar(255)", mappedType.dbNativeDataType);
    }

    @Test
    void mssqlPreservesExplicitStringLengthsForRegularColumns() {
        MSSQLDataTypeMapper mapper = new MSSQLDataTypeMapper(1);
        DataType sourceType = new DataType("varchar(100)", JDBCType.VARCHAR, StorageClass.TEXT);

        DataType mappedType = mapper.doMapTypeBestEffort(sourceType);

        assertEquals("varchar(100)", mappedType.dbNativeDataType);
    }

    @Test
    void mssqlPreservesExplicitStringLengthsWithDefaultConservativeStrategy() throws Exception {
        // Set up the destination data type mapping to ALL_TEXT (conservative/default)
        ConfLoader confLoader = ConfLoader.getInstance();
        setDataTypeMappingArray(confLoader, "dstDataTypeMapping", new DstDataTypeMapping[] { null, DstDataTypeMapping.ALL_TEXT });
        
        MSSQLDataTypeMapper mapper = new MSSQLDataTypeMapper(1);
        DataType sourceType = new DataType("varchar(100)", JDBCType.VARCHAR, StorageClass.TEXT);

        // Using mapType() instead of doMapTypeBestEffort() to go through the strategy selection logic
        DataType mappedType = mapper.mapType(sourceType);

        // With default ALL_TEXT strategy, should use doMapTypeConservative which should preserve the length
        assertEquals("varchar(100)", mappedType.dbNativeDataType, 
            "Conservative mapping should preserve explicit varchar length");
    }

    @Test
    void mssqlExactMappingCanonicalizesStringSynonymsWithLength() throws Exception {
        ConfLoader confLoader = ConfLoader.getInstance();
        setDataTypeMappingArray(confLoader, "dstDataTypeMapping", new DstDataTypeMapping[] { null, DstDataTypeMapping.EXACT });

        MSSQLDataTypeMapper mapper = new MSSQLDataTypeMapper(1);
        DataType sourceType = new DataType("character varying(100)", JDBCType.VARCHAR, StorageClass.TEXT);

        DataType mappedType = mapper.mapType(sourceType);

        assertEquals("varchar(100)", mappedType.dbNativeDataType,
                "EXACT mapping should canonicalize SQL synonyms to MSSQL-safe bounded types");
    }

    @Test
    void mssqlExactMappingAddsDefaultLengthForBareVarchar() throws Exception {
        ConfLoader confLoader = ConfLoader.getInstance();
        setDataTypeMappingArray(confLoader, "dstDataTypeMapping", new DstDataTypeMapping[] { null, DstDataTypeMapping.EXACT });

        MSSQLDataTypeMapper mapper = new MSSQLDataTypeMapper(1);
        DataType sourceType = new DataType("varchar", JDBCType.VARCHAR, StorageClass.TEXT);

        DataType mappedType = mapper.mapType(sourceType);

        assertEquals("varchar(255)", mappedType.dbNativeDataType,
                "EXACT mapping should avoid SQL Server's default VARCHAR(1)");
    }

    @Test
    void mssqlUsesSpRenameForColumnRenameOperations() {
        MSSQLSQLGenerator generator = new MSSQLSQLGenerator(1);
        Table table = new Table();
        table.id = TableID.from("device-uuid", "device-name", 1, "synclitedb", "newschema", "SalesTransaction");
        Column amountColumn = new Column(1, "amount", new DataType("varchar(100)", JDBCType.VARCHAR, StorageClass.TEXT), 0, null, 0, 0);
        RenameColumn renameColumn = new RenameColumn(table, amountColumn, "amount", "price");

        assertEquals("EXEC sp_rename 'synclitedb.newschema.SalesTransaction.amount', 'price', 'COLUMN';",
                generator.getRenameColumnSQL(renameColumn));
    }

    private static void setBooleanArray(ConfLoader confLoader, String fieldName, Boolean[] values) throws Exception {
        Field field = ConfLoader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(confLoader, values);
    }

    private static void setDataTypeMappingArray(ConfLoader confLoader, String fieldName, DstDataTypeMapping[] values) throws Exception {
        Field field = ConfLoader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(confLoader, values);
    }

    private static void setStringArray(ConfLoader confLoader, String fieldName, String[] values) throws Exception {
        Field field = ConfLoader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(confLoader, values);
    }
}
