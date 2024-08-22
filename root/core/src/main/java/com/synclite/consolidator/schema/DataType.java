package com.synclite.consolidator.schema;

import java.sql.JDBCType;

public class DataType {
    public String dbNativeDataType;
    public JDBCType javaSQLType;
    public StorageClass storageClass;

    public DataType (String dbNativeDataType, JDBCType javaSQLType, StorageClass storgeClass) {
        this.dbNativeDataType = dbNativeDataType;
        this.javaSQLType = javaSQLType;
        this.storageClass = storgeClass;
    }

    @Override
    public String toString() {
        return this.dbNativeDataType;
    }
}
