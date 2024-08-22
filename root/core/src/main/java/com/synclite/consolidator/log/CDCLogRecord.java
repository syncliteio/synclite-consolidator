package com.synclite.consolidator.log;

import java.util.List;

import com.synclite.consolidator.oper.OperType;

public class CDCLogRecord {

    public long commitId;
    public String database;
    public String schema;
    public String table;
    public String oldTable;
    public OperType opType;
    public String sql;
    public long changeNumber;
    public List<CDCColumnValues> values;
    public CDCLogSchema colSchemas;

    public CDCLogRecord(
            long commitId,
            String database,
            String schema,
            String table,
            String oldTable,
            OperType opType,
            String sql,
            long changeNumber,
            List<CDCColumnValues> values,
            CDCLogSchema colSchemas
    ) {

        this.commitId = commitId;
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.oldTable = oldTable;
        this.opType = opType;
        this.sql = sql;
        this.changeNumber = changeNumber;
        this.values = values;
        this.colSchemas= colSchemas;
    }

    @Override
    public String toString() {
        return "CDC Log Record : commit : " + commitId + ", Table : " + database + "." + schema + "." + table + ", OP :" + opType + ", SQL : " + sql + ", change number : " + changeNumber;
    }
};


