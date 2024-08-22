package com.synclite.consolidator.oper;

public enum OperType {
    NOOP,
    INSERT,
    UPSERT,
    REPLACE,
    DELETEINSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
    LOAD,
    ADDCOLUMN,
    DROPCOLUMN,
    RENAMECOLUMN,
    ALTERCOLUMN,
    COPYCOLUMN,
    CREATETABLE,
    DROPTABLE,
    RENAMETABLE,
    COPYTABLE,
    REFRESHTABLE,
    CREATEDATABASE,
    CREATESCHEMA,
    BEGINTRAN,
    COMMITTRAN,
    CHECKPOINTTRAN,
    DELETE_IF_PREDICATE,
    MINUS, 
    NATIVE,
    PUBLISHCOLUMNLIST,
    FINISHBATCH,
    SHUTDOWN
}
