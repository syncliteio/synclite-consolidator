package com.synclite.consolidator.nativedb;

import java.nio.file.Path;
import java.sql.SQLException;

public class DB implements AutoCloseable{

    private long dbHandle;
    private DBCallback dbCallback;
    private boolean hasDBCallback;

    public DB(Path dbPath, DBCallback dbCallback) throws SQLException {
        this.dbHandle = open(dbPath.toString());
        this.dbCallback = dbCallback;
        if (dbCallback == null) {
        	this.hasDBCallback = false;
        } else {
        	this.hasDBCallback = true;
        }
    }

    public DB(Path dbPath) throws SQLException {
        this(dbPath, null);
    }

    private native long open(String path);

    private native int exec(long db, String sql, boolean hasDBCallback) throws SQLException;
    public void exec(String sql) throws SQLException{
        int result = exec(this.dbHandle, sql, hasDBCallback);
    }

    public void beginTran() throws SQLException {
        int result = exec(this.dbHandle, "BEGIN TRANSACTION", hasDBCallback);
        //handle exceptions later
    }

    public void commitTran() throws SQLException {
        int result = exec(this.dbHandle, "COMMIT", hasDBCallback);
        //handle exceptions later
    }

    public void rollbackTran() throws SQLException {
        int result = exec(this.dbHandle, "ROLLBACK", hasDBCallback);
        //handle exceptions later
    }


    private native long prepare(long db, String sql) throws SQLException;
    public PreparedStatement prepare(String sql) throws SQLException{
        long stmtHandle = prepare(this.dbHandle, sql);
        //handle exceptions later
        return new PreparedStatement(dbHandle, stmtHandle, dbCallback);
    }

    private native int close(long db) throws SQLException;

    @Override
    public void close() throws SQLException {
        close(dbHandle);
    }


    private int getChanges(
            String database,
            String table,
            String operation,
            long [] beforeImage,
            long [] afterImage
    ) throws SQLException {
        if (dbCallback != null) {
            return dbCallback.deliverChanges(database, table, operation, beforeImage, afterImage);
        }
        return 0;
    }
}
