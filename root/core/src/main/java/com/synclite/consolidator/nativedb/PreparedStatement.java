package com.synclite.consolidator.nativedb;

import java.sql.SQLException;

public class PreparedStatement implements AutoCloseable{
    private long stmtHandle;
    private long dbHandle;
    private DBCallback dbCallback;
    private boolean hasDBCallback;

    public PreparedStatement(long dbHandle, long stmtHandle, DBCallback dbCallback) throws SQLException {
        this.dbHandle = dbHandle;
        this.stmtHandle = stmtHandle;
        this.dbCallback = dbCallback;
        if (dbCallback == null) {
        	this.hasDBCallback = false;
        } else {
        	this.hasDBCallback = true;
        }
    }

    public PreparedStatement(long dbHandle, long stmtHandle) throws SQLException {
        this(dbHandle, stmtHandle, null);
    }

    private native int bindInt(long db, long pstmt, int index, int value) throws SQLException;
    public void bindInt(int index, int value) throws SQLException {
        bindInt(dbHandle, stmtHandle, index, value);
    }

    private native int bindLong(long db, long pstmt, int index, long value) throws SQLException;
    public void bindLong(int index, long value) throws SQLException {
        bindLong(dbHandle, stmtHandle, index, value);
    }

    private native int bindDouble(long db, long pstmt, int index, double value) throws SQLException ;
    public void bindDouble(int index, double value) throws SQLException {
        bindDouble(dbHandle, stmtHandle, index, value);
    }

    private native int bindText(long dbHandle, long pstmt, int index, String value, char encoding) throws SQLException;
    public void bindText(int index, String value, char encoding) throws SQLException {
        if (value == null) {
            bindNull(dbHandle, stmtHandle, index);
        } else {
            bindText(dbHandle, stmtHandle, index, value, encoding);
        }
    }

    private native int bindBlob(long db, long pstmt, int index, byte[] value) throws SQLException;
    public void bindBlob(int index, byte[] value) throws SQLException {
        if (value == null) {
            bindNull(dbHandle, stmtHandle, index);
        } else {
            bindBlob(dbHandle, stmtHandle, index, value);
        }
    }

    private native int bindNull(long db, long pstmt, int index) throws SQLException;
    public void bindNull(int index) throws SQLException {
        bindNull(dbHandle, stmtHandle, index);
    }

    private native int bindNativeValue(long db, long pstmt, int index, long value) throws SQLException;
    public void bindNativeValue(int index, long value) throws SQLException {
        if (value == 0) {
            bindNull(dbHandle, stmtHandle, index);
        } else {
            bindNativeValue(dbHandle, stmtHandle, index, value);
        }
    }

    private native int step(long db, long pstmt, boolean hasDBCallback) throws SQLException;
    public void step() throws SQLException {
        step(dbHandle, stmtHandle, hasDBCallback);
    }

    private native int stepQuery(long db, long pstmt) throws SQLException;
    public int stepQuery() throws SQLException {
        return stepQuery(dbHandle, stmtHandle);
    }

    private native int getInt(long db, long pstmt, int index) throws SQLException ;
    public int getInt(int index) throws SQLException {
        return getInt(dbHandle, stmtHandle, index);
    }

    private native long getLong(long db, long pstmt, int index) throws SQLException ;
    public long getLong(int index) throws SQLException {
        return getLong(dbHandle, stmtHandle, index);
    }

    private native String getString(long db, long pstmt, int index) throws SQLException ;
    public String getString(int index) throws SQLException {
        return getString(dbHandle, stmtHandle, index);
    }

    private native long getNativeValue(long db, long pstmt, int index) throws SQLException ;
    public long getNativeValue(int index) throws SQLException {
        return getNativeValue(dbHandle, stmtHandle, index);
    }

    private native int finalizePrepared(long db, long pstmt) throws SQLException ;
    public void finalizePrepared() throws SQLException {
        finalizePrepared(dbHandle, stmtHandle);
    }

    private native int reset(long db, long pstmt) throws SQLException ;
    public void reset() throws SQLException {
        reset(dbHandle, stmtHandle);
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

    @Override
    public void close() throws SQLException {
        finalizePrepared();
    }
}
