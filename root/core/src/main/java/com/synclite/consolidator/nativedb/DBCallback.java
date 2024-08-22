package com.synclite.consolidator.nativedb;

public interface DBCallback {

    public int deliverChanges (
            String database,
            String table,
            String operation,
            long [] beforeImage,
            long [] afterImage
    );

    public void setException(Exception e);
    public Exception getException();
}
