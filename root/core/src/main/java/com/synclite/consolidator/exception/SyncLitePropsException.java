package com.synclite.consolidator.exception;

public class SyncLitePropsException extends SyncLiteException {

    public SyncLitePropsException(String message) {
        super(message);
    }

    public SyncLitePropsException(Exception e) {
        super(e);
    }

    public SyncLitePropsException(String message, Exception e) {
        super(message, e);
    }

}
