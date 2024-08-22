package com.synclite.consolidator.exception;

public class SyncLiteStageException extends SyncLiteException {
    public SyncLiteStageException (String message) {
        super(message);
    }

    public SyncLiteStageException (Exception e) {
        super(e);
    }

    public SyncLiteStageException (String message, Exception e) {
        super(message, e);
    }
}

