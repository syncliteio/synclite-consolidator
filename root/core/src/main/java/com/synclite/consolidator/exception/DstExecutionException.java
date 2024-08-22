package com.synclite.consolidator.exception;

public class DstExecutionException extends SyncLiteException {
    public DstExecutionException (String message) {
        super(message);
    }

    public DstExecutionException (Exception e) {
        super(e);
    }

    public DstExecutionException (String message, Exception e) {
        super(message, e);
    }

}
