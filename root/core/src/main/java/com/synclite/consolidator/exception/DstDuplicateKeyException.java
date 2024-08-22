package com.synclite.consolidator.exception;

public class DstDuplicateKeyException extends DstExecutionException {

    public DstDuplicateKeyException (String message) {
        super(message);
    }

    public DstDuplicateKeyException (Exception e) {
        super(e);
    }

    public DstDuplicateKeyException (String message, Exception e) {
        super(message, e);
    }

}
