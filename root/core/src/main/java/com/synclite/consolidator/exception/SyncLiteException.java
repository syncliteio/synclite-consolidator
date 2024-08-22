package com.synclite.consolidator.exception;

public class SyncLiteException extends Exception {

    public SyncLiteException(String message) {
        super(message);
    }

    public SyncLiteException(Exception e) {
        super(e);
    }

    public SyncLiteException(String message, Exception e) {
        super(message, e);
    }

    @Override
    public String toString() {
        StringBuilder stack = new StringBuilder();
        stack.append(getMessage());
        stack.append("\n");
        stack.append(getCause());
        for (StackTraceElement element : this.getStackTrace()) {
            stack.append("\n");
            stack.append(element);
        }
        return stack.toString();
    }
}
