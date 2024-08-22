package com.synclite.consolidator.log;

public class CDCColumnValues {
    public long columnIndex;
    public long beforeValue;
    public long afterValue;

    public CDCColumnValues(long columnIndex, long beforeValue, long afterValue) {
        this.columnIndex= columnIndex;
        this.beforeValue= beforeValue;
        this.afterValue = afterValue;
    }
}
