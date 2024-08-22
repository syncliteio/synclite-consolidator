package com.synclite.consolidator.log;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.log.CDCLogSegment.CDCLogSegmentWriter;


public class CDCLogSegmentWriterHolder implements AutoCloseable {

    private CDCLogSegmentWriter writer;

    public CDCLogSegmentWriterHolder(CDCLogSegmentWriter writer) {
        this.writer = writer;
    }

    public final void beginTran() throws SyncLiteException {
        writer.beginTran();
    }

    public final void commitTran() throws SyncLiteException {
        writer.commitTran();
    }

    public void writeCDCLog(CDCLogRecord record) throws SyncLiteException {
        writer.writeCDCLog(record);
    }
    public void replaceWriter(CDCLogSegmentWriter newWriter) throws SyncLiteException {
        if(writer != null) {
            this.writer.close();
        }
        this.writer = newWriter;
    }

    @Override
    public void close() throws SyncLiteException {
        if (writer != null) {
            writer.close();
        }
    }
}
