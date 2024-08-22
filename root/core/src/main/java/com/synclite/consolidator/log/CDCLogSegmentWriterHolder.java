/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

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
