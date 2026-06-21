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

package com.synclite.consolidator.exception;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;

public final class DstErrorClassifier {

    private DstErrorClassifier() {}

    /**
     * Returns true when {@code t} (or any cause in its chain) looks like a
     * destination-wide / transient infrastructure failure: connection lost,
     * cannot reach destination, auth/admin shutdown, deadlock, serialization
     * failure, resource exhaustion, or socket/IO timeout.
     *
     * Used by the segment-apply loops to decide whether
     * {@code dst-skip-failed-log-files=true} should be honored after
     * retries are exhausted. The skip flag is meant for poison-pill segments
     * (bad data, missing table/column, syntax) — not for outages, where
     * skipping silently advances past every subsequent segment and loses
     * data. When this returns true, the caller must NOT skip; the next
     * processor cycle will retry the same segment.
     */
    public static boolean isTransientDstError(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof SQLTransientException
                    || cur instanceof SQLRecoverableException
                    || cur instanceof SQLNonTransientConnectionException
                    || cur instanceof java.net.SocketException
                    || cur instanceof java.net.SocketTimeoutException
                    || cur instanceof java.net.UnknownHostException
                    || cur instanceof java.net.ConnectException
                    || cur instanceof java.io.InterruptedIOException) {
                return true;
            }
            if (cur instanceof SQLException) {
                String state = ((SQLException) cur).getSQLState();
                if (state != null && state.length() >= 2) {
                    String cls = state.substring(0, 2);
                    // 08 = Connection Exception, 40 = Transaction Rollback (incl. deadlock),
                    // 53 = Insufficient Resources, 57 = Operator Intervention (incl. admin/crash shutdown, cannot_connect_now)
                    if (cls.equals("08") || cls.equals("40") || cls.equals("53") || cls.equals("57")) {
                        return true;
                    }
                }
            }
            String msg = cur.getMessage();
            if (msg != null) {
                String lm = msg.toLowerCase();
                if (lm.contains("connection refused")
                        || lm.contains("connection reset")
                        || lm.contains("connection closed")
                        || lm.contains("connection timed out")
                        || lm.contains("broken pipe")
                        || lm.contains("network is unreachable")
                        || lm.contains("no route to host")
                        || lm.contains("could not connect")
                        || lm.contains("server closed the connection")
                        || lm.contains("an i/o error occurred while sending")
                        || lm.contains("deadlock")) {
                    return true;
                }
            }
            Throwable next = cur.getCause();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        return false;
    }
}
