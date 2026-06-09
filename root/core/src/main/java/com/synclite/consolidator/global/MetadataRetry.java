/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * Mirrors the retry envelope used by Rust's `retry_dst(...)` so that every
 * consolidator metadata write is uniformly guarded against transient dst
 * failures.
 */
package com.synclite.consolidator.global;

import org.apache.log4j.Logger;

public final class MetadataRetry {
    private MetadataRetry() {}

    @FunctionalInterface
    public interface MetadataOp<E extends Exception> {
        void run() throws E;
    }

    public static <E extends Exception> void retry(
            int dstIndex, Logger tracer, String opName, MetadataOp<E> op) throws E {
        long retryCount = Math.max(1L, ConfLoader.getInstance().getDstOperRetryCount(dstIndex));
        long retryIntervalMs = ConfLoader.getInstance().getDstOperRetryIntervalMs(dstIndex);
        for (long i = 0; i < retryCount; ++i) {
            try {
                op.run();
                return;
            } catch (Exception e) {
                if (i == retryCount - 1) {
                    @SuppressWarnings("unchecked")
                    E typed = (E) e;
                    throw typed;
                }
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (tracer != null) {
                    tracer.info("Retry attempt : " + (i + 2) + " : Retrying metadata op " + opName
                            + " after exception : " + e.getMessage());
                }
            }
        }
    }
}
