package com.synclite.consolidator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class DestinationCycleRunnerTest {

	@Test
	void failedDestinationDoesNotBlockHealthyDestinationOrCleanup() {
		List<Integer> attempted = new ArrayList<Integer>();
		AtomicInteger cleanupCount = new AtomicInteger();
		Exception failure = new Exception("destination 1 offline");

		DestinationCycleRunner.Result result = DestinationCycleRunner.run(
				Arrays.asList(1, 2),
				dstIndex -> {
					attempted.add(dstIndex);
					if (dstIndex == 1) {
						throw failure;
					}
					return true;
				},
				cleanupCount::incrementAndGet);

		assertEquals(Arrays.asList(1, 2), attempted);
		assertEquals(1, cleanupCount.get());
		assertTrue(result.hasMoreWork());
		assertTrue(result.requiresRetry());
		assertEquals(1, result.getDestinationFailures().size());
		assertEquals(1, result.getDestinationFailures().get(0).getDstIndex());
		assertSame(failure, result.getDestinationFailures().get(0).getCause());
		assertNull(result.getCleanupFailure());
	}

	@Test
	void cleanupFailureRequestsRetryWithoutChangingDestinationProgress() {
		Exception cleanupFailure = new Exception("slowest checkpoint unavailable");

		DestinationCycleRunner.Result result = DestinationCycleRunner.run(
				Arrays.asList(1, 2),
				dstIndex -> false,
				() -> {
					throw cleanupFailure;
				});

		assertFalse(result.hasMoreWork());
		assertTrue(result.getDestinationFailures().isEmpty());
		assertTrue(result.requiresRetry());
		assertSame(cleanupFailure, result.getCleanupFailure());
	}

	@Test
	void successfulIdleCycleDoesNotRequestRetry() {
		DestinationCycleRunner.Result result = DestinationCycleRunner.run(
				Arrays.asList(1, 2), dstIndex -> false, () -> { });

		assertFalse(result.hasMoreWork());
		assertFalse(result.requiresRetry());
		assertTrue(result.getDestinationFailures().isEmpty());
		assertNull(result.getCleanupFailure());
	}
}