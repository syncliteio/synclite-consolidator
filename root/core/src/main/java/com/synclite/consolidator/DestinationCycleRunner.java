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

package com.synclite.consolidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Runs one device cycle without allowing a failed destination to prevent
 * later destinations from making progress.
 */
final class DestinationCycleRunner {

	@FunctionalInterface
	interface DestinationOperation {
		boolean process(int dstIndex) throws Exception;
	}

	@FunctionalInterface
	interface CleanupOperation {
		void cleanUp() throws Exception;
	}

	static final class DestinationFailure {
		private final int dstIndex;
		private final Exception cause;

		private DestinationFailure(int dstIndex, Exception cause) {
			this.dstIndex = dstIndex;
			this.cause = cause;
		}

		int getDstIndex() {
			return dstIndex;
		}

		Exception getCause() {
			return cause;
		}
	}

	static final class Result {
		private final boolean hasMoreWork;
		private final List<DestinationFailure> destinationFailures;
		private final Exception cleanupFailure;

		private Result(boolean hasMoreWork, List<DestinationFailure> destinationFailures,
				Exception cleanupFailure) {
			this.hasMoreWork = hasMoreWork;
			this.destinationFailures = Collections.unmodifiableList(destinationFailures);
			this.cleanupFailure = cleanupFailure;
		}

		boolean hasMoreWork() {
			return hasMoreWork;
		}

		List<DestinationFailure> getDestinationFailures() {
			return destinationFailures;
		}

		Exception getCleanupFailure() {
			return cleanupFailure;
		}

		boolean requiresRetry() {
			return !destinationFailures.isEmpty() || cleanupFailure != null;
		}
	}

	private DestinationCycleRunner() {
	}

	static Result run(Iterable<Integer> dstIndexes, DestinationOperation destinationOperation,
			CleanupOperation cleanupOperation) {
		Objects.requireNonNull(dstIndexes, "dstIndexes");
		Objects.requireNonNull(destinationOperation, "destinationOperation");
		Objects.requireNonNull(cleanupOperation, "cleanupOperation");

		boolean hasMoreWork = false;
		List<DestinationFailure> destinationFailures = new ArrayList<DestinationFailure>();
		for (int dstIndex : dstIndexes) {
			try {
				hasMoreWork = destinationOperation.process(dstIndex) || hasMoreWork;
			} catch (Exception e) {
				destinationFailures.add(new DestinationFailure(dstIndex, e));
				if (e instanceof InterruptedException) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		}

		Exception cleanupFailure = null;
		try {
			cleanupOperation.cleanUp();
		} catch (Exception e) {
			cleanupFailure = e;
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
		}

		return new Result(hasMoreWork, destinationFailures, cleanupFailure);
	}
}