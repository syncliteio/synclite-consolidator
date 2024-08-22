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

package com.synclite.consolidator.watchdog;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstType;

public class WatchDogs {
	
	private static final class InstanceHolder {
		private static WatchDogs INSTANCE = new WatchDogs();
	}

	public static WatchDogs getInstance() {
		return InstanceHolder.INSTANCE;
	}
	
	public void startWatchDogs(Logger tracer) throws SyncLiteException {
		Monitor.createAndGetInstance(tracer).start();
		
		for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
			if (ConfLoader.getInstance().getDstType(dstIndex) == DstType.DUCKDB) {
				(new DuckDBReader(tracer, dstIndex)).start();
			}
		}
		
		if (ConfLoader.getInstance().getEnableRequestProcessor()) {
			new RequestDispatcher(tracer).start();
		}
	}
}
