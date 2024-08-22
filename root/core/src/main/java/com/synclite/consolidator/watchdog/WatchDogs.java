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
