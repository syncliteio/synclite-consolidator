package com.synclite.consolidator.processor;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteException;

public abstract class RequestProcessor {
	
	protected Logger tracer;
	protected HashMap<String, Object> args;
	
	RequestProcessor(Logger tr, HashMap<String, Object> args) throws SyncLiteException {
		this.tracer = tr;
		this.args = args;
	}
	
	protected abstract void doProcessRequest(HashMap<String, Object> arguments) throws SyncLiteException;
	
	public void processRequest(HashMap<String, Object> arguments) throws SyncLiteException {		
		CompletableFuture.runAsync(() -> {
			try {
				doProcessRequest(arguments);
			} catch (SyncLiteException e) {
				tracer.error("RequestProcessor : failed with exception : ", e);
			}
		});
	}
	
}
