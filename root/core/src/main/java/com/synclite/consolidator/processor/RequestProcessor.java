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
