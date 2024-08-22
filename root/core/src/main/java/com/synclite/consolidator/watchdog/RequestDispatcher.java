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

import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.processor.DeviceCommandProcessor;
import com.synclite.consolidator.processor.RequestProcessor;

public class RequestDispatcher extends Thread{

	private Logger tracer;

	public RequestDispatcher(Logger tracer) {
		this.tracer = tracer;
	}

	public void run() {
		try (ZContext context = new ZContext()) {
			//  Socket to talk to clients
			ZMQ.Socket socket = context.createSocket(SocketType.REP);
			socket.setZAPDomain("tcp://localhost");
			int port = ConfLoader.getInstance().getRequestProcessorPort();
			socket.bind("tcp://localhost:" + port);

			while (!Thread.currentThread().isInterrupted()) {
				byte[] packet = socket.recv(0);
				String req = new String(packet, ZMQ.CHARSET);	

				// Parse the JSON message into a JSON object

				JSONObject json = new JSONObject(req);

				Iterator<String> keys = json.keys();
				HashMap<String, Object> args = new HashMap<String, Object>();
				RequestProcessor rp = null;
				String requestType = null;

				while (keys.hasNext()) {
					String key = keys.next();
					Object value = json.get(key);

					if (key.equals("type")) {
						requestType = value.toString();
					} else {
						args.put(key, value);
					}
				}
				if (requestType == null) {
					String response = "ERROR: No request type received";
					tracer.error("RequestDispatcher : No request type received");
					socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
				} else {
					try {
						rp = getRequestProcessor(requestType, args);
						if (rp == null) {
							String response = "ERROR: Invalid request type received : " + requestType;
							tracer.error("RequestDispatcher : Invalid request type received : " + requestType);
							socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
						} else {
							try {
								tracer.info("RequestDispatcher : processing request type : " + requestType);
								rp.processRequest(args);
								String response = "SUCCESS: Request submitted successfully.";
								tracer.info("RequestDispatcher : submitted request : " + requestType + " successfully");
								socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
							} catch (SyncLiteException e) {
								String response = "ERROR: Request failed with exception : " + e.getMessage();
								tracer.error("Request " + requestType + " failed with exception : ", e); 
								socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
							}
						}
					} catch (Exception e) {
						String response = "ERROR: Failed to instantiate request type : " + requestType + " with exception : " + e.getMessage();
						tracer.error("RequestDispatcher : Failed to request request type : " + requestType, e);
						socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
					}
				}
			}
		}
	}

	private RequestProcessor getRequestProcessor(String key, HashMap<String, Object> args) throws SyncLiteException {
		String requestType = key.trim().toUpperCase();
		switch(requestType) {
		case "COMMAND_DEVICES":
			return new DeviceCommandProcessor(this.tracer, args);
		default:
			return null;
		}
	} 


}
