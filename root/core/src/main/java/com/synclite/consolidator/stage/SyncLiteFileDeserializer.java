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

package com.synclite.consolidator.stage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class SyncLiteFileDeserializer implements Deserializer<Map<?,?>>{

	@Override
	public Map<?, ?> deserialize(String topic, byte[] data) {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bais);
			Object o = in.readObject();
			if (o instanceof Map) {
				return (Map<?,?>) o;
			} else
				return new HashMap<String, String>();
		} catch (ClassNotFoundException e) {
			//Ignore
		} catch (IOException e) {
			//Ignore here
		} finally {
			try {
				bais.close();
			} catch (IOException ex) {
				//Ignore here
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return new HashMap<String, String>();
	}
}
