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

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;

public class DeviceTelemetryProcessor extends DeviceProcessor {

	private DeviceEventStreamer streamer; 

    protected DeviceTelemetryProcessor(Device device, int dstIndex) throws SyncLiteException {
        super(device, dstIndex);
        this.streamer = new DeviceEventStreamer(device, dstIndex);
    }

	@Override
	public boolean hasMoreWork() throws SyncLiteException {
		return streamer.hasMoreWork();
	}

	@Override
	public long syncDevice() throws SyncLiteException {

	    long importedOperCnt = streamer.syncDevice();
	    return importedOperCnt;
	}

	@Override
	public long consolidateDevice() throws SyncLiteException {		
		return streamer.consolidateDevice();
	}

}
