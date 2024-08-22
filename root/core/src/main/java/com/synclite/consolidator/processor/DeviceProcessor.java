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

import java.util.concurrent.ConcurrentHashMap;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.DstType;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;

public abstract class DeviceProcessor {

    private static final ConcurrentHashMap<String, DeviceProcessor> processors = new ConcurrentHashMap<String, DeviceProcessor>();
    protected Device device;
    protected int dstIndex;

    protected DeviceProcessor(Device device, int dstIndex) {
        this.device = device;
        this.dstIndex = dstIndex;
    }

    public static DeviceProcessor getInstance(Device device, int dstIndex) {
        if (device == null) {
            return null;
        }
        return processors.computeIfAbsent(device.getDeviceName() + device.getDeviceUUID() + dstIndex, s -> {
            try {
            	if (SyncLiteLoggerInfo.isTransactionalDeviceType(device.getDeviceType())) {
            		if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.REPLICATION) {
            			if (ConfLoader.getInstance().getDstType(dstIndex) == DstType.SQLITE) {
            				return new DeviceReplicator(device, dstIndex);
            			} else {
            				return new DeviceTransactionalProcessor(device, dstIndex);
            			}
            		} else {
            			return new DeviceTransactionalProcessor(device, dstIndex);
            		}
            	} else {
            		return new DeviceTelemetryProcessor(device, dstIndex);
            	}            	
            } catch (SyncLiteException e) {
                throw new RuntimeException(e);
            }
        });
    }

	public long processDevice() throws SyncLiteException {
    	return syncDevice();
    }
    
    /*
    public long processDevice() throws SyncLiteException {
        DeviceCommand deviceCommand = this.device.peekDeviceCommand();
        if (deviceCommand != null) {
            device.tracer.info("Processing device command : " + deviceCommand);
            //process the command.
            boolean success = false;
            switch(deviceCommand.type) {
            case REINITIALIZE_DEVICES:
                success = reinitializeDevice();
                break;
            case RESYNC_DEVICES:
                success = resyncDevice();
                break;
            default:
                throw new SyncLiteException("Unsupported device command : " + deviceCommand);
            }
            this.device.pollDeviceCommand();
            if (success) {
                device.tracer.info("Processed device command " + deviceCommand + " successfully");
            } else {
                device.tracer.info("Processed device command " + deviceCommand + " with failure");
            }
            return 0;
        } else if (Main.COMMAND == UserCommand.SYNC) {
            return syncDevice();
        } else if (Main.COMMAND == UserCommand.IMPORT) {
            return consolidateDevice();
        }
        return 0;
    }*/

    public abstract boolean hasMoreWork() throws SyncLiteException;
    
    public abstract long syncDevice() throws SyncLiteException;

    public abstract long consolidateDevice() throws SyncLiteException;

	public void removeInstance(Device device, long dstIndex) {
		processors.remove(device.getDeviceName() + device.getDeviceUUID() + dstIndex);
	}
}
