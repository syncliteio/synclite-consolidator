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
