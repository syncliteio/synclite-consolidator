package com.synclite.consolidator.processor;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;

public class DeviceTransactionalProcessor extends DeviceProcessor {

    private DeviceReplicator replicator;
    private DeviceConsolidator consolidator;

    protected DeviceTransactionalProcessor(Device device, int dstIndex) throws SyncLiteException {
        super(device, dstIndex);
        if (dstIndex == 1) {
        	this.replicator = new DeviceReplicator(device, dstIndex);
        } else {
        	this.replicator = null;
        }
        this.consolidator = new DeviceConsolidator(device, dstIndex);
    }

    @Override
    public long syncDevice() throws SyncLiteException {
    	long replicatedOperCnt = 0;
    	if (this.replicator != null) {
    		replicatedOperCnt = replicator.syncDevice();
    	}
    	long importedOperCnt = 0;
    	if (this.consolidator != null) {
    		importedOperCnt = consolidator.syncDevice();
    	}
        return replicatedOperCnt + importedOperCnt; 
    }

    @Override
    public boolean hasMoreWork() throws SyncLiteException {
    	return (((replicator != null) && replicator.hasMoreWork()) || ((consolidator != null) && consolidator.hasMoreWork())); 
    }
    
    @Override
    public long consolidateDevice() throws SyncLiteException {
    
    	long replicatedOperCnt = 0; 
    	if (replicator != null) {
    		replicator.consolidateDevice();
    	}
        long importedOperCnt = 0; 
        if (consolidator != null) {		
        	consolidator.consolidateDevice();
        }
        return replicatedOperCnt + importedOperCnt;
    }
}
