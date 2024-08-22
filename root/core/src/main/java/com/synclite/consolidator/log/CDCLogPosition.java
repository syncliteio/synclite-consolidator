package com.synclite.consolidator.log;

public class CDCLogPosition {
    public long commitId;
    public long changeNumber;
    public long txnChangeNumber;
    public long logSegmentSequenceNumber;
    public long txnCount;

    public CDCLogPosition(long commitId, long changeNumber, long txnChangeNumber, long cdcLogSegmentSequenceNumber, long txnCount) {
        this.commitId = commitId;
        this.changeNumber = changeNumber;
        this.txnChangeNumber = txnChangeNumber;
        this.logSegmentSequenceNumber = cdcLogSegmentSequenceNumber;
        this.txnCount = txnCount;
    }
    
    public int compare(CDCLogPosition l) {
    	if (this.logSegmentSequenceNumber < l.logSegmentSequenceNumber) {
    		return -1;
    	} else if (this.logSegmentSequenceNumber > l.logSegmentSequenceNumber) {
    		return 1;
    	} else {
    		if (this.commitId < l.commitId) {
	    		return -1;
	    	} else if (this.commitId > l.commitId) {
	    		return 1;
	    	} else {
	    		if (this.changeNumber < l.changeNumber) {
	    			return -1;
	    		} else if (this.changeNumber > l.changeNumber) {
	    			return 1;
	    		} else {
		    		if (this.txnChangeNumber < l.txnChangeNumber) {
		    			return -1;
		    		} else if (this.txnChangeNumber > l.txnChangeNumber) {
		    			return 1;
		    		} else {
		    			return 0;
		    		}
	    		}
	    	}
    	}
    }
    
}
