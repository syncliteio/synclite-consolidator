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
