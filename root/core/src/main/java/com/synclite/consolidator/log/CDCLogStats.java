package com.synclite.consolidator.log;

import java.util.concurrent.ConcurrentHashMap;

import com.synclite.consolidator.schema.TableID;

public class CDCLogStats {
	public long logSequenceNumber;
	public ConcurrentHashMap<TableID, ConcurrentHashMap<String, Long>> tableStatsMap = new ConcurrentHashMap<TableID, ConcurrentHashMap<String, Long>>();	
}
