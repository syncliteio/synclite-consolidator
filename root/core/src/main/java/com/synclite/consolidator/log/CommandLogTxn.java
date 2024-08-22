package com.synclite.consolidator.log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CommandLogTxn implements Iterable<CommandLogRecord>{
    private final List<CommandLogRecord>logs;

    public CommandLogTxn() {
        this.logs = new ArrayList<CommandLogRecord>();
    }

    public void addLog(CommandLogRecord log) {
        this.logs.add(log);
    }

    @Override
    public Iterator<CommandLogRecord> iterator() {
        return logs.iterator();
    }
}
