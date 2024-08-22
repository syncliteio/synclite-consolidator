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
