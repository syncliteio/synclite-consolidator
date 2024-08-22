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

import java.util.List;
import java.util.Map;

import com.synclite.consolidator.schema.Column;

public class CDCLogSchema {

    public  List<Column> columns;
    public Map<String, String> newToOldColumnNames;

    public CDCLogSchema(List<Column> columns, Map<String, String> renamedColMap) {
        this.columns = columns;
        this.newToOldColumnNames = renamedColMap;
    }
}
