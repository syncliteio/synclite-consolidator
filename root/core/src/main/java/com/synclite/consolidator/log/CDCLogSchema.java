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
