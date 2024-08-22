package com.synclite.consolidator.oper;

import java.nio.file.Path;
import java.util.List;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public abstract class Oper {
    public Table tbl;
    public OperType operType;
    public Oper(Table tbl) {
        this.tbl = tbl;
    }
       
    public abstract void execute(SQLExecutor executor) throws DstExecutionException;

    public boolean isBatchable() {
        return false;
    }

    public abstract String getSQL(SQLGenerator generator);

    public String getFileLoaderSQL(SQLGenerator generator, Path filePath) {
    	throw new UnsupportedOperationException("File loader based SQL not supported for this operation : " + operType);
    }
    
    public abstract List<Oper> map(TableMapper tableMapper) throws SyncLiteException;
   

}
