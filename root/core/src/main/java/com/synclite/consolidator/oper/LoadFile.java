package com.synclite.consolidator.oper;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.Table;
import com.synclite.consolidator.schema.TableMapper;

public class LoadFile extends DML {

	public Path dataFilePath;
	public String format;
	public char delimiter;
	public String nullString;
	public boolean header;
	public char quote;
	public char escape;
	public Map<String, String> defaultValues;
	public long loadedRecordCount;
	
	public LoadFile(Table tbl, Map<String, String> defaultValues, Path dataFileName, String format, char delimiter, String nullString, boolean header, char quote, char escape) {
		super(tbl, null, null);
		this.dataFilePath = dataFileName;
		this.format = format;
		this.delimiter = delimiter;
		this.nullString = nullString;
		this.header = header;
		this.quote = quote;
		this.escape = escape;
		this.defaultValues = defaultValues;
        this.operType = OperType.LOAD;
        this.loadedRecordCount = 0;
	}

	@Override
	public void execute(SQLExecutor executor) throws DstExecutionException {
		executor.loadFile(this);
	}

    @Override
    public boolean isBatchable() {
        return false;
    }

	@Override
	public String getSQL(SQLGenerator generator) {
		return generator.getLoadFileSQL(this);
	}

	@Override
	public List<Oper> map(TableMapper tableMapper) {
		return tableMapper.mapOper(this);
	}

}
