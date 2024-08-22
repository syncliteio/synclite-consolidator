package com.synclite.consolidator.oper;

import java.util.List;

import com.synclite.consolidator.schema.Table;

public abstract class DML extends Oper {

    public List<Object> beforeValues;
    public List<Object> afterValues;

    public DML(
        Table tbl,
        List<Object> beforeValues,
        List<Object> afterValues
    ) {
        super(tbl);
        this.beforeValues = beforeValues;
        this.afterValues = afterValues;
    }
    
	public void replaceArgs(List<Object> beforeValues, List<Object> afterValues) {
		this.beforeValues= beforeValues;
		this.afterValues= afterValues;
	}

}
