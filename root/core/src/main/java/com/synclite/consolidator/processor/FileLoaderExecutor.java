package com.synclite.consolidator.processor;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.DeleteInsert;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.oper.Oper;
import com.synclite.consolidator.oper.Update;
import com.synclite.consolidator.oper.Upsert;
import com.synclite.consolidator.schema.Column;

public abstract class FileLoaderExecutor extends JDBCExecutor {
	protected Path localCSVPath;
	protected String remoteCSVPath;
	protected FileWriter csvFileWriter;
	protected CSVPrinter csvPrinter;
	protected String fileLoaderInsertSql;
	protected String fileLoaderUpsertSql;
	protected String fileLoaderUpdateSql;
	protected String fileLoaderDeleteSql;
	protected boolean enableSingleRecBatchOptimization = true;
	
	public FileLoaderExecutor(Device device, int dstIndex, Logger tracer) throws DstExecutionException {
		super(device, dstIndex, tracer);	
		if (device != null) {
			this.localCSVPath = Path.of(device.getDeviceDataRoot().toString(), device.getDeviceUUID() + "-" + device.getDeviceName() + "-" + "data.csv");			
		}	
	}	
	
	@Override
    protected void bindInsertArgs(Insert oper) throws SQLException {
		//Add insert record to csv
		try {
			csvPrinter.printRecord(oper.afterValues);
		} catch (IOException e) {
			throw new SQLException("Failed to add INSERT values to CSV file : ", e);
		}
	}

	@Override
	protected void bindDeleteInsertArgs(DeleteInsert oper) throws DstExecutionException {
		try {
			bindInsertArgs(oper.getInsertOper());
		} catch(SQLException e) {
			throw new DstExecutionException("Failed to add DELETEINSERT values to csv file : ", e);
		}
	}

	@Override
    protected void bindUpsertArgs(Upsert oper) throws SQLException {
		//Add insert record to csv
		try {
			csvPrinter.printRecord(oper.afterValues);
		} catch (IOException e) {
			throw new SQLException("Failed to add UPSERT values to CSV file : ", e);
		}
	}
	
	@Override
    protected void bindUpdateArgs(Update oper) throws SQLException {
		try {
			List<Object> vals = new ArrayList<Object>(oper.beforeValues.size() + oper.afterValues.size());
			vals.addAll(oper.beforeValues);
			vals.addAll(oper.afterValues);
			csvPrinter.printRecord(vals);
		} catch (IOException e) {
			throw new SQLException("Failed to add INSERT values to CSV file : ", e);
		}
	}

	@Override
    protected void bindDeleteArgs(Delete oper) throws SQLException {
		try {
			csvPrinter.printRecord(oper.beforeValues);
		} catch (IOException e) {
			throw new SQLException("Failed to add INSERT values to CSV file : ", e);
		}
	}

	@Override
    protected void addToInsertBatch() throws DstExecutionException {
		//Do nothing
    }

	@Override
    protected void addToUpsertBatch() throws DstExecutionException {
		//Do nothing
    }

	@Override
    protected void addToUpdateBatch() throws DstExecutionException {
		//Do nothing
    }

	@Override
    protected void addToDeleteBatch() throws DstExecutionException {
		//Do nothing
    }

	@Override
    protected boolean isOperPrepared(Oper oper) throws DstExecutionException {
		if ((csvPrinter != null) && (csvFileWriter != null)) {
			return true;
		}
		return false;
    }
    
	private final void resetCSVPrinter() throws DstExecutionException {
		try {
			if (csvPrinter != null){
				csvPrinter.close();
				csvPrinter = null;
				csvFileWriter = null;
			}
		} catch (IOException e) {
			throw new DstExecutionException("Failed to reset a CSV batch : ", e);
		}		
	}
	
	@Override
    protected final void resetBatch() throws DstExecutionException {
		super.resetBatch();
		resetCSVPrinter();
    }


	@Override
	protected void executeInsertBatch() throws DstExecutionException {
		try {
			//Optimization
			//If the batch size is 1 then better to bind and execute prepared statement itself instead of file write and file based load
			//			
			if (enableSingleRecBatchOptimization && (batchOperCount == 1)) {
				//prevBatchOper has the oper. 
				bindAndExecutePreparedInsert((Insert) prevBatchOper);
				resetCSVPrinter();
			} else {				
				csvPrinter.flush();
				resetCSVPrinter();
				putFile();
				executeFileLoaderInsertSql();
				prepareInsertStatement((Insert) prevBatchOper);
		}
		} catch (DstExecutionException | IOException e) {
			throw new DstExecutionException("Failed to flush CSV batch and execute insert batch", e);
		}
	}

	@Override
	protected void executeDeleteInsertBatch() throws DstExecutionException {
		try {
			//Optimization
			//If the batch size is 1 then better to bind and execute prepared statement itself instead of file write and file based load
			//
			if (enableSingleRecBatchOptimization && (batchOperCount == 1)) {
				//prevBatchOper has the oper. 
				bindAndExecutePreparedDelete(((DeleteInsert) prevBatchOper).getDeleteOper());				
				bindAndExecutePreparedInsert(((DeleteInsert) prevBatchOper).getInsertOper());
				resetCSVPrinter();
			} else {				
				csvPrinter.flush();
				resetCSVPrinter();
				putFile();
				executeFileLoaderDeleteSql();
				executeFileLoaderInsertSql();
				prepareDeleteStatement(((DeleteInsert) prevBatchOper).getDeleteOper());
				prepareInsertStatement(((DeleteInsert) prevBatchOper).getInsertOper());
		}
		} catch (DstExecutionException | IOException e) {
			throw new DstExecutionException("Failed to flush CSV batch and execute deleteinsert batch", e);
		}
	}

	@Override
	protected void executeUpsertBatch() throws DstExecutionException {
		//TODO : Implement optimization of batch size 1 as in other operations
		try {
			csvPrinter.flush();
			resetCSVPrinter();
			putFile();
			executeFileLoaderUpsertSql();
			prepareUpsertStatement((Upsert) prevBatchOper);
		} catch (DstExecutionException | IOException e) {
			throw new DstExecutionException("Failed to flush CSV batch and execute upsert batch : ", e);
		}
	}

	@Override
	protected void executeUpdateBatch() throws DstExecutionException {
		try {
			//Optimization
			//If the batch size is 1 then better to bind and execute prepared statement itself instead of file write and file based load
			//
			if (enableSingleRecBatchOptimization && (batchOperCount == 1)) {
				//prevBatchOper has the oper. 
				bindAndExecutePreparedUpdate((Update) prevBatchOper);
				resetCSVPrinter();
			} else {				
				csvPrinter.flush();
				resetCSVPrinter();
				putFile();
				executeFileLoaderUpdateSql();
				prepareUpdateStatement((Update) prevBatchOper);
			}
		} catch (DstExecutionException | IOException e) {
			throw new DstExecutionException("Failed to flush CSV batch and execute update batch : ", e);
		}
	}

	protected abstract void putFile() throws DstExecutionException;

	@Override
	protected void executeDeleteBatch() throws DstExecutionException {
		try {
			//Optimization
			//If the batch size is 1 then better to bind and execute prepared statement itself instead of file write and file based load
			//			
			if (enableSingleRecBatchOptimization && (batchOperCount == 1)) {
				//prevBatchOper has the oper. 
				bindAndExecutePreparedDelete((Delete) prevBatchOper);
				resetCSVPrinter();
			} else {
				csvPrinter.flush();
				resetCSVPrinter();
				putFile();
				executeFileLoaderDeleteSql();
				prepareDeleteStatement((Delete) prevBatchOper);				
			}
		} catch (DstExecutionException | IOException e) {
			throw new DstExecutionException("Failed to flush CSV batch and executed delete batch : ", e);
		}
	
	}
	
	protected void executeFileLoaderInsertSql() throws DstExecutionException {
    	try (Statement stmt = conn.createStatement()) {
    		stmt.execute(fileLoaderInsertSql);
    		fileLoaderInsertSql = null;
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader insert SQL : " + fileLoaderInsertSql, e);
    	}
    }

	protected void executeFileLoaderUpsertSql() throws DstExecutionException {
    	try (Statement stmt = conn.createStatement()) {
    		stmt.execute(fileLoaderUpsertSql);
    		fileLoaderUpsertSql = null;
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader upsert SQL : " + fileLoaderUpsertSql, e);
    	}
    }

	protected void executeFileLoaderUpdateSql() throws DstExecutionException {
    	try (Statement stmt = conn.createStatement()) {
    		stmt.execute(fileLoaderUpdateSql);
    		fileLoaderUpdateSql = null;
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader update SQL : " + fileLoaderUpdateSql, e);
    	}
    }

	protected void executeFileLoaderDeleteSql() throws DstExecutionException {
    	try (Statement stmt = conn.createStatement()) {
    		stmt.execute(fileLoaderDeleteSql);
    		fileLoaderDeleteSql = null;
    	} catch (Exception e) {
    		throw new DstExecutionException("Failed to execute file loader delete SQL : " + fileLoaderDeleteSql, e);
    	}
    }

	protected void prepareInsertStatement(Insert oper) throws DstExecutionException {
        try {
        	createCSVPrinter(oper);        	
        	prepareInsertSql(oper);
            //Keep the original INSERT/UPDATE/DELETE/UPSERT statement prepared for the optimization of batch size 1
           	super.prepareInsertStatement(oper);
        } catch (DstExecutionException e) {
            throw new DstExecutionException("Failed to prepare insert statement : ", e);
        }
    }

	protected void prepareInsertSql(Insert oper) {
    	fileLoaderInsertSql = oper.getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderInsertSql);
	}

	protected void prepareUpdateSql(Update oper) {
    	fileLoaderUpdateSql = oper.getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderUpdateSql);
	}

	protected void prepareUpsertSql(Upsert oper) {
    	fileLoaderUpsertSql = oper.getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderUpsertSql);
	}

	protected void prepareDeleteSql(Delete oper) {
    	fileLoaderDeleteSql = oper.getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderDeleteSql);
	}

	protected void prepareDeleteInsertSql(DeleteInsert oper) {
    	fileLoaderDeleteSql = oper.getDeleteOper().getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderDeleteSql);
    	
    	fileLoaderInsertSql = oper.getInsertOper().getFileLoaderSQL(this.sqlGenerator, localCSVPath);
        tracer.debug(oper.tbl + " SQL : " + fileLoaderInsertSql);
	}

	protected void prepareDeleteInsertStatement(DeleteInsert oper) throws DstExecutionException {
        try {
        	createCSVPrinter(oper);
            prepareDeleteInsertSql(oper);
            //Keep the original INSERT/UPDATE/DELETE/UPSERT statement prepared for the optimization of batch size 1
           	super.prepareDeleteInsertStatement(oper);
        } catch (DstExecutionException e) {
            throw new DstExecutionException("Failed to prepare insert statement : ", e);
        }
    }

	protected void prepareUpsertStatement(Upsert oper) throws DstExecutionException {
        try {
        	createCSVPrinter(oper);
        	prepareUpsertSql(oper);
            //Keep the original INSERT/UPDATE/DELETE/UPSERT statement prepared for the optimization of batch size 1
           	super.prepareUpsertStatement(oper);
        } catch (DstExecutionException e) {
            throw new DstExecutionException("Failed to prepare upsert statement : ", e);
        }
    }

	protected final void prepareUpdateStatement(Update oper) throws DstExecutionException {
        try {
        	createCSVPrinter(oper);
        	prepareUpdateSql(oper);
            //Keep the original INSERT/UPDATE/DELETE/UPSERT statement prepared for the optimization of batch size 1
           	super.prepareUpdateStatement(oper);
        } catch (DstExecutionException e) {
            throw new DstExecutionException("Failed to prepare update statement : ", e);
        }
    }

	protected final void prepareDeleteStatement(Delete oper) throws DstExecutionException {
        try {
        	createCSVPrinter(oper);
        	prepareDeleteSql(oper);
        	//Keep the original INSERT/UPDATE/DELETE/UPSERT statement prepared for the optimization of batch size 1
           	super.prepareDeleteStatement(oper);
        } catch (DstExecutionException e) {
            throw new DstExecutionException("Failed to prepare delete statement : ", e);
        }
    }

	protected final void createCSVPrinter(Oper oper) throws DstExecutionException {
    	//Create new csv file and write header out based on oper.        	
		try {
			csvFileWriter = new FileWriter(localCSVPath.toString());
        	csvPrinter = CSVFormat.DEFAULT
              	  .withHeader(getCSVHeader(oper)).print(csvFileWriter);
		} catch (IOException e) {				
			throw new DstExecutionException("Failed to create a new CSV file and a CSV printer at path : " + localCSVPath, e);
		}
	}

	private final String[] getCSVHeader(Oper oper) {
		switch (oper.operType) {
		case INSERT:
		case UPSERT:	
		case DELETE:
		case DELETEINSERT:	
			String[] header = new String[oper.tbl.columns.size()];
			int idx=0;
			for (Column c : oper.tbl.columns) {
				header[idx] = sqlGenerator.getColumnNameSQL(c);
				++idx;
			}
			return header;
		case UPDATE:
			String[] updateHeader = new String[2 * oper.tbl.columns.size()];
			idx=0;
			for (Column c : oper.tbl.columns) {
				updateHeader[idx] = sqlGenerator.getColumnNameSQL(c) + "b";
				++idx;
			}
			for (Column c : oper.tbl.columns) {
				updateHeader[idx] = sqlGenerator.getColumnNameSQL(c) + "a";
				++idx;
			}
			return updateHeader;
		default:
			throw new UnsupportedOperationException("CSV file loading not supported for oper : " + oper.operType);
		}		
	}	

	
	protected void bindAndExecutePreparedInsert(Insert insertOper) throws DstExecutionException {
		try {
			super.bindInsertArgs(insertOper);
			super.addToInsertBatch();
			super.executeInsertBatch();
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to bind and execute insert operation : " + insertOper, e);
		}		
	}

	protected void bindAndExecutePreparedUpdate(Update updateOper) throws DstExecutionException {
		try {
			super.bindUpdateArgs(updateOper);
			super.addToUpdateBatch();
			super.executeUpdateBatch();
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to bind and execute update operation : " + updateOper, e);
		}		
	}

	protected void bindAndExecutePreparedDelete(Delete deleteOper) throws DstExecutionException {
		try {
			super.bindDeleteArgs(deleteOper);
			super.addToDeleteBatch();
			super.executeDeleteBatch();
		} catch (SQLException e) {
			throw new DstExecutionException("Failed to bind and execute delete operation : " + deleteOper, e);
		}
	}

}
