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

package com.synclite.consolidator.web;

import java.io.IOException;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Servlet implementation class ValidateDBWriterConfiguration
 */
@WebServlet("/validateDBWriterConfiguration")
public class ValidateDBWriterConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public ValidateDBWriterConfiguration() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		Integer dstIndex = 1;
		try {

			String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();

			initTracer(Path.of(deviceDataRoot));

			if (request.getSession().getAttribute("num-destinations") == null) {
				throw new ServletException("\"Number Of Destination Databases\" not configured.");
			}			
			Integer numDestinations = Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
			String dstIndexStr = request.getParameter("dst-index");
			try {
				dstIndex = Integer.valueOf(dstIndexStr);
				if (dstIndex == null) {
					throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
				} else if (dstIndex <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Destination Index\"");
				} else if (dstIndex > numDestinations) {
					throw new ServletException("\"Destination Index\"" + dstIndex + " exceeded configured \"Number Of Destinations Databases\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
			}

		
			String dstInsertBatchSizeStr = request.getParameter("dst-insert-batch-size-" + dstIndex);
			try {
				if (Long.valueOf(dstInsertBatchSizeStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Insert Batch Size\"");
				} else if (Long.valueOf(dstInsertBatchSizeStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Insert Batch Size\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Insert Batch Size\"");
			}
			
			String dstUpdateBatchSizeStr = request.getParameter("dst-update-batch-size-" + dstIndex);
			try {
				if (Long.valueOf(dstUpdateBatchSizeStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Update Batch Size\"");
				} else if (Long.valueOf(dstUpdateBatchSizeStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Update Batch Size\"");
				} 
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Update Batch Size\"");
			}
			
			String dstDeleteBatchSizeStr = request.getParameter("dst-delete-batch-size-" + dstIndex);
			try {
				if (Long.valueOf(dstDeleteBatchSizeStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Delete Batch Size\"");
				} else if (Long.valueOf(dstDeleteBatchSizeStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Delete Batch Size\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Delete Batch Size\"");
			}

			String dstObjectInitMode = request.getParameter("dst-object-init-mode-" + dstIndex);
			if (dstObjectInitMode != null) {
				request.getSession().setAttribute("dst-object-init-mode-" + dstIndex, dstObjectInitMode);
			} else {
				request.getSession().removeAttribute("dst-object-init-mode-" + dstIndex);
			}

			String dstTxnRetryCountStr = request.getParameter("dst-txn-retry-count-" + dstIndex);
			try {
				if (Long.valueOf(dstTxnRetryCountStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Transaction Retry Count\"");
				} else if (Long.valueOf(dstTxnRetryCountStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Transaction Retry Count\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Transaction Retry Count\"");
			}

			String dstTxnRetryIntervalMsStr = request.getParameter("dst-txn-retry-interval-ms-" + dstIndex);
			try {
				if (Long.valueOf(dstTxnRetryIntervalMsStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Transaction Retry Interval\"");
				} else if (Long.valueOf(dstTxnRetryIntervalMsStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Transaction Retry Interval\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Transaction Retry Interval\"");
			}

			String dstIdempotentDataIngestionStr = request.getParameter("dst-idempotent-data-ingestion-" + dstIndex);
			try {
				if (Boolean.valueOf(dstIdempotentDataIngestionStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Enable Idempotent Data Ingestion\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Enable Idempotent Data Ingestion\"");
			}

			String dstIdempotentDataIngestionMethodStr = request.getParameter("dst-idempotent-data-ingestion-method-" + dstIndex);

			String dstSkipFailedLogFilesStr = request.getParameter("dst-skip-failed-log-files-" + dstIndex);
			try {
				if (Boolean.valueOf(dstSkipFailedLogFilesStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Skip Failed Log Files\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Skip Failed Log Files\"");
			}

			String dstDisableMetadataTableStr = request.getParameter("dst-disable-metadata-table-" + dstIndex);
			try {
				Boolean val = Boolean.valueOf(dstDisableMetadataTableStr); 
				if (val == null) {
					throw new ServletException("Please specify a valid boolean value for \"Disable SyncLite Metadata on Destination DB\"");
				}
				if (val == true) {
					if (dstIdempotentDataIngestionStr.equals("false")) {
						throw new ServletException("\"Idempotent Data Ingestion\" must be enabled when SyncLite Metadata is disabed on Destination DB");
					}
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Disable SyncLite Metadata on Destination DB\"");
			}

			String dstSetUnparsableValuesToNullStr = request.getParameter("dst-set-unparsable-values-to-null-" + dstIndex);
			try {
				if (Boolean.valueOf(dstSetUnparsableValuesToNullStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Set Unparsable Values To Null\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Set Unparsable Values To Null\"");
			}

			String dstQuoteObjectNamesStr = request.getParameter("dst-quote-object-names-" + dstIndex);
			try {
				if (Boolean.valueOf(dstQuoteObjectNamesStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Quote Object Names\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Quote Object Names\"");
			}

			String dstQuoteColumnNamesStr = request.getParameter("dst-quote-column-names-" + dstIndex);
			try {
				if (Boolean.valueOf(dstQuoteColumnNamesStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Quote Column Names\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Quote Column Names\"");
			}

			String dstUseCatalogScopeResolutionStr = request.getParameter("dst-use-catalog-scope-resolution-" + dstIndex);
			try {
				if (Boolean.valueOf(dstUseCatalogScopeResolutionStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Use Catalog Scope Resolution\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Use Catalog Scope Resolution\"");
			}

			String dstUseSchemaScopeResolutionStr = request.getParameter("dst-use-schema-scope-resolution-" + dstIndex);
			try {
				if (Boolean.valueOf(dstUseSchemaScopeResolutionStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Use Schema Scope Resolution\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Use Schema Scope Resolution\"");
			}

			String dstDeviceSchemaNamePolicyStr = request.getParameter("dst-device-schema-name-policy-" + dstIndex);
			if (dstDeviceSchemaNamePolicyStr != null) {
				request.getSession().setAttribute("dst-device-schema-name-policy-" + dstIndex, dstDeviceSchemaNamePolicyStr);
			} else {
				request.getSession().removeAttribute("dst-device-schema-name-policy-" + dstIndex); 
			}
			
			String dstClickHouseEngine = request.getParameter("dst-clickhouse-engine-" + dstIndex);
			if (dstClickHouseEngine != null) {
				request.getSession().setAttribute("dst-clickhouse-engine-" + dstIndex, dstClickHouseEngine);
			} else {
				request.getSession().removeAttribute("dst-clickhouse-engine-" + dstIndex);
			}

			String dstCreateTableSuffix = request.getParameter("dst-create-table-suffix-" + dstIndex);
			if ((dstCreateTableSuffix != null) && (!dstCreateTableSuffix.isBlank())) {
				request.getSession().setAttribute("dst-create-table-suffix-" + dstIndex, dstCreateTableSuffix);
			} else {
				request.getSession().removeAttribute("dst-create-table-suffix-" + dstIndex);
			}

			String dstMongoDBUseTransactionsStr= request.getParameter("dst-mongodb-use-transactions-" + dstIndex);
			try {
				if (Boolean.valueOf(dstMongoDBUseTransactionsStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Use MongoDB Transactions\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Use MongoDB Transactions\"");
			}

			if (request.getSession().getAttribute("dst-type-" + dstIndex).toString().equals("CSV")) {
				//Validate CSV format configs				
				//Get and validate csv options.
				String dstCsvFilesWithHeader = request.getParameter("dst-csv-files-with-headers-" + dstIndex);
				String dstCsvFilesFieldDelimiter = request.getParameter("dst-csv-files-field-delimiter-" + dstIndex);
				if (dstCsvFilesFieldDelimiter.isBlank()) {
					throw new ServletException("\"Destination CSV File Field Delimiter\" cannot be blank");
				}
				if (dstCsvFilesFieldDelimiter.length() > 1) {
					throw new ServletException("\"Destination CSV File Field Delimiter\" must be a single character");
				}

				String dstCsvFilesRecordDelimiter = request.getParameter("dst-csv-files-record-delimiter-" + dstIndex);
				if (dstCsvFilesRecordDelimiter.isBlank()) {
					throw new ServletException("\"Destination CSV File Record Delimiter\" cannot be blank");
				}

				String dstCsvFilesEscapeCharacter = request.getParameter("dst-csv-files-escape-character-" + dstIndex);
				if (dstCsvFilesEscapeCharacter.isBlank()) {
					throw new ServletException("\"Destination CSV File Escape Character\" cannot be blank");
				}			
				if (dstCsvFilesEscapeCharacter.length() > 1) {
					throw new ServletException("\"Destination CSV File Escape Character\" must be a single character");
				}

				String dstCsvFilesQuoteCharacter = request.getParameter("dst-csv-files-quote-character-" + dstIndex);
				if (dstCsvFilesQuoteCharacter.isBlank()) {
					throw new ServletException("\"Destination CSV File Quote Character\" cannot be blank");
				}				
				if (dstCsvFilesEscapeCharacter.length() > 1) {
					throw new ServletException("\"Destination CSV File Quote Character\" must be a single character");
				}
				
				String dstCsvFilesNullString = request.getParameter("dst-csv-files-null-string-" + dstIndex);
				
				request.getSession().setAttribute("dst-csv-files-with-headers-" + dstIndex, dstCsvFilesWithHeader);
				request.getSession().setAttribute("dst-csv-files-field-delimiter-" + dstIndex, dstCsvFilesFieldDelimiter);
				request.getSession().setAttribute("dst-csv-files-record-delimiter-" + dstIndex, dstCsvFilesRecordDelimiter);
				request.getSession().setAttribute("dst-csv-files-escape-character-" + dstIndex, dstCsvFilesEscapeCharacter);
				request.getSession().setAttribute("dst-csv-files-quote-character-" + dstIndex, dstCsvFilesQuoteCharacter);
				request.getSession().setAttribute("dst-csv-files-null-string-" + dstIndex, dstCsvFilesNullString);				
			}

			request.getSession().setAttribute("dst-insert-batch-size-" + dstIndex, dstInsertBatchSizeStr);
			request.getSession().setAttribute("dst-update-batch-size-" + dstIndex, dstUpdateBatchSizeStr);
			request.getSession().setAttribute("dst-delete-batch-size-" + dstIndex, dstDeleteBatchSizeStr);
			request.getSession().setAttribute("dst-txn-retry-count-" + dstIndex, dstTxnRetryCountStr);
			request.getSession().setAttribute("dst-txn-retry-interval-ms-" + dstIndex, dstTxnRetryIntervalMsStr);
			request.getSession().setAttribute("dst-idempotent-data-ingestion-" + dstIndex, dstIdempotentDataIngestionStr);
			request.getSession().setAttribute("dst-idempotent-data-ingestion-method-" + dstIndex, dstIdempotentDataIngestionMethodStr);
			request.getSession().setAttribute("dst-disable-metadata-table-" + dstIndex, dstDisableMetadataTableStr);			
			request.getSession().setAttribute("dst-skip-failed-log-files-" + dstIndex, dstSkipFailedLogFilesStr);			
			request.getSession().setAttribute("dst-set-unparsable-values-to-null-" + dstIndex, dstSetUnparsableValuesToNullStr);			
			request.getSession().setAttribute("dst-quote-object-names-" + dstIndex, dstQuoteObjectNamesStr);
			request.getSession().setAttribute("dst-quote-column-names-" + dstIndex, dstQuoteColumnNamesStr);
			request.getSession().setAttribute("dst-use-catalog-scope-resolution-" + dstIndex, dstUseCatalogScopeResolutionStr);
			request.getSession().setAttribute("dst-use-schema-scope-resolution-" + dstIndex, dstUseSchemaScopeResolutionStr);

			request.getSession().setAttribute("dst-clickhouse-engine-" + dstIndex, dstClickHouseEngine);
			request.getSession().setAttribute("dst-create-table-suffix-" + dstIndex, dstCreateTableSuffix);
			request.getSession().setAttribute("dst-mongodb-use-transactions-" + dstIndex, dstMongoDBUseTransactionsStr);
			
			if (dstIndex < numDestinations) {
				++dstIndex;
				//request.getRequestDispatcher("configureDestinationDB.jsp?dstIndex=" + dstIndex).forward(request, response);
				response.sendRedirect("configureDestinationDB.jsp?dstIndex=" + dstIndex);
			} else {
				if (numDestinations > 1) {
					//request.getRequestDispatcher("mapDevicesToDsts.jsp").forward(request, response);
					response.sendRedirect("mapDevicesToDsts.jsp");
				} else {
					//request.getRequestDispatcher("jobSummary.jsp").forward(request, response);
					response.sendRedirect("jobSummary.jsp");
				}
			}
		} catch(Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			String errorMsg = "Exception : " + e.getClass() + " : " + e.getMessage(); 
			this.globalTracer.error("Failed to validate db writer configuration for destination index : " + dstIndex + " : " + e.getMessage(), e);
			request.getRequestDispatcher("configureDBWriter.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ValidateJobConfiguration.class);
		if (this.globalTracer.getAppender("ConsolidatorTracer") == null) {
			globalTracer.setLevel(Level.INFO);
			RollingFileAppender fa = new RollingFileAppender();
			fa.setName("consolidatorTracer");
			fa.setFile(workDir.resolve("synclite_consolidator.trace").toString());
			fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
			fa.setMaxBackupIndex(10);
			fa.setAppend(true);
			fa.activateOptions();
			globalTracer.addAppender(fa);
		}
	}

}
