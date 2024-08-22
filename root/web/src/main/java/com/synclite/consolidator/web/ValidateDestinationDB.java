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
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Servlet implementation class ValidateDestinationDB
 */
@WebServlet("/validateDestinationDB")
public class ValidateDestinationDB extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public ValidateDestinationDB() {
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
			if (request.getSession().getAttribute("num-destinations") == null) {
				throw new ServletException("\"Number Of Destination Databases\" not configured.");
			}			
			Integer numDestinations = Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
			String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();

			initTracer(Path.of(deviceDataRoot));

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
			
			String dstTypeStr = request.getParameter("dst-type-" + dstIndex);

			if (dstTypeStr.equals("CSV")) {
				//Allow CSV only if src app type is DBREADER or QREADER.
				if (request.getSession().getAttribute("src-app-type") != null) {
					String srcAppType = request.getSession().getAttribute("src-app-type").toString();
					if (! srcAppType.equals("SYNCLITE-DBREADER") && ! srcAppType.equals("SYNCLITE-QREADER")) {
						throw new ServletException("\"CSV Files\" destination is not supported for source app type : " + srcAppType);
					}
				}
			}
			
			String dstDataLakeDataFormatStr = "SQLITE";			
			if (request.getParameter("dst-data-lake-data-format-" + dstIndex) != null) {
				dstDataLakeDataFormatStr = request.getParameter("dst-data-lake-data-format-" + dstIndex);
			}
			
			String dstConnectionString = request.getParameter("dst-connection-string-" + dstIndex);
			if (!dstTypeStr.equals("NONE") && !dstTypeStr.equals("CSV") && !dstTypeStr.equals("APACHE_ICEBERG")) {
				if ((dstConnectionString == null) || dstConnectionString.trim().isEmpty()) {
					throw new ServletException("\"JDBC Connection URL\" must be specified");
				}
			}

			String dstUser = null;
			if (request.getParameter("dst-user-" + dstIndex) != null) {
				if (!request.getParameter("dst-user-" + dstIndex).isBlank()) {
					dstUser = request.getParameter("dst-user-" + dstIndex);
				}
			}

			String dstPassword = null;
			if (request.getParameter("dst-password-" + dstIndex) != null) {
				if (!request.getParameter("dst-password-" + dstIndex).isBlank()) {
					dstPassword = request.getParameter("dst-password-" + dstIndex);
				}
			}
			
			String dstDatabase = null;
			if (request.getParameter("dst-database-" + dstIndex) != null) {
				dstDatabase = request.getParameter("dst-database-" + dstIndex);
				if (dstDatabase.isBlank()) {
					throw new ServletException("\"Database Name\" must be specified");
				}
			}
			
			
/*			if ((dstDatabase== null) || dstDatabase.trim().isEmpty()) {
				throw new ServletException("\"Database Name\" must be specified");
			}*/

			String dstSchema = null;
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				dstSchema = request.getParameter("dst-schema-" + dstIndex);
				if (dstSchema.isBlank()) {
					throw new ServletException("\"Schema Name\" must be specified");
				}
			}
/*			if (!request.getSession().getAttribute("dst-sync-mode").toString().equals("REPLICATION")) {
				if ((dstSchema == null) || dstSchema.trim().isEmpty()) {
					throw new ServletException("\"Schema Name\" must be specified");
				}
			}
*/
			
			String dstConnectionTimeoutSStr = request.getParameter("dst-connection-timeout-s-" + dstIndex);
			long dstConnectionTimeoutMs = 30000L;
			try {
				if (Long.valueOf(dstConnectionTimeoutSStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Connection Timeout\"");
				} else if (Long.valueOf(dstConnectionTimeoutSStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Connection Timeout\"");
				}
				dstConnectionTimeoutMs = Long.valueOf(dstConnectionTimeoutSStr) * 1000;
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Connection Timeout\"");
			}

			String dstAlias;
	        String supportedDstAliasFormat = "^[a-zA-Z0-9_-]+$";
			if (request.getParameter("dst-alias-" + dstIndex) != null) {
				dstAlias = request.getParameter("dst-alias-" + dstIndex);			
				if (dstAlias.isBlank()) {
					throw new ServletException("\"Destination DB Alias\" must be specified");
				}
				
				if (!dstAlias.matches(supportedDstAliasFormat)) {
					throw new ServletException("\"Destination DB Alias\" must contain only alphanumeric characters, hyphens, or underscore characters.");
				}
				
				if (dstAlias.length() > 64) {
					throw new ServletException("\"Destination DB Alias\" must be a string of length upto 64 characters.");
				}
			} else {
				throw new ServletException("\"Destination DB Alias\" must be specified");
			}

			//Load respective JDBC driver
			
			DstType dstType = DstType.valueOf(dstTypeStr);
			DstDataLakeDataFormat dstDataLakeDataFormat = DstDataLakeDataFormat.valueOf(dstDataLakeDataFormatStr);
			String dstTypeName = "SQLite";
			
			switch(dstType) {
			case APACHE_ICEBERG:
				dstTypeName = "Apache Iceberg";				
				break;
				
			case DUCKDB:
				dstTypeName = "DuckDB";
				String jdbcPrefix = "jdbc:duckdb:";
				Path dbPath = Path.of(dstConnectionString.substring(jdbcPrefix.length()));
				vaidateDBPath(dbPath);
				break;
				
			case SQLITE:
				dstTypeName = "SQLite";
				jdbcPrefix = "jdbc:sqlite:";
				int lastIndex = dstConnectionString.length();
				if (dstConnectionString.lastIndexOf("?") > 0) {
					lastIndex = dstConnectionString.lastIndexOf("?");
				}
				dbPath = Path.of(dstConnectionString.substring(jdbcPrefix.length(), lastIndex));
				vaidateDBPath(dbPath);
				break;

			case MONGODB:
				dstTypeName = "MongoDB";
				validateMongoDBConnection(dstConnectionString, dstConnectionTimeoutMs, dstDatabase, dstUser, dstPassword);
				break;
				
			case MYSQL:
				dstTypeName = "MySQL";
				Class.forName("com.mysql.cj.jdbc.Driver");
				//Validate connecting to destination DB
				validateConnection(dstConnectionString, dstConnectionTimeoutMs, dstUser, dstPassword);
				break;
				
			case POSTGRESQL :
				dstTypeName = "PostgreSQL";
				Class.forName("org.postgresql.Driver");
				validateConnection(dstConnectionString, dstConnectionTimeoutMs, dstUser, dstPassword);
				break;
			}
											
			request.getSession().setAttribute("dst-type-" + dstIndex, dstType);
			request.getSession().setAttribute("dst-type-name-" + dstIndex, dstTypeName);
			request.getSession().setAttribute("dst-connection-string-" + dstIndex, dstConnectionString);
			request.getSession().setAttribute("dst-connection-timeout-s-" + dstIndex, dstConnectionTimeoutSStr);
			if (dstUser != null) {
				request.getSession().setAttribute("dst-user-" + dstIndex, dstUser);
			} else {
				request.getSession().removeAttribute("dst-user-" + dstIndex);
			}
			if (dstPassword != null) {
				request.getSession().setAttribute("dst-password-" + dstIndex, dstPassword);
			} else {
				request.getSession().removeAttribute("dst-password-" + dstIndex);
			}
			if (dstDatabase != null) {
				request.getSession().setAttribute("dst-database-" + dstIndex, dstDatabase);
			} else {
				request.getSession().removeAttribute("dst-database-" + dstIndex);
			}
			if (dstSchema != null) {
				request.getSession().setAttribute("dst-schema-" + dstIndex, dstSchema);
			} else {
				request.getSession().removeAttribute("dst-schema-" + dstIndex);
			}
			
			if (dstType == DstType.DUCKDB) {
				//Validate duckdb reader port number
				
				String dstDuckDBReaderPortStr;
				if (request.getParameter("dst-duckdb-reader-port-" + dstIndex) != null) {
					dstDuckDBReaderPortStr = request.getParameter("dst-duckdb-reader-port-" + dstIndex);
					try {
						if (Integer.valueOf(dstDuckDBReaderPortStr) == null) {
							throw new ServletException("Please specify a valid numeric value for \"DuckDB Reader Port Number\"");
						} else if (Long.valueOf(dstDuckDBReaderPortStr) <= 0) {
							throw new ServletException("Please specify a positive numeric value for \"DuckDB Reader Port Number\"");
						}
					} catch(NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"DuckDB Reader Port Number\"");
					}
					
					//Validate if the port is available
					int port = Integer.valueOf(dstDuckDBReaderPortStr);
					try (ServerSocket serverSocket = new ServerSocket(port)) {
						//Port is available
					} catch (Exception e) {
						throw new ServletException("Specified DuckDB Reader port " + port + " is not available. Please specify a different port number");
					}
					
					request.getSession().setAttribute("dst-duckdb-reader-port-" + dstIndex, dstDuckDBReaderPortStr);
				} else {
					throw new ServletException("\"DuckDB Reader Port Number\" must be specified");
				}
			} else {
				request.getSession().removeAttribute("dst-duckdb-reader-port-" + dstIndex);
			}
			
			if (dstType == DstType.POSTGRESQL) {
				if (request.getParameter("dst-postgresql-vector-extension-enabled-" + dstIndex) != null) {
					String val = request.getParameter("dst-postgresql-vector-extension-enabled-" + dstIndex);
					request.getSession().setAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex, val);
				} else {
					request.getSession().removeAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex);
				}
			}
			
			//If MongoDB then append a jvm-argument
			//MongoDB Connections require this -Djdk.tls.client.protocols=TLSv1.2
			//
			if (dstType == DstType.MONGODB) {
				String jvmArguments = "";
				if (request.getSession().getAttribute("jvm-arguments") != null) {
					if (! jvmArguments.contains("-Djdk.tls.client.protocols=TLSv1.2")) {
						jvmArguments = request.getSession().getAttribute("jvm-arguments").toString() + " -Djdk.tls.client.protocols=TLSv1.2";
					}
				} else {
					jvmArguments = "-Djdk.tls.client.protocols=TLSv1.2";
				}
				request.getSession().setAttribute("jvm-arguments", jvmArguments);
			}			
			
			if (dstType == DstType.APACHE_ICEBERG) {
				String dstSparkConfigurations = request.getParameter("dst-spark-configuration-" + dstIndex);
				if (dstSparkConfigurations.isBlank()) {
					throw new ServletException("\"Spark Configurations\" cannot be blank");
				}
				String dstSparkConfPath = Path.of(deviceDataRoot, "dst_spark_configuration_" + dstIndexStr +".conf").toString();				
				try {
					Files.writeString(Path.of(dstSparkConfPath), dstSparkConfigurations);
				} catch (IOException e) {
					throw new ServletException("Failed to write spark configurations into configuration file : " + dstSparkConfPath + " : " + e.getMessage(), e);
				}
				request.getSession().setAttribute("dst-spark-configuration-file-" + dstIndex, dstSparkConfPath);
				
				if (System.getProperty("os.name").startsWith("Windows")) {
					//Copy winutils.exe
					Path hadoopHomeDir = Path.of(deviceDataRoot, "hadoopHome", "bin");
					try {
						Files.createDirectories(hadoopHomeDir);
						Path dstPath = hadoopHomeDir.resolve("winutils.exe");
						Path srcPath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib", "lib", "winutils.exe");
						Files.copy(srcPath, dstPath);
					} catch (IOException e) {
						throw new ServletException("Failed to copy winutils.exe to hadoopHome : " + hadoopHomeDir + " : " + e.getMessage(), e);
					}
				}
			}
				
			request.getSession().setAttribute("dst-alias-" + dstIndex, dstAlias);
			//request.getRequestDispatcher("configureDataTypes.jsp?dstIndex=" + dstIndex).forward(request, response);
			response.sendRedirect("configureDataTypes.jsp?dstIndex=" + dstIndex);

		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			globalTracer.error("Failed to validate configured destination db : " +  " : " + e.getMessage(), e);
			request.getRequestDispatcher("configureDestinationDB.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private final void validateConnection(String dstConnectionString, long dstConnectionTimeoutMs, String user, String password) throws ServletException {
		if (user == null) {
			Properties properties = new Properties();
			properties.setProperty("connectionTimeout", String.valueOf(dstConnectionTimeoutMs));
			try(Connection conn = DriverManager.getConnection(dstConnectionString, properties)) {
				//Do nothing
			} catch (SQLException e){
				throw new ServletException("Failed to connect to destination database. Please verify specified connection string." +  e.getMessage(), e);
			}
		} else {
			Properties properties = new Properties();			
			properties.setProperty("user", user);			
			if (password != null) {
				properties.setProperty("password", password);
			}
			properties.setProperty("connectionTimeout", String.valueOf(dstConnectionTimeoutMs));
			try(Connection conn = DriverManager.getConnection(dstConnectionString, properties)) {
				//Do nothing
			} catch (SQLException e){
				throw new ServletException("Failed to connect to destination database. Please verify specified connection string." +  e.getMessage(), e);
			}
		}
	}

	private final void validateMongoDBConnection(String dstConnectionString, long dstConnectionTimeoutMs, String dbName, String user, String password) throws ServletException {
		try {	        
			/*
	        ServerApi serverApi = ServerApi.builder()
	                .version(ServerApiVersion.V1)
	                .build();
	        MongoClientSettings settings = MongoClientSettings.builder()
	                .applyConnectionString(new ConnectionString(dstConnectionString))
	                .serverApi(serverApi)
	                .build();
	        
	        try (MongoClient mongoClient = MongoClients.create(settings)) {	        
		        MongoDatabase db = mongoClient.getDatabase(dbName);
		        db.listCollectionNames().first();
			}
			*/

	        try (MongoClient mongoClient = MongoClients.create(dstConnectionString)) {	        
		        MongoDatabase db = mongoClient.getDatabase(dbName);
		        db.listCollectionNames().first();
			}

		} catch (Exception e) {
			throw new ServletException("Failed to connect to destination MongoDB database : " + e.getMessage(), e);
		}
	}

	private final void vaidateDBPath(Path dbPath) throws ServletException {
		if (dbPath == null) {
			throw new ServletException("Invalid connection string specified. Please verify specified connection string.");
		}
		Path parentDir = dbPath.getParent();
		if (parentDir.toFile().exists()) {
			if (!parentDir.toFile().canWrite()) {
				throw new ServletException("The directory specified in the connection string is not writable : " + parentDir + ". Please verify specified connection string.");
			}
		} else {
			throw new ServletException("The directory specified in the connection string is invalid. Please verify specified connection string.");
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
