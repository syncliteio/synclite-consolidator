<%-- 
    Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
    in compliance with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
    or implied.  See the License for the specific language governing permissions and limitations
    under the License.
--%>

<%@page import="com.synclite.consolidator.web.DstType"%>
<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.HashMap"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<%
String errorMsg = request.getParameter("errorMsg");
HashMap<String, Object> properties = new HashMap<String, Object>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}


properties.put("job-name", session.getAttribute("job-name").toString());
Integer dstIndex = 1;

if (request.getParameter("dstIndex") != null) {
	dstIndex = Integer.valueOf(request.getParameter("dstIndex"));
}
Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());

String defaultConnStrCSV = "Not Applicable";
String defaultConnStrClickHouse = "jdbc:ch://localhost:8123?username=synclite&password=synclite";
String defaultConnStrDuckDB = "jdbc:duckdb:" + Path.of(properties.get("device-data-root").toString(), "consolidated_db_" + dstIndex + ".duckdb");
String defaultConnStrFerretDB = "mongodb://synclite:synclite@localhost:27017/ferretdb?authMechanism=PLAIN";
String defaultConnStrMongoDB = "mongodb://localhost:27017/?w=majority";
String defaultConnStrMySQL = "jdbc:mysql://127.0.0.1:3306/syncliteschema?user=synclite&password=synclite";
String defaultConnStrPostgreSQL = "jdbc:postgresql://127.0.0.1:5432/synclitedb?user=synclite&password=synclite"; 
String defaultConnStrSQLite = "jdbc:sqlite:" + Path.of(properties.get("device-data-root").toString(), "consolidated_db_" + dstIndex + ".sqlite") + "?journal_mode=WAL";
String defaultConnStrSQLServer = "jdbc:sqlserver://localhost:1433;encrypt=true;trustServerCertificate=true;username=synclite;password=synclite;databaseName=synclitedb";

if (request.getParameter("dst-type-" + dstIndex) != null) {
	properties.put("dst-type-" + dstIndex, request.getParameter("dst-type-" + dstIndex));
	switch (request.getParameter("dst-type-" + dstIndex).toString()) {
		case "APACHE_ICEBERG":
			properties.put("dst-database-" + dstIndex, "synclitedb");
			properties.put("dst-schema-" + dstIndex, "Not Applicable");
			properties.put("dst-user-" + dstIndex, "Not Applicable");
			properties.put("dst-password-" + dstIndex, "Not Applicable");
			properties.put("dst-connection-string-" + dstIndex, "Not Applicable");

			StringBuilder sparkConfigurationBuilder = new StringBuilder();
			String defaultWareHousePath = Path.of(properties.get("device-data-root").toString(), "icebergWarehouse" + dstIndex).toString();
			sparkConfigurationBuilder.append("spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog").append("\n");
			sparkConfigurationBuilder.append("spark.sql.catalog.spark_catalog.type = hadoop").append("\n");
			sparkConfigurationBuilder.append("spark.sql.catalog.spark_catalog.warehouse = file:///" + defaultWareHousePath.replace("\\", "\\\\"));
			properties.put("dst-spark-configuration-" + dstIndex, sparkConfigurationBuilder.toString());
			break;
			
		case "SQLITE" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrSQLite);
			}
			
			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "Not Applicable");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "Not Applicable");
			}
			properties.put("dst-user-" + dstIndex, "Not Applicable");
			properties.put("dst-password-" + dstIndex, "Not Applicable");
			break;

		case "CLICKHOUSE" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrClickHouse);
			}

			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "synclitedb");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "Not Applicable");
			}
			break;
			
		case "DUCKDB" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrDuckDB);
			}
			
			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "Not Applicable");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "syncliteschema");
			}
			
			if (request.getParameter("dst-duckdb-reader-port-" + dstIndex) != null) {
				properties.put("dst-duckdb-reader-port-", request.getParameter("dst-duckdb-reader-port-" + dstIndex));
			} else {
				int defaultDuckDBReaderPort = 10000 - dstIndex;
				properties.put("dst-duckdb-reader-port-" + dstIndex, String.valueOf(defaultDuckDBReaderPort));					
			}
			
			properties.put("dst-user-" + dstIndex, "Not Applicable");
			properties.put("dst-password-" + dstIndex, "Not Applicable");
			break;

		case "MONGODB" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrMongoDB);
			}

			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "synclitedb");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "Not Applicable");
			}
			break;
			
		case "FERRETDB" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrFerretDB);
			}

			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "synclitedb");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "Not Applicable");
			}
			break;
			
		case "MYSQL" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrMySQL);
			}

			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "Not Applicable");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "syncliteschema");
			}
			break;

		case "POSTGRESQL" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrPostgreSQL);
			}
			
			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "synclitedb");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "syncliteschema");
			}
			break;
			
		case "MSSQL" :
			if (request.getParameter("dst-connection-string-" + dstIndex) != null) {
				properties.put("dst-connection-string-" + dstIndex, request.getParameter("dst-connection-string-" + dstIndex));
			} else {
				properties.put("dst-connection-string-" + dstIndex, defaultConnStrSQLServer);
			}

			if (request.getParameter("dst-user-" + dstIndex) != null) {
				properties.put("dst-user-" + dstIndex, request.getParameter("dst-user-" + dstIndex));
			} else {
				properties.put("dst-user-" + dstIndex, "");
			}

			if (request.getParameter("dst-password-" + dstIndex) != null) {
				properties.put("dst-password-" + dstIndex, request.getParameter("dst-password-" + dstIndex));
			} else {
				properties.put("dst-password-" + dstIndex, "");
			}

			if (request.getParameter("dst-database-" + dstIndex) != null) {
				properties.put("dst-database-" + dstIndex, request.getParameter("dst-database-" + dstIndex));
			} else {
				properties.put("dst-database-" + dstIndex, "synclitedb");
			}
			
			if (request.getParameter("dst-schema-" + dstIndex) != null) {
				properties.put("dst-schema-" + dstIndex, request.getParameter("dst-schema-" + dstIndex));
			} else {
				properties.put("dst-schema-" + dstIndex, "syncliteschema");
			}
			break;
	
	}

	if (request.getParameter("dst-connection-timeout-s-" + dstIndex) != null) {
		properties.put("dst-connection-timeout-s-" + dstIndex, request.getParameter("dst-connection-timeout-s-" + dstIndex));
	} else {
		properties.put("dst-connection-timeout-s-" + dstIndex, "30");
	}

	if (request.getParameter("dst-postgresql-vector-extension-enabled-" + dstIndex) != null) {
		properties.put("dst-postgresql-vector-extension-enabled-" + dstIndex, request.getParameter("dst-postgresql-vector-extension-enabled-" + dstIndex));
	} else {
		properties.put("dst-postgresql-vector-extension-enabled-" + dstIndex, "false");
	}

	if (request.getParameter("dst-alias-" + dstIndex) != null) {
		properties.put("dst-alias-" + dstIndex, request.getParameter("dst-alias-" + dstIndex));
	} else {
		properties.put("dst-alias-" + dstIndex, "DB-" + dstIndex);
	}

	if (request.getParameter("dst-data-lake-object-switch-interval-" + dstIndex) != null) {
		properties.put("dst-data-lake-object-switch-interval-" + dstIndex, request.getParameter("dst-data-lake-object-switch-interval-" + dstIndex));
	} else {
		properties.put("dst-data-lake-object-switch-interval-" + dstIndex, "1");				
	}
	
	if (request.getParameter("dst-data-lake-object-switch-interval-unit-" + dstIndex) != null) {
		properties.put("dst-data-lake-object-switch-interval-unit-" + dstIndex, request.getParameter("dst-data-lake-object-switch-interval-unit-" + dstIndex));
	} else {
		properties.put("dst-data-lake-object-switch-interval-unit-" + dstIndex, "DAYS");				
	}

	if (request.getParameter("dst-data-lake-export-object-on-finish-batch-" + dstIndex) != null) {
		properties.put("dst-data-lake-export-object-on-finish-batch-" + dstIndex, request.getParameter("dst-data-lake-export-object-on-finish-batch-" + dstIndex));
	} else {
		if(request.getSession().getAttribute("src-app-type").toString().equals("SYNCLITE-DBREADER")) {
			properties.put("dst-data-lake-export-object-on-finish-batch-" + dstIndex, "true");
		} else {
			properties.put("dst-data-lake-export-object-on-finish-batch-" + dstIndex, "false");
		}
	}
	
	
	if (request.getParameter("dst-data-lake-local-storage-dir-" + dstIndex) != null) {
		properties.put("dst-data-lake-local-storage-dir-" + dstIndex, request.getParameter("dst-data-lake-local-storage-dir-" + dstIndex));
	} else {
		Path defaultDataLakeRoot = Path.of(System.getProperty("user.home"), "synclite", "workDir", "DB-" + dstIndex, "datalake");
		properties.put("dst-data-lake-local-storage-dir-" + dstIndex, defaultDataLakeRoot);
	}
	
	if (request.getParameter("dst-data-lake-publishing-" + dstIndex) != null) {
		properties.put("dst-data-lake-publishing-" + dstIndex, request.getParameter("dst-data-lake-publishing-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-publishing-" + dstIndex, "false");
	}
	
	if (request.getParameter("dst-data-lake-type-" + dstIndex) != null) {
		properties.put("dst-data-lake-type-" + dstIndex, request.getParameter("dst-data-lake-type-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-type-" + dstIndex, "S3");
	}

	if (request.getParameter("dst-data-lake-s3-endpoint-" + dstIndex) != null) {
		properties.put("dst-data-lake-s3-endpoint-" + dstIndex, request.getParameter("dst-data-lake-s3-endpoint-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-s3-endpoint-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-s3-bucket-name-" + dstIndex) != null) {
		properties.put("dst-data-lake-s3-bucket-name-" + dstIndex, request.getParameter("dst-data-lake-s3-bucket-name-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-s3-bucket-name-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-s3-access-key-" + dstIndex) != null) {
		properties.put("dst-data-lake-s3-access-key-" + dstIndex, request.getParameter("dst-data-lake-s3-access-key-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-s3-access-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-s3-secret-key-" + dstIndex) != null) {
		properties.put("dst-data-lake-s3-secret-key-" + dstIndex, request.getParameter("dst-data-lake-s3-secret-key-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-s3-secret-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-minio-endpoint-" + dstIndex) != null) {
		properties.put("dst-data-lake-minio-endpoint-" + dstIndex, request.getParameter("dst-data-lake-minio-endpoint-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-minio-endpoint-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-minio-bucket-name-" + dstIndex) != null) {
		properties.put("dst-data-lake-minio-bucket-name-" + dstIndex, request.getParameter("dst-data-lake-minio-bucket-name-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-minio-bucket-name-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-minio-access-key-" + dstIndex) != null) {
		properties.put("dst-data-lake-minio-access-key-" + dstIndex, request.getParameter("dst-data-lake-minio-access-key-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-minio-access-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-data-lake-minio-secret-key-" + dstIndex) != null) {
		properties.put("dst-data-lake-minio-secret-key-" + dstIndex, request.getParameter("dst-data-lake-minio-secret-key-" + dstIndex));
	} else {		
		properties.put("dst-data-lake-minio-secret-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-type-" + dstIndex) != null) {
		properties.put("dst-file-storage-type-" + dstIndex, request.getParameter("dst-file-storage-type-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-type-" + dstIndex, "LOCAL_FS");
	}

	if (request.getParameter("dst-file-storage-local-fs-directory-" + dstIndex) != null) {
		properties.put("dst-file-storage-local-fs-directory-" + dstIndex, request.getParameter("dst-file-storage-local-fs-directory-" + dstIndex));
	} else {		
		Path defaultLocalFSDirectory = Path.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "workDir", "DB-" + dstIndex, "export");
		properties.put("dst-file-storage-local-fs-directory-" + dstIndex, defaultLocalFSDirectory.toString());
	}

	if (request.getParameter("dst-file-storage-sftp-host-" + dstIndex) != null) {
		properties.put("dst-file-storage-sftp-host-" + dstIndex, request.getParameter("dst-file-storage-sftp-host-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-sftp-host-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-sftp-port-" + dstIndex) != null) {
		properties.put("dst-file-storage-sftp-port-" + dstIndex, request.getParameter("dst-file-storage-sftp-port-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-sftp-port-" + dstIndex, "22");
	}
	
	if (request.getParameter("dst-file-storage-sftp-directory-" + dstIndex) != null) {
		properties.put("dst-file-storage-sftp-directory-" + dstIndex, request.getParameter("dst-file-storage-sftp-directory-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-sftp-directory-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-sftp-user-" + dstIndex) != null) {
		properties.put("dst-file-storage-sftp-user-" + dstIndex, request.getParameter("dst-file-storage-sftp-user-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-sftp-user-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-sftp-password-" + dstIndex) != null) {
		properties.put("dst-file-storage-sftp-password-" + dstIndex, request.getParameter("dst-file-storage-sftp-password-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-sftp-password-" + dstIndex, "");
	}
	
	if (request.getParameter("dst-file-storage-s3-url-" + dstIndex) != null) {
		properties.put("dst-file-storage-s3-url-" + dstIndex, request.getParameter("dst-file-storage-s3-url-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-s3-url-" + dstIndex, "");
	}
	
	if (request.getParameter("dst-file-storage-s3-bucket-name-" + dstIndex) != null) {
		properties.put("dst-file-storage-s3-bucket-name-" + dstIndex, request.getParameter("dst-file-storage-s3-bucket-name-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-s3-bucket-name-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-s3-access-key-" + dstIndex) != null) {
		properties.put("dst-file-storage-s3-access-key-" + dstIndex, request.getParameter("dst-file-storage-s3-access-key-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-s3-access-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-s3-secret-key-" + dstIndex) != null) {
		properties.put("dst-file-storage-s3-secret-key-" + dstIndex, request.getParameter("dst-file-storage-s3-secret-key-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-s3-secret-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-minio-url-" + dstIndex) != null) {
		properties.put("dst-file-storage-minio-url-" + dstIndex, request.getParameter("dst-file-storage-minio-url-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-minio-url-" + dstIndex, "");
	}
	
	if (request.getParameter("dst-file-storage-minio-bucket-name-" + dstIndex) != null) {
		properties.put("dst-file-storage-minio-bucket-name-" + dstIndex, request.getParameter("dst-file-storage-minio-bucket-name-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-minio-bucket-name-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-minio-access-key-" + dstIndex) != null) {
		properties.put("dst-file-storage-minio-access-key-" + dstIndex, request.getParameter("dst-file-storage-minio-access-key-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-minio-access-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-file-storage-minio-secret-key-" + dstIndex) != null) {
		properties.put("dst-file-storage-minio-secret-key-" + dstIndex, request.getParameter("dst-file-storage-minio-secret-key-" + dstIndex));
	} else {		
		properties.put("dst-file-storage-minio-secret-key-" + dstIndex, "");
	}

	if (request.getParameter("dst-databricks-dbfs-endpoint-" + dstIndex) != null) {
		properties.put("dst-databricks-dbfs-endpoint-" + dstIndex, request.getParameter("dst-databricks-dbfs-endpoint-" + dstIndex));
	} else {
		properties.put("dst-databricks-dbfs-endpoint-" + dstIndex, "https://adb-123425678912345.11.azuredatabricks.net/");
	}

	if (request.getParameter("dst-databricks-dbfs-access-token-" + dstIndex) != null) {
		properties.put("dst-databricks-dbfs-access-token-" + dstIndex, request.getParameter("dst-databricks-dbfs-access-token-" + dstIndex));
	} else {
		properties.put("dst-databricks-dbfs-access-token-" + dstIndex, "");
	}

	if (request.getParameter("dst-databricks-dbfs-basepath-" + dstIndex) != null) {
		properties.put("dst-databricks-dbfs-basepath-" + dstIndex, request.getParameter("dst-databricks-dbfs-basepath-" + dstIndex));
	} else {
		properties.put("dst-databricks-dbfs-basepath-" + dstIndex, "/files");
	}
} else {
	//Popuate default values in the map
	properties.put("dst-type-" + dstIndex, "SQLITE");
	properties.put("dst-connection-string-" + dstIndex, defaultConnStrSQLite);
	properties.put("dst-user-" + dstIndex, "Not Applicable");
	properties.put("dst-password-" + dstIndex, "Not Applicable");
	properties.put("dst-database-" + dstIndex, "Not Applicable");
	properties.put("dst-schema-" + dstIndex, "Not Applicable");
	properties.put("dst-connection-timeout-s-" + dstIndex, "30");
	properties.put("dst-alias-" + dstIndex, "DB-" + dstIndex);
	properties.put("dst-data-lake-data-format-" + dstIndex, "SQLITE");
	properties.put("dst-data-lake-object-switch-interval-" + dstIndex, "1");
	properties.put("dst-data-lake-object-switch-interval-unit-" + dstIndex, "DAYS");
	Path defaultDataLakeRoot = Path.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "workDir", "DB-" + dstIndex, "datalake");
	properties.put("dst-data-lake-local-storage-dir-" + dstIndex, defaultDataLakeRoot);
	properties.put("dst-data-lake-publishing-" + dstIndex, "false");
	properties.put("dst-data-lake-type-" + dstIndex, "S3");
	properties.put("dst-data-lake-s3-endpoint-" + dstIndex, "");
	properties.put("dst-data-lake-s3-bucket-name-" + dstIndex, "");
	properties.put("dst-data-lake-s3-access-key-" + dstIndex, "");
	properties.put("dst-data-lake-s3-secret-key-" + dstIndex, "");
	properties.put("dst-data-lake-minio-endpoint-" + dstIndex, "");
	properties.put("dst-data-lake-minio-bucket-name-" + dstIndex, "");
	properties.put("dst-data-lake-minio-access-key-" + dstIndex, "");
	properties.put("dst-data-lake-minio-secret-key-" + dstIndex, "");

	
	//Read configs from syncJobs.props if they are present
	Path propsPath = Path.of(properties.get("device-data-root").toString(), "synclite_consolidator.conf");
	BufferedReader reader = null;
	try {
		if (Files.exists(propsPath)) {		
	   	    reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (!line.startsWith("=")) {								
							properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
						}
					}
				}  else {
					properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				}					
				line = reader.readLine();
			}
			reader.close();
		}
	} catch (Exception e) { 
		if (reader != null) {
			reader.close();
		}
		throw e;
	} 

	//Read spark configuration from config file if prsent
	if (properties.get("dst-type-" + dstIndex).equals("APACHE_ICEBERG")) {
		Path sparkConfFile = Path.of(properties.get("device-data-root").toString(), "dst_spark_configuration_" + dstIndex + ".conf");
		if (Files.exists(sparkConfFile)) {
			reader = null;
			String readSparkConf = Files.readString(sparkConfFile);
			if (readSparkConf != null) {
				properties.put("dst-spark-configuration-" + dstIndex, readSparkConf);
			}
		}
	}

	/*
	//Reset the default conn str to preconfigured conn str for respective destination if any.	
	switch(properties.get("dst-type-" + dstIndex).toString()) {
	case "SQLITE" :
	case "SQLITE_DATALAKE" :	
		defaultConnStrSQLite = properties.get("dst-connection-string-" + dstIndex).toString();
		break;
	case "DUCKDB" :
	case "DUCKDB_DATALAKE" :	
		defaultConnStrDuckDB = properties.get("dst-connection-string-" + dstIndex).toString();
		break;
	case "MSSQL" :
		defaultConnStrSQLServer = properties.get("dst-connection-string-" + dstIndex).toString();
		break;
	case "MYSQL" :
		defaultConnStrMySQL = properties.get("dst-connection-string-" + dstIndex).toString();
		break;
	case "POSTGRESQL" :
		defaultConnStrPostgreSQL = properties.get("dst-connection-string-" + dstIndex).toString();
		break;
	case "SNOWFLAKE" :
		defaultConnStrSnowflake = properties.get("dst-connection-string-" + dstIndex).toString();
		break;		
	}
	*/
}
%>

<script type="text/javascript">

	function resetFields() {
		var element = document.getElementById("dst-connection-string-<%=dstIndex%>");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("dst-user-<%=dstIndex%>");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("dst-password-<%=dstIndex%>");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("dst-database-<%=dstIndex%>");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("dst-schema-<%=dstIndex%>");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
	}
	
	function populateDefaults() {
		var dstType = document.getElementById("dst-type-<%=dstIndex%>").value;
		if (dstType.toString() === "SQLITE") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-database-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-database-<%=dstIndex%>").readonly= true;			
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-schema-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-schema-<%=dstIndex%>").readonly = true;
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrSQLite.replace("\\", "\\\\")%>"
			document.getElementById("dst-user-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-user-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-user-<%=dstIndex%>").readonly= true;		
			document.getElementById("dst-password-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-password-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-password-<%=dstIndex%>").readonly= true;			
		} else if (dstType.toString() === "APACHE_ICEBERG") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-schema-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-schema-<%=dstIndex%>").readonly = true;
			document.getElementById("dst-user-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-user-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-user-<%=dstIndex%>").readonly= true;		
			document.getElementById("dst-password-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-password-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-password-<%=dstIndex%>").readonly= true;
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrCSV%>";
		} else if (dstType.toString() === "DUCKDB") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-database-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-database-<%=dstIndex%>").readonly = true;
			document.getElementById("dst-schema-<%=dstIndex%>").value = "syncliteschema";
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrDuckDB.replace("\\", "\\\\")%>";
			document.getElementById("dst-user-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-user-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-user-<%=dstIndex%>").readonly= true;		
			document.getElementById("dst-password-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-password-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-password-<%=dstIndex%>").readonly= true;			
		} else if (dstType.toString() === "MONGODB") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrMongoDB%>";
		} else if (dstType.toString() === "FERRETDB") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrFerretDB%>";
		} else if (dstType.toString() === "CLICKHOUSE") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-database-<%=dstIndex%>").disabled = false;
			document.getElementById("dst-database-<%=dstIndex%>").readonly= false;
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-schema-<%=dstIndex%>").disabled= true;
			document.getElementById("dst-schema-<%=dstIndex%>").readonly = true;			
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrClickHouse%>";
		} else if (dstType.toString() === "MYSQL") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-database-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-database-<%=dstIndex%>").readonly= true;
			document.getElementById("dst-schema-<%=dstIndex%>").value = "syncliteschema";
			document.getElementById("dst-schema-<%=dstIndex%>").disabled= false;
			document.getElementById("dst-schema-<%=dstIndex%>").readonly = false;			
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrMySQL%>";		
		} else if (dstType.toString() === "MSSQL") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "syncliteschema";
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrSQLServer%>";		
		} else if (dstType.toString() === "POSTGRESQL") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "syncliteschema";
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrPostgreSQL%>";
		} else if (dstType.toString() === "SQLITE") {
			document.getElementById("dst-database-<%=dstIndex%>").value = "synclitedb";
			document.getElementById("dst-schema-<%=dstIndex%>").value = "Not Applicable";
			document.getElementById("dst-schema-<%=dstIndex%>").disabled = true;
			document.getElementById("dst-schema-<%=dstIndex%>").readonly = true;
			document.getElementById("dst-connection-string-<%=dstIndex%>").value = "<%=defaultConnStrSQLite%>";
		} 	 
	}
</script>
<title>Configure Destination Database</title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">		
		<%
		if (numDestinations == 1) {
			out.println("<h2>Configure Destination Database</h2>");
		} else {
			out.println("<h2>Configure Destination Database " + dstIndex + "</h2>");
		}
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDestinationDB"
			method="post">

			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
			<table>
				<tbody>
					<tr>
						<td>Destination Type</td>
						<td><select id="dst-type-<%=dstIndex%>" name="dst-type-<%=dstIndex%>" value="<%=properties.get("dst-type-" + dstIndex)%>" onchange="this.form.action='configureDestinationDB.jsp?dstIndex=<%=dstIndex%>'; resetFields(); this.form.submit();" title="Select destination database type.">
								<%
								if (properties.get("dst-type-" + dstIndex).equals("APACHE_ICEBERG")) {									
									out.println("<option value=\"APACHE_ICEBERG\" selected>Apache Iceberg</option>");
								} else {
									out.println("<option value=\"APACHE_ICEBERG\">Apache Iceberg</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("CLICKHOUSE")) {
									out.println("<option value=\"CLICKHOUSE\" selected>ClickHouse</option>");
								} else {
									out.println("<option value=\"CLICKHOUSE\">ClickHouse</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("DUCKDB")) {
									out.println("<option value=\"DUCKDB\" selected>DuckDB</option>");
								} else {
									out.println("<option value=\"DUCKDB\">DuckDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("FERRETDB")) {
									out.println("<option value=\"FERRETDB\" selected>FerretDB</option>");
								} else {
									out.println("<option value=\"FERRETDB\">FerretDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MONGODB")) {
									out.println("<option value=\"MONGODB\" selected>MongoDB</option>");
								} else {
									out.println("<option value=\"MONGODB\">MongoDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MYSQL")) {
									out.println("<option value=\"MYSQL\" selected>MySQL</option>");
								} else {
									out.println("<option value=\"MYSQL\">MySQL</option>");
								}							
								if (properties.get("dst-type-" + dstIndex).equals("MSSQL")) {
									out.println("<option value=\"MSSQL\" selected>Microsoft SQL Server</option>");
								} else {
									out.println("<option value=\"MSSQL\">Microsoft SQL Server</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("POSTGRESQL")) {
									out.println("<option value=\"POSTGRESQL\" selected>PostgreSQL</option>");
								} else {
									out.println("<option value=\"POSTGRESQL\">PostgreSQL</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("SQLITE")) {
									out.println("<option value=\"SQLITE\" selected>SQLite</option>");
								} else {
									out.println("<option value=\"SQLITE\">SQLite</option>");
								}
								%>
							</select>
						</td>
					</tr>					
					<% 
						if (properties.get("dst-type-" + dstIndex).toString().equals("APACHE_ICEBERG")) {
							out.println("<tr>");
							out.println("<td>Spark Configurations</td>");
							out.println("<td><textarea id=\"dst-spark-configuration-" + dstIndex + "\" name=\"dst-spark-configuration-" + dstIndex + "\" rows=\"10\" cols=\"120\" title=\"Specify spark configurations as <confName>=<confValue> pairs, one line per config\">" + properties.get("dst-spark-configuration-" + dstIndex) + "</textarea></td>");
							out.println("</tr>");
						}
						if (properties.get("dst-type-" + dstIndex).equals("POSTGRESQL")) {
							out.println("<tr><td>PG Vector Extension Enabled</td>");
							out.println("<td><select id=\"dst-postgresql-vector-extension-enabled-" + dstIndex + "\" name=\"dst-postgresql-vector-extension-enabled-" + dstIndex  +"\" value=\"" + properties.get("dst-postgresql-vector-extension-" + dstIndex) + "\" title=\"Specify destination file storage type\">");
							if (properties.get("dst-postgresql-vector-extension-enabled-" + dstIndex).equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("dst-postgresql-vector-extension-enabled-" + dstIndex).equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select></td></tr>");
						}
					%>

					<tr>
						<% 
							switch (properties.get("dst-type-" + dstIndex).toString()) {
							case "MONGODB":
								out.println("<td>Connection String</td>");
								break;
							default:
								out.println("<td>JDBC Connection URL</td>");
							}
						%>
						<td><input type="text" size = 80 id="dst-connection-string-<%=dstIndex%>"
							name="dst-connection-string-<%=dstIndex%>"
							value="<%=properties.get("dst-connection-string-" + dstIndex)%>" 
							title="Specify a complete JDBC connection URL to connect to the destination database. Make sure the URL contains all properties required for a successful JDBC connection."
							<%
							if (properties.get("dst-connection-string-" + dstIndex).equals("Not Applicable")) {
								out.print(" disabled");
							}
							%>/>
						</td>
					</tr>
				
					<tr>
						<td>User Name</td>
						<td><input type="text" id="dst-user-<%=dstIndex%>" name="dst-user-<%=dstIndex%>"
							value="<%=properties.get("dst-user-" + dstIndex)%>" 
							title="Specify user name. User must have a privilege to peform DDL and DML operations e.g. CREATE/DROP/INSERT/UPDATE/DELETE"							
							<%
							if (properties.get("dst-user-" + dstIndex).equals("Not Applicable")) {
								out.print(" disabled");
							}
							%>/>
						</td>
					</tr>
					<tr>
						<td>Password</td>
						<td><input type="password" id="dst-password-<%=dstIndex%>" name="dst-password-<%=dstIndex%>"
							value="<%=properties.get("dst-password-" + dstIndex)%>" 
							title="Specify user password" 
							<%
							if (properties.get("dst-password-" + dstIndex).equals("Not Applicable")) {
								out.print(" disabled");
							}
							%>
							/></td>
					</tr>

					<tr>
						<td>Connection Timeout (s)</td>
						<td><input type="number" id="dst-connection-timeout-s-<%=dstIndex%>"
							name="dst-connection-timeout-s-<%=dstIndex%>"
							value="<%=properties.get("dst-connection-timeout-s-" + dstIndex)%>" 
							title="Specify database connection timeout in seconds."/></td>
					</tr>
					
					<tr>
						<% 
						if (properties.get("dst-type-" + dstIndex).equals("AZURE_EVENTHUB")) {
							out.println("<td>Azure Event Hub Name</td>");
						} else {
							out.println("<td>Database Name</td>");
						}
						%>
						<td><input type="text" id="dst-database-<%=dstIndex%>" name="dst-database-<%=dstIndex%>"
							value="<%=properties.get("dst-database-" + dstIndex)%>" 
							title="Specify the database/catalog name if the configured destination DB supports the concept of database/catalog."							
							<%
							if (properties.get("dst-database-" + dstIndex).equals("Not Applicable")) {
								out.print(" disabled");
							}
							%>
							/></td>
					</tr>
					<tr>
						<td>Schema Name</td>
						<td><input type="text" id="dst-schema-<%=dstIndex%>" name="dst-schema-<%=dstIndex%>"
							value="<%=properties.get("dst-schema-" + dstIndex)%>"
							title="Specify the schema name if the destination DB supports the concept of schema."
							<%
							if (properties.get("dst-schema-" + dstIndex).equals("Not Applicable")) {
								out.print(" disabled");
							}
							%>
							/></td>
					</tr>
					<%
						if (properties.get("dst-type-" + dstIndex).equals("DUCKDB")) {
							out.println("<tr>");
							out.println("<td>DuckDB Reader Port</td>");
							out.println("<td>");
							out.println("<input type=\"number\" id=\"dst-duckdb-reader-port-" + dstIndex + "\" " +
								"name=\"dst-duckdb-reader-port-" + dstIndex +  "\" " +
								"value=\"" + properties.get("dst-duckdb-reader-port-" + dstIndex) + "\" " + 
								"title=\"Specify port number to expose for enable querying data from destination duckdb.\"/>");
							out.println("</td>");
							out.println("</tr>");
						} 					%>
					<tr>
						<td>Destination DB Alias</td>
						<td><input type="text" id="dst-alias-<%=dstIndex%>" name="dst-alias-<%=dstIndex%>"
							value="<%=properties.get("dst-alias-" + dstIndex)%>"
							title="Specify an alias for this destination database. This alias is only used in Consolidator to identify individual destinations"/></td>
					</tr>					
				</tbody>				
			</table>
			<center>
				<button type="submit" name="next">Next</button>
			</center>			
		</form>
	</div>
</body>
</html>