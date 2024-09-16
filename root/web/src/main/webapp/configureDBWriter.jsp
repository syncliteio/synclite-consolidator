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
HashMap<String, String> properties = new HashMap<String, String>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

Integer dstIndex = 1;

if (request.getParameter("dstIndex") != null) {
	dstIndex = Integer.valueOf(request.getParameter("dstIndex"));
}
Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());

properties.put("dst-type-" + dstIndex, session.getAttribute("dst-type-" + dstIndex).toString());
properties.put("dst-alias-" + dstIndex, session.getAttribute("dst-alias-" + dstIndex).toString());
properties.put("src-app-type", session.getAttribute("src-app-type").toString());

if (request.getParameter("dst-type-" + dstIndex) != null) {
	//populate from request object
	properties.put("dst-device-schema-name-policy-" + dstIndex, request.getParameter("dst-device-schema-name-policy-" + dstIndex));
	properties.put("dst-insert-batch-size-" + dstIndex, request.getParameter("dst-insert-batch-size-" + dstIndex));
	properties.put("dst-update-batch-size-" + dstIndex, request.getParameter("dst-update-batch-size-" + dstIndex));
	properties.put("dst-delete-batch-size-" + dstIndex, request.getParameter("dst-delete-batch-size-" + dstIndex));
	properties.put("dst-object-init-mode-" + dstIndex, request.getParameter("dst-object-init-mode-" + dstIndex));
	properties.put("dst-txn-retry-count-" + dstIndex, request.getParameter("dst-txn-retry-count-" + dstIndex));
	properties.put("dst-txn-retry-interval-ms-" + dstIndex, request.getParameter("dst-txn-retry-interval-ms-" + dstIndex));
	properties.put("dst-idempotent-data-ingestion-" + dstIndex, request.getParameter("dst-idempotent-data-ingestion-" + dstIndex));
	properties.put("dst-idempotent-data-ingestion-method-" + dstIndex, request.getParameter("dst-idempotent-data-ingestion-method-" + dstIndex));
	properties.put("dst-skip-failed-log-files-" + dstIndex, request.getParameter("dst-skip-failed-log-files-" + dstIndex));
	properties.put("dst-set-unparsable-values-to-null-" + dstIndex, request.getParameter("dst-set-unparsable-values-to-null-" + dstIndex));
	properties.put("dst-quote-object-names-" + dstIndex, request.getParameter("dst-quote-object-names-" + dstIndex));
	properties.put("dst-quote-column-names-" + dstIndex, request.getParameter("dst-quote-column-names-" + dstIndex));
	properties.put("dst-use-catalog-scope-resolution-" + dstIndex, request.getParameter("dst-use-catalog-scope-resolution-" + dstIndex));
	properties.put("dst-use-schema-scope-resolution-" + dstIndex, request.getParameter("dst-use-schema-scope-resolution-" + dstIndex));
	properties.put("dst-create-table-suffix-" + dstIndex, request.getParameter("dst-create-table-suffix-" + dstIndex));

	if (request.getParameter("dst-clickhouse-engine-" + dstIndex) != null) {
		properties.put("dst-clickhouse-engine-" + dstIndex, request.getParameter("dst-clickhouse-engine-" + dstIndex));
	}
	if (request.getParameter("dst-mongodb-use-transactions-" + dstIndex) != null) {
		properties.put("dst-mongodb-use-transactions-" + dstIndex, request.getParameter("dst-mongodb-use-transactions-" + dstIndex));
	}
	
	if (request.getParameter("dst-csv-files-with-headers-" + dstIndex) != null) {
		properties.put("dst-csv-files-with-headers-" + dstIndex, request.getParameter("dst-csv-files-with-headers-" + dstIndex));
	}

	if (request.getParameter("dst-csv-files-field-delimiter-" + dstIndex) != null) {
		properties.put("dst-csv-files-field-delimiter-" + dstIndex, request.getParameter("dst-csv-files-field-delimiter-" + dstIndex));
	}

	if (request.getParameter("dst-csv-files-record-delimiter-" + dstIndex) != null) {
		properties.put("dst-csv-files-record-delimiter-" + dstIndex, request.getParameter("dst-csv-files-record-delimiter-" + dstIndex));
	}

	if (request.getParameter("dst-csv-files-escape-character-" + dstIndex) != null) {
		properties.put("dst-csv-files-escape-character-" + dstIndex, request.getParameter("dst-csv-files-escape-character-" + dstIndex));
	}

	if (request.getParameter("dst-csv-files-quote-character-" + dstIndex) != null) {
		properties.put("dst-csv-files-quote-character-" + dstIndex, request.getParameter("dst-csv-files-quote-character-" + dstIndex));
	}

	if (request.getParameter("dst-csv-files-null-string-" + dstIndex) != null) {
		properties.put("dst-csv-files-null-string-" + dstIndex, request.getParameter("dst-csv-files-null-string-" + dstIndex));
	}

} else {
	//Popuate default values in the map	

	switch(properties.get("dst-type-" + dstIndex)) {	
	case "CSV":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "10");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;

	case "SQLITE":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;

	case "DUCKDB":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;

	case "FERRETDB":	
	case "MONGODB":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "10");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;
		
	case "MYSQL":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;

	case "POSTGRESQL":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;

	case "CLICKHOUSE":
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;
	
	default:
		properties.put("dst-insert-batch-size-" + dstIndex, "100000");
		properties.put("dst-update-batch-size-" + dstIndex, "100000");
		properties.put("dst-delete-batch-size-" + dstIndex, "100000");
		properties.put("dst-txn-retry-count-" + dstIndex, "100");
		properties.put("dst-txn-retry-interval-ms-" + dstIndex, "10000");
		break;
	}
	
	properties.put("dst-skip-failed-log-files-" + dstIndex, "false");
	properties.put("dst-set-unparsable-values-to-null-" + dstIndex, "false");
	properties.put("dst-quote-object-names-" + dstIndex, "false");
	properties.put("dst-quote-column-names-" + dstIndex, "false");
	properties.put("dst-use-catalog-scope-resolution-" + dstIndex, "true");
	properties.put("dst-use-schema-scope-resolution-" + dstIndex, "true");
	
	if(properties.get("src-app-type").equals("SYNCLITE-DBREADER")) {
		properties.put("dst-idempotent-data-ingestion-" + dstIndex, "true");
		properties.put("dst-device-schema-name-policy-" + dstIndex, "SPECIFIED_DST_CATALOG_SCHEMA");		
	} else {
		properties.put("dst-idempotent-data-ingestion-" + dstIndex, "false");
		properties.put("dst-device-schema-name-policy-" + dstIndex, "SYNCLITE_DEVICE_ID_AND_NAME");
	}
	
	//ClickHouse does not support idempotent dta ingestion.
	if (properties.get("dst-type-" + dstIndex).equals("CLICKHOUSE")) {
		properties.put("dst-idempotent-data-ingestion-" + dstIndex, "false");	
	}

	properties.put("dst-idempotent-data-ingestion-method-" + dstIndex, "NATIVE_UPSERT");

	//For some systems always set idempotent data ingestion to true by default as it does not support transactions
	if (properties.get("dst-type-" + dstIndex).equals("MONGODB") || properties.get("dst-type-" + dstIndex).equals("COSMOSDB_MONGODB") || (properties.get("dst-type-" + dstIndex).equals("FERRETDB")) || (properties.get("dst-type-" + dstIndex).equals("APACHE_ICEBERG"))) {
		properties.put("dst-idempotent-data-ingestion-" + dstIndex, "true");
	}
	
		
	properties.put("dst-object-init-mode-" + dstIndex, "TRY_CREATE_APPEND_DATA");
	
	properties.put("dst-create-table-suffix-" + dstIndex, "");

	properties.put("dst-clickhouse-engine-" + dstIndex, "ReplacingMergeTree");

	properties.put("dst-mongodb-use-transactions-" + dstIndex, "false");

	properties.put("dst-csv-files-with-headers-" + dstIndex, "true");
	properties.put("dst-csv-files-field-delimiter-" + dstIndex, ",");
	properties.put("dst-csv-files-record-delimiter-" + dstIndex, "\\r\\n");
	properties.put("dst-csv-files-escape-character-" + dstIndex, "\"");
	properties.put("dst-csv-files-quote-character-" + dstIndex, "\"");
	properties.put("dst-csv-files-null-string-" + dstIndex, "null");

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
}

%>

<script type="text/javascript">
</script>

<title>Configure Database Writer For Destination DB <%=dstIndex%></title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<%
		if (numDestinations == 1) {
			out.println("<h2>Configure Destination DB Writer</h2>");
		} else {
			out.println("<h2>Configure Destination DB Writer (Destination DB " + dstIndex + " : " + properties.get("dst-alias-" + dstIndex) + ")</h2>");
		}
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDBWriterConfiguration"
			method="post">
			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
			<table>
				<tbody>
					<tr>
						<td>DB Type</td>
						<td><select id="dst-type-<%=dstIndex%>" name="dst-type-<%=dstIndex%>" disabled>
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
								if (properties.get("dst-type-" + dstIndex).equals("CSV")) {
									out.println("<option value=\"CSV\" selected>CSV Files</option>");
								} else {
									out.println("<option value=\"CSV\">CSV Files</option>");
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
					if (session.getAttribute("dst-sync-mode").toString().equals("REPLICATION")) {
						out.println("<tr>");
						out.println("<td>Replicate Device Tables to Destination Schema with</td>");
						out.println("<td>");
						out.println("<select id=\"dst-device-schema-name-policy-" + dstIndex + "\" name=\"dst-device-schema-name-policy-" + dstIndex  + "\" title=\"Specify table name suffix policy. Given the fact that multiple devices may be replicating same table, it is recommended to choose <device name>/<device id> as a schema to hold each table coming from respective devices\">");
						if (properties.get("dst-device-schema-name-policy-" + dstIndex).equals("SYNCLITE_DEVICE_ID")) {
							out.println("<option value=\"SYNCLITE_DEVICE_ID\" selected>SyncLite Device ID as schema name</option>");
						} else {
							out.println("<option value=\"SYNCLITE_DEVICE_ID\">SyncLite Device ID as schema name</option>");
						}
						if (properties.get("dst-device-schema-name-policy-" + dstIndex).equals("SYNCLITE_DEVICE_NAME")) {
							out.println("<option value=\"SYNCLITE_DEVICE_NAME\" selected>SyncLite Device name as schema name</option>");
						} else {
							out.println("<option value=\"SYNCLITE_DEVICE_NAME\">SyncLite Device Name as schema name</option>");
						}
						if (properties.get("dst-device-schema-name-policy-" + dstIndex).equals("SYNCLITE_DEVICE_ID_AND_NAME")) {
							out.println("<option value=\"SYNCLITE_DEVICE_ID_AND_NAME\" selected>SyncLite Device ID + Name as schema name</option>");
						} else {
							out.println("<option value=\"SYNCLITE_DEVICE_ID_AND_NAME\">SyncLite Device ID + Name schema name</option>");
						}
						if (properties.get("dst-device-schema-name-policy-" + dstIndex).equals("SPECIFIED_DST_CATALOG_SCHEMA")) {
							out.println("<option value=\"SPECIFIED_DST_CATALOG_SCHEMA\" selected>Specified catalog/schema</option>");
						} else {
							out.println("<option value=\"SPECIFIED_DST_CATALOG_SCHEMA\">Specified catalog/schema</option>");
						}
						
						out.println("</td>");
						out.println("</tr>");
					}
					%>
					
					<% 
						if(properties.get("dst-type-" + dstIndex).toString().equals("CSV")) {
							out.println("<tr><td>Add CSV File Headers</td>");
							out.println("<td><select id=\"dst-csv-files-with-headers-" + dstIndex + "\" name=\"dst-csv-files-with-headers-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-with-headers-" + dstIndex) + "\" title=\"Specify if header should be added to destination CSV files\">");
							if (properties.get("dst-csv-files-with-headers-" + dstIndex).equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("dst-csv-files-with-headers-" + dstIndex).equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select></td></tr>");

							out.println("<tr><td>CSV File Field Delimiter</td>");
							out.println("<td><input type=\"text\" name=\"dst-csv-files-field-delimiter-" + dstIndex +  "\" id=\"dst-csv-files-field-delimiter-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-field-delimiter-" + dstIndex) + "\" title=\"Specify field delimiter to use in destination CSV files\"></td></tr>");

							out.println("<tr><td>CSV File Record Delimiter</td>");
							out.println("<td><input type=\"text\" name=\"dst-csv-files-record-delimiter-" + dstIndex + "\" id=\"dst-csv-files-record-delimiter-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-record-delimiter-" + dstIndex) + "\" title=\"Specify record delimiter to use use in destination CSV files\"></td></tr>");

							out.println("<tr><td>CSV File Escape Character</td>");
							out.println("<td><input type=\"text\" name=\"dst-csv-files-escape-character-" + dstIndex + "\" id=\"dst-csv-files-escape-character-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-escape-character-" + dstIndex).toString().replace("\"", "&quot;") + "\" title=\"Specify escape character to use in destination CSV files\"></td></tr>");

							out.println("<tr><td>CSV File Quote Character</td>");
							out.println("<td><input type=\"text\" name=\"dst-csv-files-quote-character-" + dstIndex + "\" id=\"dst-csv-files-quote-character-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-quote-character-" + dstIndex).toString().replace("\"", "&quot;") + "\" title=\"Specify quote character to use in destination CSV files\"></td></tr>");

							out.println("<tr><td>CSV File Null String</td>");
							out.println("<td><input type=\"text\" name=\"dst-csv-files-null-string-" + dstIndex + "\" id=\"dst-csv-files-null-string-" + dstIndex + "\" value=\"" + properties.get("dst-csv-files-null-string-" + dstIndex) + "\" title=\"Specify null string to use in destination CSV files\"></td></tr>");
						}
					%>	

					<tr>
						<td>Destination Object Initialization Mode</td>
						<td><select id="dst-object-init-mode-<%=dstIndex%>" name="dst-object-init-mode-<%=dstIndex%>">
								<%
								if (properties.get("dst-object-init-mode-" + dstIndex).equals("TRY_CREATE_APPEND_DATA")) {
									out.println("<option value=\"TRY_CREATE_APPEND_DATA\" selected>Create objects if not exist and append data to existing objects</option>");
								} else {
									out.println("<option value=\"TRY_CREATE_APPEND_DATA\">Create objects if not exist and append data to existing objects</option>");
								}
								if (properties.get("dst-object-init-mode-" + dstIndex).equals("APPEND_DATA")) {
									out.println("<option value=\"APPEND_DATA\" selected>Append data to existing objects</option>");
								} else {
									out.println("<option value=\"APPEND_DATA\">Append data to existing objects</option>");
								}
								if (properties.get("dst-object-init-mode-" + dstIndex).equals("TRY_CREATE_DELETE_DATA")) {
									out.println("<option value=\"TRY_CREATE_DELETE_DATA\" selected>Create objects if not exist and delete data from existing objects</option>");
								} else {
									out.println("<option value=\"TRY_CREATE_DELETE_DATA\">Create objects if not exist and delete data from existing objects</option>");
								}
								if (properties.get("dst-object-init-mode-" + dstIndex).equals("DELETE_DATA")) {
									out.println("<option value=\"DELETE_DATA\" selected>Delete data from existing objects</option>");
								} else {
									out.println("<option value=\"DELETE_DATA\">Delete data from existing objects</option>");
								}
								if (properties.get("dst-object-init-mode-" + dstIndex).equals("OVERWRITE_OBJECT")) {
									out.println("<option value=\"OVERWRITE_OBJECT\" selected>Drop and recreate existing objects</option>");
								} else {
									out.println("<option value=\"OVERWRITE_OBJECT\">Drop and recreate existing objects</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Create Table Suffix</td>
						<td><input type="text"  id="dst-create-table-suffix-<%=dstIndex%>"
							name="dst-create-table-suffix-<%=dstIndex%>"
							value="<%=properties.get("dst-create-table-suffix-" + dstIndex)%>"
							title="Specify a suffix to create table statements if needed"/>
						</td>				
					</tr>

					<tr>
						<td>Insert/Upsert/Replace Batch Size (Operations)</td>
						<td><input type="number" id="dst-insert-batch-size-<%=dstIndex%>"
							name="dst-insert-batch-size-<%=dstIndex%>"
							value="<%=properties.get("dst-insert-batch-size-" + dstIndex)%>" 
							title="Specify a batch size for INSERT operations which works the best for selected destination type and your SQL workload."/></td>
					</tr>

					<tr>
						<td>Update Batch Size (Operations)</td>
						<td><input type="number" id="dst-update-batch-size-<%=dstIndex%>"
							name="dst-update-batch-size-<%=dstIndex%>"
							value="<%=properties.get("dst-update-batch-size-" + dstIndex)%>" 
							title="Specify a batch size for UPDATE operations which works the best for selected destination type and your SQL workload."/></td>
					</tr>

					<tr>
						<td>Delete Batch Size (Operations)</td>
						<td><input type="number" id="dst-delete-batch-size-<%=dstIndex%>"
							name="dst-delete-batch-size-<%=dstIndex%>"
							value="<%=properties.get("dst-delete-batch-size-" + dstIndex)%>" 
							title="Specify a batch size for DELETE operations which works the best for selected destination type and your SQL workload."/></td>
					</tr>

					<tr>
						<td>Failed Transaction Retry Count</td>
						<td><input type="number" id="dst-txn-retry-count-<%=dstIndex%>"
							name="dst-txn-retry-count-<%=dstIndex%>"
							value="<%=properties.get("dst-txn-retry-count-" + dstIndex)%>" 
							title="Specify number of retry attempts for each failed transaction on the destination database. After exhausting the retries, a device is marked as failed and retried again after 'Failed Device Retry Interval'"/></td>
					</tr>

					<tr>
						<td>Failed Transaction Retry Interval (ms)</td>
						<td><input type="number" id="dst-txn-retry-interval-ms-<%=dstIndex%>"
							name="dst-txn-retry-interval-ms-<%=dstIndex%>"
							value=<%=properties.get("dst-txn-retry-interval-ms-" + dstIndex)%> " 
							title="Specify a minimum backoff interval in milliseconds after which a failed transaction will be retried on destination database."/></td>
					</tr>

					<tr>
						<td>Enable Idempotent Data Ingestion</td>
						<td><select id="dst-idempotent-data-ingestion-<%=dstIndex%>"
							name="dst-idempotent-data-ingestion-<%=dstIndex%>" title="Specify if each incoming INSERT operation should be applied idempotently as a combination of DELETE followed by an INSERT ( or UPSERT if available on the destination) to ensure idempotency. Please note that this option will take effort only if the given table has a primary key. Enable this option with caution as it may impact data ingestion performance.">
								<%
								if (! properties.get("dst-type-" + dstIndex).equals("CLICKHOUSE")) {
									if (properties.get("dst-idempotent-data-ingestion-" + dstIndex).equals("true")) {
										out.println("<option value=\"true\" selected>true</option>");
									} else {
										out.println("<option value=\"true\">true</option>");
									}									
								}
								if (properties.get("dst-idempotent-data-ingestion-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Idempotent Data Ingestion Method</td>
						<td><select id="dst-idempotent-data-ingestion-method-<%=dstIndex%>"
							name="dst-idempotent-data-ingestion-method-<%=dstIndex%>" title="Specify data ingestion method as either NATIVE_UPSERT or DELETE_INSERT. Please note that NATIVE_UPSERT or NATIVE_REPLACE is used only if the destination database supports UPSERT or REPLACE mechanism respectively else DELETE_INSERT method is used.">
								<%
									if (properties.get("dst-idempotent-data-ingestion-method-" + dstIndex).equals("DELETE_INSERT")) {
										out.println("<option value=\"DELETE_INSERT\" selected>Delete Insert</option>");
									} else {
										out.println("<option value=\"DELETE_INSERT\">Delete Insert</option>");
									}

									if (properties.get("dst-idempotent-data-ingestion-method-" + dstIndex).equals("NATIVE_REPLACE")) {
										out.println("<option value=\"NATIVE_REPLACE\" selected>Native Replace</option>");
									} else {
										out.println("<option value=\"NATIVE_REPLACE\">Native Replace</option>");
									}									
	
									if (properties.get("dst-idempotent-data-ingestion-method-" + dstIndex).equals("NATIVE_UPSERT")) {
										out.println("<option value=\"NATIVE_UPSERT\" selected>Native Upsert</option>");
									} else {
										out.println("<option value=\"NATIVE_UPSERT\">Native Upsert</option>");
									}									
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Set Unparsable Values To NULL</td>
						<td><select id="dst-set-unparsable-values-to-null-<%=dstIndex%>"
							name="dst-set-unparsable-values-to-null-<%=dstIndex%>" title="Specify if consolidator should set incoming unparsable values to null on destination db.">
								<%
								if (properties.get("dst-set-unparsable-values-to-null-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-set-unparsable-values-to-null-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Quote Object Names</td>
						<td><select id="dst-quote-object-names-<%=dstIndex%>"
							name="dst-quote-object-names-<%=dstIndex%>" title="Specify if consolidator should add quotations around object names in all SQL statements executed on destination DB.">
								<%
								if (properties.get("dst-quote-object-names-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-quote-object-names-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Quote Column Names</td>
						<td><select id="dst-quote-column-names-<%=dstIndex%>"
							name="dst-quote-column-names-<%=dstIndex%>" title="Specify if consolidator should add quotations around column names in all SQL statements executed on destination DB.">
								<%
								if (properties.get("dst-quote-column-names-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-quote-column-names-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Use Catalog Scope Resolution</td>
						<td><select id="dst-use-catalog-scope-resolution-<%=dstIndex%>"
							name="dst-use-catalog-scope-resolution-<%=dstIndex%>" title="Specify if consolidator should add catalog name prefix while framing SQL statemewnts for each destination table">
								<%
								if (properties.get("dst-use-catalog-scope-resolution-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-use-catalog-scope-resolution-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Use Schema Scope Resolution</td>
						<td><select id="dst-use-schema-scope-resolution-<%=dstIndex%>"
							name="dst-use-schema-scope-resolution-<%=dstIndex%>" title="Specify if consolidator should add schema name prefix while framing SQL statements for each destination table">
								<%
								if (properties.get("dst-use-schema-scope-resolution-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-use-schema-scope-resolution-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Skip Failed Log Files</td>
						<td><select id="dst-skip-failed-log-files-<%=dstIndex%>"
							name="dst-skip-failed-log-files-<%=dstIndex%>" title="Specify if a failed log segment should be skipped after all retry attempts. Enable this option with caution as it impacts correctness of consolidated data. Note that each failed log segment is preserved in failed_logs sub-directory under the work directory.">
								<%
								if (properties.get("dst-skip-failed-log-files-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-skip-failed-log-files-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<%
						if (properties.get("dst-type-" + dstIndex).equals("CLICKHOUSE")) {
							out.println("<tr>");
							out.println("<td>ClickHouse Engine</td>");
							out.println("<td><input type=\"text\" id=\"dst-clickhouse-engine-" + dstIndex + "\" name=\"dst-clickhouse-engine-" + dstIndex + "\" value=\"" + properties.get("dst-clickhouse-engine-" + dstIndex) + "\" title=\"Specify ClickHouse Engine to use for destination tables. Default is set to ReplacingMergeTree to facililate enevtual record upserting behaviour as required for incremental replication\"/></td>");
							out.println("</tr>");
						}
	
						if (properties.get("dst-type-" + dstIndex).equals("MONGODB") || properties.get("dst-type-" + dstIndex).equals("COSMOSDB_MONGODB") || properties.get("dst-type-" + dstIndex).equals("FERRETDB")) {
							out.println("<tr>");
							out.println("<td>Use MongoDB Transactions</td>");
							out.println("<td>");
							out.println("<select id=\"dst-mongodb-use-transactions-\"" + dstIndex + "\" name=\"dst-mongodb-use-transactions-\""  + dstIndex + "\" title=\"Specify if consolidator should peform all MongoDB writes under trasnactions. Set this to true only if the destination MongoDB version supports and is enabled to process transactions.\">");
							if (properties.get("dst-mongodb-use-transactions-" + dstIndex).equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("dst-mongodb-use-transactions-" + dstIndex).equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select>");
							out.println("</td>");
							out.println("</tr>");
						}
					%>
				</tbody>				
			</table>
			<center>
				<button type="submit" name="next">Next</button>
			</center>			
		</form>
	</div>
</body>
</html>
