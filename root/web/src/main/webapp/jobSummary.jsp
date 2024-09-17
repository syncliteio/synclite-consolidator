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

<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<script type="text/javascript">
</script>
<title>SyncLite Consolidator Job Summary</title>
</head>
<%

if (session.getAttribute("device-data-root") == null) {
	response.sendRedirect("syncLiteTerms.jsp");
}

String saveStatus = request.getParameter("saveStatus");
String saveStatusDetails = request.getParameter("saveStatusDetails");
%>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Consolidator Job Summary</h2>
		<%
		if (saveStatus != null) {
			if (saveStatus.equals("SUCCESS")) {
				out.println("<h4 style=\"color: blue;\"> Job started successfully</h4>");
			} else if (saveStatus.equals("FAIL")) {
				out.println("<h4 style=\"color: red;\"> Job configuration and statup failed with error : "
				+ saveStatusDetails.replace("<", "&lt;").replace(">", "&gt;") + "</h4>");
			}
		}
		%>

		<form action="${pageContext.request.contextPath}/saveJobConfiguration"
			method="post">

			<table>
				<tr></tr>
				<tr>
					<th>Job Configuration</th>
				</tr>
				<tr>
					<td>
						<b>Consolidator Work Directory</b>      : <% out.print(session.getAttribute("device-data-root").toString()); %><br>
						<b>Source Application Type</b> 			: <% out.print(session.getAttribute("src-app-type").toString()); %><br>
						<b>Sync Mode</b>                        : <% out.print(session.getAttribute("dst-sync-mode").toString()); %><br>
						<b>Device Processors</b>                : <% out.print(session.getAttribute("num-device-processors").toString()); %><br>
						<b>Allowed Device Name Pattern</b>      : <% out.print(session.getAttribute("device-name-pattern").toString()); %><br>
						<b>Allowed Device ID Pattern</b>        : <% out.print(session.getAttribute("device-id-pattern").toString()); %><br>
						<b>Enable Replicas For Streaming/Telemetry Devices</b>   : <% out.print(session.getAttribute("enable-replicas-for-telemetry-devices").toString()); %><br>
					    <b>Disable Replicas For Appender Devices</b>   : <% out.print(session.getAttribute("disable-replicas-for-appender-devices").toString()); %><br>						
					    <b>Skip Missing/Corrupt Transaction Files</b>   : <% out.print(session.getAttribute("skip-bad-txn-files").toString()); %><br>						
						<b>Failed Device Retry Interval (s)</b> : <% out.print(session.getAttribute("failed-device-retry-interval-s")); %><br>
						<b>Job Trace Level</b>                  : <% out.print(session.getAttribute("device-trace-level").toString()); %><br>
						<b>Enable Online Request Processor</b>  : <% out.print(session.getAttribute("enable-request-processor").toString()); %><br>
						<% 						
							if (request.getSession().getAttribute("request-processor-port") != null) {
								out.print("<b>Online Request Processor Port</b> : "  + session.getAttribute("request-processor-port").toString() + "<br>");
							}
						%>
						<b>Enable Device Command Handler</b>  : <% out.print(session.getAttribute("enable-device-command-handler").toString()); %><br>
						<% 						
							if (request.getSession().getAttribute("enable-device-command-handler").toString().equals("true")) {
								out.print("<b>Device Command Handler Timeout (s)</b> : "  + session.getAttribute("device-command-timeout-s").toString() + "<br>");
							}
							if (request.getSession().getAttribute("jvm-arguments") != null) {
								out.print("<b>JVM Arguments</b> : "  + session.getAttribute("jvm-arguments").toString() + "<br>");
							}
						%>
					</td>
				</tr>
			</table>
			<table>
				<tr></tr>
				<tr>
					<th>Job Monitor Configuration</th>
				</tr>
				<tr>
					<td>
						<b>Update Statistics Interval (S)</b>  		: <% out.print(session.getAttribute("update-statistics-interval-s").toString()); %><br>
						<b>Enable Prometheus Statistics Publisher</b>   : <% out.print(session.getAttribute("enable-prometheus-statistics-publisher").toString()); %><br>
						<%
						if (session.getAttribute("enable-prometheus-statistics-publisher").toString().equals("true")) {
							out.print("<b>Prometheus Push Gateway URL</b> : " + session.getAttribute("prometheus-push-gateway-url").toString() + "<br>");
							out.print("<b>Prometheus Statistics Publisher Interval (S)</b> : " + session.getAttribute("prometheus-statistics-publisher-interval-s").toString() + "<br>");
						}
						%>
					</td>
				</tr>
			</table>
			<table>
				<tr></tr>
				<tr>
					<th>Device Stage Configuration</th>
				</tr>
				<tr>
					<td>
						<b>Device Stage Type</b>      			: <% out.print(session.getAttribute("device-stage-type").toString()); %><br>
						<%
							if (session.getAttribute("device-stage-type").toString().equals("FS")) {
								out.print("<b>Device Data Stage Directory</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
								if (session.getAttribute("device-command-root") != null) {
									out.print("<b>Device Command Stage Directory</b> : " + session.getAttribute("device-command-root").toString() + "<br>");
								}
							} else if (session.getAttribute("device-stage-type").toString().equals("LOCAL_SFTP")) {
								out.print("<b>SFTP Device Data Stage Directory</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
								if (session.getAttribute("device-command-root") != null) {
									out.print("<b>SFTP Device Command Stage Directory</b> : " + session.getAttribute("device-command-root").toString() + "<br>");
								}								
								out.print("<b>Device Stage Directory</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
							} else if (session.getAttribute("device-stage-type").toString().equals("REMOTE_SFTP")) {
								out.print("<b>Remote SFTP Server Host</b> : " + session.getAttribute("stage-sftp-host").toString() + "<br>");
								out.print("<b>SFTP Server Port</b> : " + session.getAttribute("stage-sftp-port").toString() + "<br>");
								out.print("<b>SFTP User</b> : " + session.getAttribute("stage-sftp-user").toString() + "<br>");
								out.print("<b>SFTP User Password</b> : " + session.getAttribute("stage-sftp-password").toString() + "<br>");
								out.print("<b>SFTP Device Data Stage Directory</b> : " + session.getAttribute("stage-sftp-data-directory").toString() + "<br>");
								if (session.getAttribute("stage-sftp-command-directory") != null) {
									out.print("<b>SFTP Device Command Stage Directory</b> : " + session.getAttribute("stage-sftp-command-directory").toString() + "<br>");
								}
							} else if (session.getAttribute("device-stage-type").toString().equals("MS_ONEDRIVE")) {
								out.print("<b>Microsoft OneDrive Device Data Stage Directory</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
								if (session.getAttribute("device-command-root") != null) {
									out.print("<b>Microsoft OneDrive Device Command Stage Directory</b> : " + session.getAttribute("device-command-root").toString() + "<br>");
								}
							} else if (session.getAttribute("device-stage-type").toString().equals("GOOGLE_DRIVE")) {
								out.print("<b>Google Drive Device Data Stage Diretory</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
								if (session.getAttribute("device-command-root") != null) {
									out.print("<b>Google Drive Command Stage Directory</b> : " + session.getAttribute("device-command-root").toString() + "<br>");
								}								
							} else if (session.getAttribute("device-stage-type").toString().equals("LOCAL_MINIO")) {
								out.print("<b>MinIO Device Data Stage Bucket Storage Path</b> : " + session.getAttribute("device-upload-root").toString() + "<br>");
								out.print("<b>MinIO Endpoint</b> : " + session.getAttribute("minio-endpoint").toString() + "<br>");
								out.print("<b>MinIO Device Data Stage Bucket Name</b> : " + session.getAttribute("stage-minio-data-bucket-name").toString() + "<br>");
								if (session.getAttribute("stage-minio-command-bucket-name") != null) {
									out.print("<b>MinIO Device Command Stage Bucket Name</b> : " + session.getAttribute("stage-minio-command-bucket-name").toString() + "<br>");
								}							
								out.print("<b>MinIO Access Key</b> : " + session.getAttribute("minio-access-key").toString() + "<br>");
								out.print("<b>MinIO Secret Key</b> : " + session.getAttribute("minio-secret-key").toString() + "<br>");
							} else if (session.getAttribute("device-stage-type").toString().equals("REMOTE_MINIO")) {
								out.print("<b>MinIO Endpoint</b> : " + session.getAttribute("minio-endpoint").toString() + "<br>");
								out.print("<b>MinIO Device Data Stage Bucket Name</b> : " + session.getAttribute("stage-minio-data-bucket-name").toString() + "<br>");
								if (session.getAttribute("stage-minio-command-bucket-name") != null) {
									out.print("<b>MinIO Device Command Stage Bucket Name</b> : " + session.getAttribute("stage-minio-command-bucket-name").toString() + "<br>");
								}
								out.print("<b>MinIO Access Key</b> : " + session.getAttribute("minio-access-key").toString() + "<br>");
								out.print("<b>MinIO Secret Key</b> : " + session.getAttribute("minio-secret-key").toString() + "<br>");
								out.print("<b>New Device Scan Interval (s)</b> : " + session.getAttribute("device-scanner-interval-s").toString() + "<br>");
							} else if (session.getAttribute("device-stage-type").toString().equals("S3")) {
								out.print("<b>S3 Endpoint</b> : " + session.getAttribute("s3-endpoint").toString() + "<br>");
								out.print("<b>S3 Device Data Stage Bucket Name</b> : " + session.getAttribute("stage-s3-data-bucket-name").toString() + "<br>");
								if (session.getAttribute("stage-s3-command-bucket-name") != null) {
									out.print("<b>S3 Device Command Stage Bucket Name</b> : " + session.getAttribute("stage-s3-command-bucket-name").toString() + "<br>");
								}	
								out.print("<b>S3 Access Key</b> : " + session.getAttribute("s3-access-key").toString() + "<br>");
								out.print("<b>S3 Secret Key</b> : " + session.getAttribute("s3-secret-key").toString() + "<br>");
								if (session.getAttribute("throttle-stage-request-rate") != null) {
									out.print("<b>Throttle Request Rate</b> : " + session.getAttribute("throttle-stage-request-rate").toString() + "<br>");
								}
								if (session.getAttribute("max-stage-requests-per-minute") != null) {
									out.print("<b>Maximum Requests Per Minute</b> : " + session.getAttribute("max-stage-requests-per-minute").toString() + "<br>");
								}
								out.print("<b>New Device Scan Interval (s)</b> : " + session.getAttribute("device-scanner-interval-s").toString() + "<br>");							
							} else if (session.getAttribute("device-stage-type").toString().equals("KAFKA")) {
								if (session.getAttribute("stage-kafka-consumer-properties-file") != null) {
									out.print("<b>Stage Kafka Consumer Properties File</b> : " + session.getAttribute("stage-kafka-consumer-properties-file").toString() + "<br>");
								}
								if (session.getAttribute("stage-kafka-producer-properties-file") != null) {
									out.print("<b>Stage Kafka Producer Properties File</b> : " + session.getAttribute("stage-kafka-producer-properties-file").toString() + "<br>");
								}
							}
						%>
						<b>Device Encryption Enabled</b>          	 : <% out.print(session.getAttribute("device-encryption-enabled").toString()); %><br>
						<% 
						if (session.getAttribute("device-decryption-key-file") != null) {
							out.print("<b>Device Decryption Key File</b> : " + session.getAttribute("device-decryption-key-file").toString() + "<br>");
						}	
						%>
						<b>Device Scheduler Type</b>          : <% out.print(session.getAttribute("device-scheduler-type").toString()); %><br>
						<b>Device Polling Interval (ms)</b>          : <% out.print(session.getAttribute("device-polling-interval-ms").toString()); %><br>
						<b>Failed Operation Retry Count</b>          : <% out.print(session.getAttribute("stage-oper-retry-count").toString()); %><br>
						<b>Failed Operation Retry Interval (ms)</b>  : <% out.print(session.getAttribute("stage-oper-retry-interval-ms").toString()); %><br>						
					</td>
				</tr>
			</table>
			<% 
				Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());
				for (Integer dstIndex = 1; dstIndex <= numDestinations ; ++dstIndex) {
					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Configuration");
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					out.print("<b>Database Type</b> : " + session.getAttribute("dst-type-name-" + dstIndex).toString() + "<br>");
					if (session.getAttribute("dst-connection-string-" + dstIndex) != null) {
						out.print("<b>JDBC Connection URL</b> : " + session.getAttribute("dst-connection-string-" + dstIndex).toString() +"<br>");
					}
					if (session.getAttribute("dst-database-" + dstIndex) != null) {
						out.print("<b>Database Name</b> : " + session.getAttribute("dst-database-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-schema-" + dstIndex) != null) {
						out.print("<b>Schema Name</b> : " + session.getAttribute("dst-schema-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-user-" + dstIndex) != null) {
						out.print("<b>Database User</b> : " + session.getAttribute("dst-user-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-password-" + dstIndex) != null) {
						out.print("<b>Database User Password</b> : " + "*****" + "<br>");
					}
					if (session.getAttribute("dst-connection-timeout-s-" + dstIndex) != null) {
						out.print("<b>Database Connection Timeout (s)</b> : " + session.getAttribute("dst-connection-timeout-s-" + dstIndex).toString() + "<br>");
					}			
					
			    	if (session.getAttribute("dst-type-" + dstIndex).toString().equals("DATALAKE")) {
			    		out.println("<b>Data Lake Data Format </b> : ");
			    		out.print(session.getAttribute("dst-data-lake-data-format-" + dstIndex).toString() + "<br>");			    		
			    		out.println("<b>Data Lake Object Switch Interval </b> : ");
			    		out.print(session.getAttribute("dst-data-lake-object-switch-interval-" + dstIndex).toString());
			    		out.print(" " + session.getAttribute("dst-data-lake-object-switch-interval-unit-" + dstIndex) + "<br>");
			    		out.println("<b>Data Lake Local Storage Directory</b> : ");
			    		out.print(" " + session.getAttribute("dst-data-lake-local-storage-dir-" + dstIndex) + "<br>");
			    		out.println("<b>Publish Data Lake</b> : ");
			    		out.print(" " + session.getAttribute("dst-data-lake-publishing-" + dstIndex) + "<br>");
						if (session.getAttribute("dst-data-lake-publishing-" + dstIndex).toString().equals("true")) {
				    		out.println("<b>Publish Data Lake To</b> : ");
				    		out.print(" " + session.getAttribute("dst-data-lake-type-" + dstIndex) + "<br>");
				    		if (session.getAttribute("dst-data-lake-type-" + dstIndex).toString().equals("S3")) {
					    		out.println("<b>Data Lake S3 Endpoint</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-s3-endpoint-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake S3 Bucket Name</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-s3-bucket-name-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake S3 Access Key</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-s3-access-key-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake S3 Secret Key</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-s3-secret-key-" + dstIndex) + "<br>");
				    		} else if (session.getAttribute("dst-data-lake-type-" + dstIndex).toString().equals("MINIO")) {
					    		out.println("<b>Data Lake MinIO Endpoint</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-minio-endpoint-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake MinIO Bucket Name</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-minio-bucket-name-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake MinIO Access Key</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-minio-access-key-" + dstIndex) + "<br>");
					    		out.println("<b>Data Lake MinIO Secret Key</b> : ");
					    		out.print(" " + session.getAttribute("dst-data-lake-minio-secret-key-" + dstIndex) + "<br>");				    			
				    		}
						}			    		
			    	} else if (session.getAttribute("dst-type-" + dstIndex).toString().equals("CSV")) {
						if (session.getAttribute("dst-file-storage-type-" + dstIndex) != null) {
							out.print("<b>File Storage Type</b> : " + session.getAttribute("dst-file-storage-type-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-sftp-host-" + dstIndex) != null) {
							out.print("<b>Destination SFTP Host</b> : " + session.getAttribute("dst-file-storage-sftp-host-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-sftp-port-" + dstIndex) != null) {
							out.print("<b>Destination SFTP Port</b> : " + session.getAttribute("dst-file-storage-sftp-port-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-sftp-directory-" + dstIndex) != null) {
							out.print("<b>Destination SFTP Directory</b> : " + session.getAttribute("dst-file-storage-sftp-directory-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-sftp-user-" + dstIndex) != null) {
							out.print("<b>Destination SFTP User</b> : " + session.getAttribute("dst-file-storage-sftp-user-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-sftp-password-" + dstIndex) != null) {
							out.print("<b>Destination SFTP Password</b> : " + "*****" + "<br>");
						}

						if (session.getAttribute("dst-file-storage-s3-url-" + dstIndex) != null) {
							out.print("<b>Destination S3 URL</b> : " + session.getAttribute("dst-file-storage-s3-url-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-s3-bucket-name-" + dstIndex) != null) {
							out.print("<b>Destination S3 Bucket Name</b> : " + session.getAttribute("dst-file-storage-s3-bucket-name-" + dstIndex).toString() + "<br>");
						}
						
						if (session.getAttribute("dst-file-storage-s3-access-key-" + dstIndex) != null) {
							out.print("<b>Destination S3 Access Key</b> : " + "*****" + "<br>");
						}

						if (session.getAttribute("dst-file-storage-s3-secret-key-" + dstIndex) != null) {
							out.print("<b>Destination S3 Secret Key</b> : " + "*****" + "<br>");
						}

						if (session.getAttribute("dst-file-storage-minio-url-" + dstIndex) != null) {
							out.print("<b>Destination MinIO URL</b> : " + session.getAttribute("dst-file-storage-minio-url-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-file-storage-minio-bucket-name-" + dstIndex) != null) {
							out.print("<b>Destination MinIO Bucket Name</b> : " + session.getAttribute("dst-file-storage-minio-bucket-name-" + dstIndex).toString() + "<br>");
						}
						
						if (session.getAttribute("dst-file-storage-minio-access-key-" + dstIndex) != null) {
							out.print("<b>Destination MinIO Access Key</b> : " + "*****" + "<br>");
						}

						if (session.getAttribute("dst-file-storage-minio-secret-key-" + dstIndex) != null) {
							out.print("<b>Destination MinIO Secret Key</b> : " + "*****" + "<br>");
						}						
			    	} else if (session.getAttribute("dst-type-" + dstIndex).toString().equals("DATABARICS_SQL")) {
						if (session.getAttribute("dst-databrics-dbfs-endpoint-" + dstIndex) != null) {
							out.print("<b>Databrics DBFS Endpoint</b> : " + session.getAttribute("dst-databricks-dbfs-endpoint-" + dstIndex).toString() + "<br>");
						}

						if (session.getAttribute("dst-databrics-dbfs-access-token-" + dstIndex) != null) {
							out.print("<b>Databrics DBFS Access Token</b> : " + "*****" + "<br>");
						}

						if (session.getAttribute("dst-databrics-dbfs-basepath-" + dstIndex) != null) {
							out.print("<b>Databrics DBFS Base Path</b> : " + session.getAttribute("dst-databricks-dbfs-basepath-" + dstIndex).toString() + "<br>");
						}
			    	} else if (session.getAttribute("dst-type-" + dstIndex).toString().equals("POSTGRESQL")) {
						if (session.getAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex) != null) {
							out.print("<b>PG Vector Extension Enabled</b> : " + session.getAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex).toString() + "<br>");
						}
			    	}

					if (session.getAttribute("dst-alias-" + dstIndex) != null) {
						out.print("<b>Destination DB Alias</b> : " + session.getAttribute("dst-alias-" + dstIndex).toString() + "<br>");
					}
					
			    	if (session.getAttribute("dst-duckdb-reader-port-" + dstIndex) != null) {
						out.print("<b>DuckDB Reader Port</b> : " + request.getSession().getAttribute("dst-duckdb-reader-port-" + dstIndex).toString());
			    	}

					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");
					
					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Data Type Mappings");					
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					out.print("<b>Data Type Mappings</b> : ");
					if (session.getAttribute("dst-data-type-mapping-" + dstIndex).toString().equals("ALL_TEXT")) {
						out.print("All Text<br>");
					} else if (session.getAttribute("dst-data-type-mapping-" + dstIndex).toString().equals("BEST_EFFORT")) {
						out.print("Best Effort Match<br>");
					} else if (session.getAttribute("dst-data-type-mapping-" + dstIndex).toString().equals("CUSTOMIZED")) {
						out.print("Customized<br>");
					} else if(session.getAttribute("dst-data-type-mapping-" + dstIndex).toString().equals("EXACT")) {
						out.print("Exact<br>");
					}
					out.print("<br><br>");
					out.print("<span style=\"white-space: pre\">" + session.getAttribute("dst-data-type-all-mappings-" + dstIndex).toString() + "</span>");
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");					
					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Filter/Mapper Rules");					
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					if (session.getAttribute("dst-enable-filter-mapper-rules-" + dstIndex) != null) {
						out.print("<b>Enable Table/Column Filter/Mapper Rules</b> : " + session.getAttribute("dst-enable-filter-mapper-rules-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-allow-unspecified-tables-" + dstIndex) != null) {
						out.print("<b>Allow Unspecified Tables</b> : " + session.getAttribute("dst-allow-unspecified-tables-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-allow-unspecified-columns-" + dstIndex) != null) {
						out.print("<b>Allow Unspecified Columns</b> : " + session.getAttribute("dst-allow-unspecified-columns-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-filter-mapper-rules-file-" + dstIndex) != null) {
						out.print("<b>Table/Column Filter/Mapper Configuration File</b> : " + session.getAttribute("dst-filter-mapper-rules-file-" + dstIndex).toString() + "<br>");
					}
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");

					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Value Mappings");
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					if (session.getAttribute("dst-enable-value-mapper-" + dstIndex) != null) {
						out.print("<b>Enable Value Mappings</b> : " + session.getAttribute("dst-enable-value-mapper-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-value-mappings-file-" + dstIndex) != null) {
						out.print("<b>Value Mappings JSON file</b> : " + session.getAttribute("dst-value-mappings-file-" + dstIndex).toString() + "<br>");
					}
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");

					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Triggers");
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					if (session.getAttribute("dst-enable-triggers-" + dstIndex) != null) {
						out.print("<b>Enable Triggers</b> : " + session.getAttribute("dst-enable-triggers-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-triggers-file-" + dstIndex) != null) {
						out.print("<b>Triggers JSON file</b> : " + session.getAttribute("dst-triggers-file-" + dstIndex).toString() + "<br>");
					}
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");

					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Destination DB " + dstIndex + " Writer Configuration");					
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					if (session.getAttribute("dst-device-schema-name-policy-" + dstIndex) != null) {
						out.print("<b>Replicate Device Tables to Destination Schema with schema name as</b> : " + session.getAttribute("dst-device-schema-name-policy-" + dstIndex).toString() + "<br>");
					}
					out.print("<b>Destination Object Initiailization Mode</b> : " + session.getAttribute("dst-object-init-mode-" + dstIndex).toString() + "<br>");
					out.print("<b>Create Table Suffix</b> : " + session.getAttribute("dst-create-table-suffix-" + dstIndex).toString() + "<br>");
					out.print("<b>Insert Batch Size (Operations)</b> : " + session.getAttribute("dst-insert-batch-size-" + dstIndex).toString() + "<br>");
					out.print("<b>Insert Batch Size (Operations)</b> : " + session.getAttribute("dst-insert-batch-size-" + dstIndex).toString() + "<br>");
					out.print("<b>Update Batch Size (Operations)</b> : " + session.getAttribute("dst-update-batch-size-" + dstIndex).toString() + "<br>");
					out.print("<b>Delete Batch Size (Operations)</b> : " + session.getAttribute("dst-delete-batch-size-" + dstIndex).toString() + "<br>");
					out.print("<b>Transaction Retry Count</b> : " + session.getAttribute("dst-txn-retry-count-" + dstIndex).toString() + "<br>");
					out.print("<b>Transaction Retry Interval (ms)</b> : " + session.getAttribute("dst-txn-retry-interval-ms-" + dstIndex).toString() + "<br>");
					out.print("<b>Enable Idempotent Data Ingestion</b> : " + session.getAttribute("dst-idempotent-data-ingestion-" + dstIndex).toString() + "<br>");
					out.print("<b>Idempotent Data Ingestion Method</b> : " + session.getAttribute("dst-idempotent-data-ingestion-method-" + dstIndex).toString() + "<br>");
					out.print("<b>Set Unparsable Values To Null</b> : " + session.getAttribute("dst-set-unparsable-values-to-null-" + dstIndex).toString() + "<br>");
					out.print("<b>Quote Object Names</b> : " + session.getAttribute("dst-quote-object-names-" + dstIndex).toString() + "<br>");
					out.print("<b>Quote Column Names</b> : " + session.getAttribute("dst-quote-column-names-" + dstIndex).toString() + "<br>");
					out.print("<b>Use Catalog Scope Resolution</b> : " + session.getAttribute("dst-use-catalog-scope-resolution-" + dstIndex).toString() + "<br>");
					out.print("<b>Use Schema Scope Resolution</b> : " + session.getAttribute("dst-use-schema-scope-resolution-" + dstIndex).toString() + "<br>");
					out.print("<b>Disable SyncLite Metadata on Destination DB</b> : " + session.getAttribute("dst-disable-metadata-table-" + dstIndex).toString() + "<br>");
					out.print("<b>Skip Failed Log Files</b> : " + session.getAttribute("dst-skip-failed-log-files-" + dstIndex).toString() + "<br>");
					
					if (session.getAttribute("dst-clickhouse-engine-" + dstIndex) != null) {
						out.print("<b>ClickHouse Engine</b> : " + session.getAttribute("dst-clickhouse-engine-" + dstIndex).toString() + "<br>");
					}					
					if (session.getAttribute("dst-mongodb-use-transactions-" + dstIndex) != null) {
						out.print("<b>Use MongoDB Transactions</b> : " + session.getAttribute("dst-mongodb-use-transactions-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-spark-configuration-file-" + dstIndex) != null) {
						out.print("<b>Spark Configurations File </b> : " + session.getAttribute("dst-spark-configuration-file-" + dstIndex).toString() + "<br>");
					}
					if (session.getAttribute("dst-type-" + dstIndex).toString().equals("CSV")) {
						out.print("<b>CSV Files with Headers</b> : " + session.getAttribute("dst-csv-files-with-headers-" + dstIndex).toString() + "<br>");
						out.print("<b>CSV Files Field Delimiter</b> : " + session.getAttribute("dst-csv-files-field-delimiter-" + dstIndex).toString() + "<br>");
						out.print("<b>CSV Files Record Delimiter</b> : " + session.getAttribute("dst-csv-files-record-delimiter-" + dstIndex).toString() + "<br>");
						out.print("<b>CSV Files Escape Character</b> : " + session.getAttribute("dst-csv-files-escape-character-" + dstIndex).toString() + "<br>");
						out.print("<b>CSV Files Quote Character</b> : " + session.getAttribute("dst-csv-files-quote-character-" + dstIndex).toString() + "<br>");
						out.print("<b>CSV Files Null String</b> : " + session.getAttribute("dst-csv-files-null-string-" + dstIndex).toString() + "<br>");						
					}
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");					
				}			
			
				if (numDestinations > 1) {
					out.print("<table>");
					out.print("<tr></tr>");
					out.print("<tr>");
					out.print("<th>");
					out.print("Map Devices To Destination DB Configuration");
					out.print("</th>");
					out.print("</tr>");
					out.print("<tr>");
					out.print("<td>");
					
					out.print("<b>Map Devices By</b> : " + session.getAttribute("map-devices-to-dst-pattern-type").toString() + "<br>");					
					for (int dstIndex = 1 ; dstIndex<= numDestinations ; ++dstIndex) {
						String dstDBName = "Destination DB " + dstIndex + " : " + request.getSession().getAttribute("dst-alias-" + dstIndex).toString() + " (" +  request.getSession().getAttribute("dst-type-name-" + dstIndex).toString() + ")";
						out.print("<b>Device Pattern for Destination DB " + dstIndex + " : " + dstDBName + "</b> : " + request.getSession().getAttribute("map-devices-to-dst-pattern-" + dstIndex).toString() + "<br>");
					}
					
					String defaultDstDBIndex = request.getSession().getAttribute("default-dst-index-for-unmapped-devices").toString();
					String defaultDstName = request.getSession().getAttribute("dst-type-name-" + defaultDstDBIndex).toString();
					out.print("<b>Default Destination DB for Unmapped Devices</b>  : Destination DB " +  defaultDstDBIndex + " : " + defaultDstName +"<br");
					out.print("</td>");
					out.print("</tr>");
					out.print("</table>");					
				}
			%>
			<center>
				<button type="submit" name="start">Start</button>
			</center>			
		</form>
	</div>
</body>
</html>
