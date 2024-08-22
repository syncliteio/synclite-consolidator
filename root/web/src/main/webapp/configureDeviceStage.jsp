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

<script type="text/javascript">
</script>
<script type="text/javascript">
	 function onChangeThrottleRequestRate() {
		 var throttleStageRequestRate = document.getElementById("throttle-stage-request-rate").value;
		 if (throttleStageRequestRate.toString() === "true") {
			document.getElementById("max-stage-requests-per-minute").disabled = false;
		 } else {
			document.getElementById("max-stage-requests-per-minute").disabled = true;
		 }
	 }

	 function onChangeSchedulerType() {
		 var deviceSchedulerType = document.getElementById("device-scheduler-type").value;
		 if (deviceSchedulerType.toString() === "EVENT_BASED") {
			document.getElementById("device-polling-interval-ms").value = 30000;
		 } else {
			 //2 secs default for POLLING and STATIC
			 document.getElementById("device-polling-interval-ms").value = 2000;
		 }
	 }

	 function onChangeDeviceEncryptionEnabled() {
		 var deviceEncryptionEnabled = document.getElementById("device-encryption-enabled").value;
		 if (deviceEncryptionEnabled.toString() === "true") {
			document.getElementById("device-decryption-key-file").disabled = false;
		 } else {
			document.getElementById("device-decryption-key-file").disabled = true;
		 }
	 }

	 function populateDefaults() {
		 onChangeThrottleRequestRate();
		 onChangeDeviceEncryptionEnabled();
	 }
</script>

<title>Configure Device Stage</title>
</head>
<%
HashMap<String, Object> properties = new HashMap<String, Object>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

if (session.getAttribute("job-name") != null) {
	properties.put("job-name", session.getAttribute("job-name").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

String errorMsg = request.getParameter("errorMsg");

properties.put("enable-device-command-handler", session.getAttribute("enable-device-command-handler").toString());
if (request.getParameter("device-stage-type") != null) {
	//populate from request object
	properties.put("device-stage-type", request.getParameter("device-stage-type"));
	if (request.getParameter("device-upload-root") != null) {
		properties.put("device-upload-root", request.getParameter("device-upload-root"));
	} else {
		properties.put("device-upload-root", "");
	}

	if (request.getParameter("device-command-root") != null) {
		properties.put("device-command-root", request.getParameter("device-command-root"));
	} else {
		properties.put("device-command-root", "");
	}

	if (request.getParameter("stage-sftp-host") != null) {
		properties.put("stage-sftp-host", request.getParameter("stage-sftp-host"));
	} else {
		properties.put("stage-sftp-host", "");
	}

	if (request.getParameter("stage-sftp-port") != null) {
		properties.put("stage-sftp-port", request.getParameter("stage-sftp-port"));
	} else {
		properties.put("stage-sftp-port", "22");
	}

	if (request.getParameter("stage-sftp-user") != null) {
		properties.put("stage-sftp-user", request.getParameter("stage-sftp-user"));
	} else {
		properties.put("stage-sftp-user", "");
	}

	if (request.getParameter("stage-sftp-password") != null) {
		properties.put("stage-sftp-password", request.getParameter("stage-sftp-password"));
	} else {
		properties.put("stage-sftp-password", "");
	}

	if (request.getParameter("stage-sftp-data-directory") != null) {
		properties.put("stage-sftp-data-directory", request.getParameter("stage-sftp-data-directory"));
	} else {
		properties.put("stage-sftp-data-directory", "");
	}

	if (request.getParameter("stage-sftp-command-directory") != null) {
		properties.put("stage-sftp-command-directory", request.getParameter("stage-sftp-command-directory"));
	} else {
		properties.put("stage-sftp-command-directory", "");
	}

	if (request.getParameter("stage-minio-endpoint") != null) {
		properties.put("stage-minio-endpoint", request.getParameter("stage-minio-endpoint"));
	} else {
		properties.put("stage-minio-endpoint", "");
	}

	if (request.getParameter("stage-minio-data-bucket-name") != null) {
		properties.put("stage-minio-data-bucket-name", request.getParameter("stage-minio-data-bucket-name"));
	} else {
		properties.put("stage-minio-data-bucket-name", "");
	}

	if (request.getParameter("stage-minio-command-bucket-name") != null) {
		properties.put("stage-minio-command-bucket-name", request.getParameter("stage-minio-command-bucket-name"));
	} else {
		properties.put("stage-minio-command-bucket-name", "");
	}

	if (request.getParameter("stage-minio-access-key") != null) {
		properties.put("stage-minio-access-key", request.getParameter("stage-minio-access-key"));
	} else {
		properties.put("stage-minio-access-key", "");
	}
	
	if (request.getParameter("stage-minio-secret-key") != null) {
		properties.put("stage-minio-secret-key", request.getParameter("stage-minio-secret-key"));
	} else {
		properties.put("stage-minio-secret-key", "");
	}

	if (request.getParameter("stage-s3-endpoint") != null) {
		properties.put("stage-s3-endpoint", request.getParameter("stage-s3-endpoint"));
	} else {
		properties.put("stage-s3-endpoint", "");
	}

	if (request.getParameter("stage-s3-bucket-name") != null) {
		properties.put("stage-s3-data-bucket-name", request.getParameter("stage-s3-data-bucket-name"));
	} else {
		properties.put("stage-s3-data-bucket-name", "");
	}

	if (request.getParameter("stage-s3-command-bucket-name") != null) {
		properties.put("stage-s3-command-bucket-name", request.getParameter("stage-s3-command-bucket-name"));
	} else {
		properties.put("stage-s3-command-bucket-name", "");
	}

	if (request.getParameter("stage-s3-access-key") != null) {
		properties.put("stage-s3-access-key", request.getParameter("stage-s3-access-key"));
	} else {
		properties.put("stage-s3-access-key", "");
	}
	
	if (request.getParameter("stage-s3-secret-key") != null) {
		properties.put("stage-s3-secret-key", request.getParameter("stage-s3-secret-key"));
	} else {
		properties.put("stage-s3-secret-key", "");
	}

	if (request.getParameter("stage-kafka-consumer-properties") != null) {
		properties.put("stage-kafka-consumer-properties", request.getParameter("stage-kafka-consumer-properties"));
	} else {
		properties.put("stage-kafka-consumer-properties", "");
	}

	if (request.getParameter("stage-kafka-producer-properties") != null) {
		properties.put("stage-kafka-producer-properties", request.getParameter("stage-kafka-producer-properties"));
	} else {
		properties.put("stage-kafka-producer-properties", "");
	}

	if (request.getParameter("throttle-stage-request-rate") != null) {
		properties.put("throttle-stage-request-rate", request.getParameter("throttle-stage-request-rate"));
	} else {
		properties.put("throttle-stage-request-rate", "true");
	}
	
	if (request.getParameter("max-stage-requests-per-minute") != null) {
		properties.put("max-stage-requests-per-minute", request.getParameter("max-stage-requests-per-minute"));
	} else {
		properties.put("max-stage-requests-per-minute", 2);
	}

	if (request.getParameter("device-encryption-enabled") != null) {
		properties.put("device-encryption-enabled", request.getParameter("device-encryption-enabled"));
	} else {
		properties.put("device-encryption-enabled", "false");
	}
	
	if (request.getParameter("device-decryption-key-file") != null) {
		properties.put("device-decryption-key-file", request.getParameter("device-decryption-key-file"));
	} else {
		Path privateKeyPath = Path.of(System.getProperty("user.home"), ".ssh", "synclite_private_key.der");
		properties.put("device-decryption-key-file", privateKeyPath);
	}
	
	if (request.getParameter("stage-oper-retry-count") != null) {
		properties.put("stage-oper-retry-count", request.getParameter("stage-oper-retry-count"));
	} else {
		properties.put("stage-oper-retry-count", 10);
	}

	if (request.getParameter("stage-oper-retry-interval-ms") != null) {
		properties.put("stage-oper-retry-interval-ms", request.getParameter("stage-oper-retry-interval-ms"));
	} else {
		properties.put("stage-oper-retry-interval-ms", 10000);
	}

	if (request.getParameter("device-scheduler-type") != null) {
		properties.put("device-scheduler-type", request.getParameter("device-scheduler-type"));
	} else {
		properties.put("device-scheduler-type", "EVENT_BASED");
	}

	if (request.getParameter("device-scanner-interval-s") != null) {
		properties.put("device-scanner-interval-s", request.getParameter("device-scanner-interval-s"));
	} else {
		properties.put("device-scanner-interval-s", 30);
	}

	if (request.getParameter("device-polling-interval-ms") != null) {
		properties.put("device-polling-interval-ms", request.getParameter("device-polling-interval-ms"));
	} else {
		properties.put("device-polling-interval-ms", 30000);
	}

} else {
	//Popuate default values in the map
	Path defaultUploadRoot = Path.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "stageDir");
	Path defaultCommandRoot = Path.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "commandDir");
	properties.put("device-stage-type", "FS");
	properties.put("device-upload-root", defaultUploadRoot);
	properties.put("device-command-root", defaultCommandRoot);

	properties.put("stage-sftp-host", "");
	properties.put("stage-sftp-port", "");
	properties.put("stage-sftp-user", "");
	properties.put("stage-sftp-password", "");	
	properties.put("stage-sftp-data-directory", "");	
	properties.put("stage-sftp-command-directory", "");	

	properties.put("stage-minio-endpoint", "");
	properties.put("stage-minio-data-bucket-name", "");
	properties.put("stage-minio-command-bucket-name", "");
	properties.put("stage-minio-access-key", "");
	properties.put("stage-minio-secret-key", "");	

	properties.put("stage-s3-endpoint", "");
	properties.put("stage-s3-data-bucket-name", "");
	properties.put("stage-s3-command-bucket-name", "");
	properties.put("stage-s3-access-key", "");
	properties.put("stage-s3-secret-key", "");

	properties.put("stage-kafka-consumer-properties", "bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
	properties.put("stage-kafka-producer-properties", "bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");

	properties.put("throttle-stage-request-rate", "true");
	properties.put("max-stage-requests-per-minute", "2");
	properties.put("device-encryption-enabled", "false");
	Path privateKeyPath = Path.of(System.getProperty("user.home"), ".ssh", "synclite_private_key.der");
	properties.put("device-decryption-key-file", privateKeyPath);
	properties.put("stage-oper-retry-count", "10");
	properties.put("stage-oper-retry-interval-ms", "10000");
	properties.put("device-scheduler-type", "EVENT_BASED");
	properties.put("device-scanner-interval-s", 30);
	properties.put("device-polling-interval-ms", 30000);
	
	//Read configs from synclite_consolidator.conf if they are present
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
	
	//Read kafka consumer peroprties from config file if prsent
	if (properties.get("device-stage-type").equals("KAFKA")) {
		Path kafkaPropsPath = Path.of(properties.get("device-data-root").toString(), "stage_kafka_consumer.properties");
		if (Files.exists(kafkaPropsPath)) {
			reader = null;
			String readKafkaProps = Files.readString(kafkaPropsPath);
			if (readKafkaProps != null) {
				properties.put("stage-kafka-consumer-properties", readKafkaProps);
			}
		}
	}

	//Read kafka producer peroprties from config file if prsent
	if (properties.get("device-stage-type").equals("KAFKA")) {
		Path kafkaPropsPath = Path.of(properties.get("device-data-root").toString(), "stage_kafka_producer.properties");
		if (Files.exists(kafkaPropsPath)) {
			reader = null;
			String readKafkaProps = Files.readString(kafkaPropsPath);
			if (readKafkaProps != null) {
				properties.put("stage-kafka-producer-properties", readKafkaProps);
			}
		}
	}

}
%>
<body onload="populateDefaults()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure Device Stage</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDeviceStage"
			method="post">

			<table>
				<tbody>
					<tr>
						<td>Device Stage Type</td>
						<td><select id="device-stage-type" name="device-stage-type" onchange="this.form.action='configureDeviceStage.jsp'; this.form.submit();" title="Select device stage type. Device stage is a location/system holding device directories containing data and log files for devices being synced by numerous applications.">
								<%
								if (properties.get("device-stage-type").equals("S3")) {
									out.println("<option value=\"S3\" selected>Amazon S3</option>");
								} else {
									out.println("<option value=\"S3\">Amazon S3</option></option>");
								}								

								if (properties.get("device-stage-type").equals("KAFKA")) {
									out.println("<option value=\"KAFKA\" selected>Apache Kafka</option>");
								} else {
									out.println("<option value=\"KAFKA\">Apache Kafka</option>");
								}

								if (properties.get("device-stage-type").equals("GOOGLE_DRIVE")) {
									out.println("<option value=\"GOOGLE_DRIVE\" selected>Google Drive</option>");
								} else {
									out.println("<option value=\"GOOGLE_DRIVE\">Google Drive</option>");
								}

								if (properties.get("device-stage-type").equals("FS")) {
									out.println("<option value=\"FS\" selected>Local File System</option>");
								} else {
									out.println("<option value=\"FS\">Local File System</option>");
								}

								if (properties.get("device-stage-type").equals("MS_ONEDRIVE")) {
									out.println("<option value=\"MS_ONEDRIVE\" selected>Microsoft OneDrive</option>");
								} else {
									out.println("<option value=\"MS_ONEDRIVE\">Microsoft OneDrive</option>");
								}

								if (properties.get("device-stage-type").equals("LOCAL_MINIO")) {
									out.println("<option value=\"LOCAL_MINIO\" selected>MinIO Object Storage Server (Local)</option>");
								} else {
									out.println("<option value=\"LOCAL_MINIO\">MinIO Object Storage Server (Local)</option></option>");
								}								

								if (properties.get("device-stage-type").equals("REMOTE_MINIO")) {
									out.println("<option value=\"REMOTE_MINIO\" selected>MinIO Object Storage Server (Remote)</option>");
								} else {
									out.println("<option value=\"REMOTE_MINIO\">MinIO Object Storage Server (Remote)</option></option>");
								}								

								if (properties.get("device-stage-type").equals("LOCAL_SFTP")) {
									out.println("<option value=\"LOCAL_SFTP\" selected>Local SFTP Server</option>");
								} else {
									out.println("<option value=\"LOCAL_SFTP\">Local SFTP Server</option>");
								}

								if (properties.get("device-stage-type").equals("REMOTE_SFTP")) {
									out.println("<option value=\"REMOTE_SFTP\" selected>Remote SFTP Server</option>");
								} else {
									out.println("<option value=\"REMOTE_SFTP\">Remote SFTP Server</option>");
								}

								%>
							</select>
						</td>
					</tr>
					<%					

					if (properties.get("device-stage-type").equals("FS")) {
						out.println("<tr>");
						out.println("<td>Device Data Stage Directory</td>");							
						out.println("<td><input type=\"text\" size = 30 id=\"device-upload-root\" name=\"device-upload-root\"  value=\"" + properties.get("device-upload-root") + "\" title=\"Specify the device data stage directory where all device directories are uploaded by all local/remote applications.\"/></td>");
						out.println("</tr>");					 

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>Device Command Stage Directory</td>");							
							out.println("<td><input type=\"text\" size = 30 id=\"device-command-root\" name=\"device-command-root\"  value=\"" + properties.get("device-command-root") + "\" title=\"Specify the device command directory where consolidator creates command files for respective devices under their respective directories for consumption by individual devices.\"/></td>");
							out.println("</tr>"); 
						}
					} else if(properties.get("device-stage-type").equals("LOCAL_SFTP")) {
						out.println("<tr>");
						out.println("<td>SFTP Device Data Directory</td>");					
						out.println("<td><input type=\"text\" size = 50 id=\"device-upload-root\" name=\"device-upload-root\"  value=\"" + properties.get("device-upload-root") + "\" title=\"Specify local SFTP directory which has been configured to receive and store device directories.\"/></td>");
						out.println("</tr>"); 

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>SFTP Device Command Directory</td>");							
							out.println("<td><input type=\"text\" size = 30 id=\"device-command-root\" name=\"device-command-root\"  value=\"" + properties.get("device-command-root") + "\" title=\"Specify the device command directory where consolidator creates command files for respective devices under their respective directories for consumption by individual devices.\"/></td>");
							out.println("</tr>");
						}
					} else if(properties.get("device-stage-type").equals("MS_ONEDRIVE")) {
						out.println("<tr>");
						out.println("<td>Microsoft OneDrive Device Data Stage Directory</td>");							
						out.println("<td><input type=\"text\" size = 30 id=\"device-upload-root\" name=\"device-upload-root\"  value=\"" + properties.get("device-upload-root") + "\" title=\"Specify Microsoft OneDrive base path.\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>Microsoft OneDrive Device Command Directory</td>");							
							out.println("<td><input type=\"text\" size = 30 id=\"device-command-root\" name=\"device-command-root\"  value=\"" + properties.get("device-command-root") + "\" title=\"Specify the device command directory where consolidator creates command files for respective devices under their respective directories for consumption by individual devices.\"/></td>");
							out.println("</tr>");
						}
					} else if(properties.get("device-stage-type").equals("GOOGLE_DRIVE")) {
						out.println("<tr>");
						out.println("<td>Google Drive Device Data Stage Directory</td>");							
						out.println("<td><input type=\"text\" size = 30 id=\"device-upload-root\" name=\"device-upload-root\"  value=\"" + properties.get("device-upload-root") + "\" title=\"Specify Google OneDrive base path.\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>Google Drive Device Command Stage Directory</td>");							
							out.println("<td><input type=\"text\" size = 30 id=\"device-command-root\" name=\"device-command-root\"  value=\"" + properties.get("device-command-root") + "\" title=\"Specify the device command directory where consolidator creates command files for respective devices under their respective directories for consumption by individual devices.\"/></td>");
							out.println("</tr>");
						}
					} else if(properties.get("device-stage-type").equals("LOCAL_MINIO")) {						
						out.println("<tr>");
						out.println("<td>MinIO Endpoint</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-endpoint\" name=\"stage-minio-endpoint\"  value=\"" + properties.get("stage-minio-endpoint") + "\" title=\"Specify MinIO endpoint.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Device Data Stage Bucket Name</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-data-bucket-name\" name=\"stage-minio-data-bucket-name\"  value=\"" + properties.get("stage-minio-data-bucket-name") + "\" title=\"Specify MinIO bucket name which holds all device directories\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Device Data Stage Bucket Storage Path</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"device-upload-root\" name=\"device-upload-root\"  value=\"" + properties.get("device-upload-root") + "\" title=\"Specify storage path of the specified bucket. This is usually %MinIODataDirectoryPath%/%BucketName%\"/></td>");				
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Access Key</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-access-key\" name=\"stage-minio-access-key\"  value=\"" + properties.get("stage-minio-access-key") + "\" title=\"Specify MinIO access key.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Secret Key</td>");
						out.println("<td><input type=\"password\" size = 30 id=\"stage-minio-secret-key\" name=\"stage-minio-secret-key\"  value=\"" + properties.get("stage-minio-secret-key") + "\" title=\"Specify MinIO secret key.\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>MinIO Device Command Stage Bucket Name</td>");
							out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-command-bucket-name\" name=\"stage-minio-command-bucket-name\"  value=\"" + properties.get("stage-minio-command-bucket-name") + "\" title=\"Specify MinIO bucket name which holds commands to be sent to devices\"/></td>");
							out.println("</tr>");
						}						
					} else if(properties.get("device-stage-type").equals("REMOTE_MINIO")) {						
						out.println("<tr>");
						out.println("<td>MinIO Endpoint</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-endpoint\" name=\"stage-minio-endpoint\"  value=\"" + properties.get("stage-minio-endpoint") + "\" title=\"Specify MinIO endpoint.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Device Data Stage Bucket Name</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-data-bucket-name\" name=\"stage-minio-data-bucket-name\"  value=\"" + properties.get("stage-minio-data-bucket-name") + "\" title=\"Specify MinIO bucket name which holds all device directories\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>MinIO Device Command Stage Bucket Name</td>");
							out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-command-bucket-name\" name=\"stage-minio-command-bucket-name\"  value=\"" + properties.get("minio-command-bucket-name") + "\" title=\"Specify MinIO bucket name which holds commands to be sent to devices.\"/></td>");
							out.println("</tr>");
						}

						out.println("<tr>");
						out.println("<td>MinIO Access Key</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-minio-access-key\" name=\"stage-minio-access-key\"  value=\"" + properties.get("stage-minio-access-key") + "\" title=\"Specify MinIO access key.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>MinIO Secret Key</td>");
						out.println("<td><input type=\"password\" size = 30 id=\"stage-minio-secret-key\" name=\"stage-minio-secret-key\"  value=\"" + properties.get("stage-minio-secret-key") + "\" title=\"Specify MinIO secret key.\"/></td>");
						out.println("</tr>");


						out.println("<tr>");
						out.println("<td>New Device Scan Interval (s)</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"device-scanner-interval-s\" name=\"device-scanner-interval-s\"  value=\"" + properties.get("device-scanner-interval-s") + "\" title=\"Specify interval (in seconds) at which consolidator should look for new device buckets in the MinIO object storage.\"/></td>");
						out.println("</tr>");
					} else if (properties.get("device-stage-type").equals("REMOTE_SFTP")) {
						out.println("<tr>");
						out.println("<td>Remote SFTP Server Host</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-sftp-host\" name=\"stage-sftp-host\"  value=\"" + properties.get("stage-sftp-host") + "\" title=\"Specify remote SFTP host.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>SFTP Server Port</td>");
						out.println("<td><input type=\"number\" size = 30 id=\"stage-sftp-port\" name=\"stage-sftp-port\"  value=\"" + properties.get("stage-sftp-port") + "\" title=\"Specify SFTP server port\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>SFTP User</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-sftp-user\" name=\"stage-sftp-user\"  value=\"" + properties.get("stage-sftp-user") + "\" title=\"Specify SFTP user. Make sure this user has list, read and write access to the specified remote stage directory.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>SFTP User Password</td>");
						out.println("<td><input type=\"password\" size = 30 id=\"stage-sftp-password\" name=\"stage-sftp-password\"  value=\"" + properties.get("stage-sftp-password") + "\" title=\"Specify SFTP user's password.\"/></td>");
						out.println("</tr>");
						
						out.println("<tr>");
						out.println("<td>SFTP Data Stage Directory </td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-sftp-data-directory\" name=\"stage-sftp-data-directory\"  value=\"" + properties.get("stage-sftp-data-directory") + "\" title=\"Specify SFTP directory path holding device directories.\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>SFTP Command Stage Directory</td>");
							out.println("<td><input type=\"text\" size = 30 id=\"stage-sftp-command-directory\" name=\"stage-sftp-command-directory\"  value=\"" + properties.get("stage-sftp-command-directory") + "\" title=\"Specify SFTP directory which holds commands to be sent to devices\"/></td>");
							out.println("</tr>");
						}

						out.println("<tr>");
						out.println("<td>New Device Scan Interval (s)</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"device-scanner-interval-s\" name=\"device-scanner-interval-s\"  value=\"" + properties.get("device-scanner-interval-s") + "\" title=\"Specify interval (in seconds) at which consolidator should look for new device buckets in the MinIO object storage.\"/></td>");
						out.println("</tr>");
					
					} else if(properties.get("device-stage-type").equals("S3")) {						
						out.println("<tr>");
						out.println("<td>S3 Endpoint</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-s3-endpoint\" name=\"stage-s3-endpoint\"  value=\"" + properties.get("stage-s3-endpoint") + "\" title=\"Specify S3 endpoint such as https://s3.ap-south-1.amazonaws.com \"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>S3 Device Data Stage Bucket Name</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-s3-data-bucket-name\" name=\"stage-s3-data-bucket-name\"  value=\"" + properties.get("stage-s3-data-bucket-name") + "\" title=\"Specify S3 bucket name which holds all device directories\"/></td>");
						out.println("</tr>");

						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>S3 Device Command Stage Bucket Name</td>");
							out.println("<td><input type=\"text\" size = 30 id=\"stage-s3-command-bucket-name\" name=\"stage-s3-command-bucket-name\"  value=\"" + properties.get("stage-s3-command-bucket-name") + "\" title=\"Specify S3 bucket name which holds commands to be sent to devices\"/></td>");
							out.println("</tr>");
						}

						out.println("<tr>");
						out.println("<td>S3 Access Key</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"stage-s3-access-key\" name=\"stage-s3-access-key\"  value=\"" + properties.get("stage-s3-access-key") + "\" title=\"Specify S3 access key.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>S3 Secret Key</td>");
						out.println("<td><input type=\"password\" size = 30 id=\"stage-s3-secret-key\" name=\"stage-s3-secret-key\"  value=\"" + properties.get("stage-s3-secret-key") + "\" title=\"Specify S3 secret key.\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>Throttle Request Rate</td>");
						out.println("<td>");				
						out.println("<select id=\"throttle-stage-request-rate\" name=\"throttle-stage-request-rate\" onchange = \"onChangeThrottleRequestRate()\"  title=\"Specify if request rate to device stage should be throttled. This configurartion is critical to limit the storage cost for storages like Amazon S3 which charge based on request rate. If this option is set to true then SyncLite Consolidator will try to limit the storage access request rate for operations such as Get/List/Delete objects around the specified threshold. While this can help limit the storage cost, it will slow down the data consolidation and increase the overall latency (price vs latency trade-off)\">");
						if (properties.get("throttle-stage-request-rate").equals("true")) {
							out.println("<option value=\"true\" selected>true</option>");
						} else {
							out.println("<option value=\"true\">true</option>");
						}
						if (properties.get("throttle-stage-request-rate").equals("false")) {
							out.println("<option value=\"false\" selected>false</option>");
						} else {
							out.println("<option value=\"false\">false</option>");
						}
						out.println("</select>");
						out.println("</td>");
						out.println("</tr>");
						out.println("<tr>");
						out.println("<td>Maximum Stage Requests Per Minute</td>");
						out.println("<td><input type=\"text\" size=30 id=\"max-stage-requests-per-minute\" name=\"max-stage-requests-per-minute\"  value=\"" + properties.get("max-stage-requests-per-minute") + "\" title=\"Specify maximum request rate threshold\"/></td>");
						out.println("</tr>");

						out.println("<tr>");
						out.println("<td>New Device Scan Interval (s)</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"device-scanner-interval-s\" name=\"device-scanner-interval-s\"  value=\"" + properties.get("device-scanner-interval-s") + "\" title=\"Specify interval (in seconds) at which consolidator should look for new device buckets in the MinIO object storage. Please specify this value carefully by estimating the price since some storages such as Amazon S3 charge based on number of ListBucket calls made.\"/></td>");
						out.println("</tr>");
					}  else if(properties.get("device-stage-type").equals("KAFKA")) {
						if (properties.get("enable-device-command-handler").toString().equals("true")) {
							out.println("<tr>");
							out.println("<td>Kafka Producer Properties</td>");
							out.println("<td><textarea id=\"stage-kafka-producer-properties\" name=\"stage-kafka-producer-properties\"  rows=\"4\" cols=\"70\" title=\"Specify Kafka producer properties (to be used for device command handler) as <propName>=<propValue> pairs, one line per property\">" + properties.get("stage-kafka-producer-properties") + "</textarea></td>");
							out.println("</tr>");						
						}

						out.println("<tr>");
						out.println("<td>Kafka Consumer Properties</td>");
						out.println("<td><textarea id=\"stage-kafka-consumer-properties\" name=\"stage-kafka-consumer-properties\"  rows=\"4\" cols=\"70\" title=\"Specify Kafka consumer properties as <propName>=<propValue> pairs, one line per property\">" + properties.get("stage-kafka-consumer-properties") + "</textarea></td>");
						out.println("</tr>");						

						out.println("<tr>");
						out.println("<td>New Device Scan Interval (s)</td>");
						out.println("<td><input type=\"text\" size = 30 id=\"device-scanner-interval-s\" name=\"device-scanner-interval-s\"  value=\"" + properties.get("device-scanner-interval-s") + "\" title=\"Specify interval (in seconds) at which consolidator should look for new device topics in Kafka.\"/></td>");
						out.println("</tr>");
					}
					
					out.println("<tr>");
					out.println("<td>Device Encryption Enabled</td>");
					out.println("<td>");				
					out.println("<select id=\"device-encryption-enabled\" name=\"device-encryption-enabled\" onchange = \"onChangeDeviceEncryptionEnabled()\"  title=\"Specify if device encryption has been enabled.\">");
					if (properties.get("device-encryption-enabled").equals("true")) {
						out.println("<option value=\"true\" selected>true</option>");
					} else {
						out.println("<option value=\"true\">true</option>");
					}
					if (properties.get("device-encryption-enabled").equals("false")) {
						out.println("<option value=\"false\" selected>false</option>");
					} else {
						out.println("<option value=\"false\">false</option>");
					}
					out.println("</select>");
					out.println("</td>");
					out.println("</tr>");
					
					out.println("<tr>");
					out.println("<td>Device Decryption Key File</td>");
					out.println("<td><input type=\"text\" size = 50 id=\"device-decryption-key-file\" name=\"device-decryption-key-file\"  value=\"" + properties.get("device-decryption-key-file") + "\" title=\"Specify the path of private key DER file\" disabled/></td>");
					out.println("</tr>");				

					%>

					<tr>
						<td>Device Scheduler Type</td>
						<td><select id="device-scheduler-type" name="device-scheduler-type"  onchange="onChangeSchedulerType()" title="Select device scheduler type. Device scheduler type EVENT_BASED is only applicable for local file system based stage types as it leverages FileSystem notifications on arrival of new log segments.">
								<%
								if (properties.get("device-stage-type").equals("REMOTE_MINIO") ||
									properties.get("device-stage-type").equals("S3") || 
									properties.get("device-stage-type").equals("KAFKA") ||
									properties.get("device-stage-type").equals("REMOTE_SFTP")
								) {
									out.println("<option value=\"POLLING\" selected>Polling</option>");								
								} else {									
									if (properties.get("device-scheduler-type").equals("POLLING")) {
										out.println("<option value=\"POLLING\" selected>Polling</option>");
									} else {
										out.println("<option value=\"POLLING\">Polling</option>");					
									}
									if (properties.get("device-scheduler-type").equals("EVENT_BASED")) {
										out.println("<option value=\"EVENT_BASED\" selected>Event Based</option>");
									} else {
										out.println("<option value=\"EVENT_BASED\">Event Based</option>");
									}
									if (properties.get("device-scheduler-type").equals("STATIC")) {
										out.println("<option value=\"STATIC\" selected>Static</option>");
									} else {
										out.println("<option value=\"STATIC\">Static</option>");
									}
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Device Polling Interval (ms)</td>
						<td>
							<input type="text" size = 30 id="device-polling-interval-ms" name="device-polling-interval-ms"  value=<%=properties.get("device-polling-interval-ms")%> title="Specify interval (in milliseconds) at which consolidator should look for new log segments to process in each device.">
						</td>
					</tr>

					<tr>
						<td>Failed Operation Retry Count</td>
						<td><input type="number" id="stage-oper-retry-count"
							name="stage-oper-retry-count"
							value="<%=properties.get("stage-oper-retry-count")%>" 
							title="Specify number of retry attempts for each failed operation on the device stage. After exhausting the retries, a device is marked as failed and retried after 'Failed Device Retry Interval'"/></td>
					</tr>

					<tr>
						<td>Failed Operation Retry Interval (ms)</td>
						<td><input type="number" id="stage-oper-retry-interval-ms"
							name="stage-oper-retry-interval-ms"
							value=<%=properties.get("stage-oper-retry-interval-ms")%> " 
							title="Specify a minimum backoff interval in milliseconds after which a failed operation will be retried on device stage."/></td>
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
