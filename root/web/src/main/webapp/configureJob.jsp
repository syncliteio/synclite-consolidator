<%@page import="java.nio.file.StandardCopyOption"%>
<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.net.ServerSocket"%>
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
	 function onChangeEnableRequestProcessor() {
		 var requestProcessorEnabled = document.getElementById("enable-request-processor").value;
		 if (requestProcessorEnabled.toString() === "true") {
			document.getElementById("request-processor-port").disabled = false;
		 } else {
			document.getElementById("request-processor-port").disabled = true;
		 }
	 }
	 
	 function onChangeDeviceCommandHandlerEnabled() {
		 var deviceCommandHandlerEnabled = document.getElementById("device-command-handler-enabled").value;
		 if (deviceCommandHandlerEnabled.toString() === "true") {
			document.getElementById("device-command-timeout-s").disabled = false;
		 } else {
			document.getElementById("device-command-timeout-s").disabled = true;			 
		 }
	 }

</script>
<title>Configure Job</title>
</head>
<%
String errorMsg = request.getParameter("errorMsg");
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

if (session.getAttribute("src-app-type") != null) {
	properties.put("src-app-type", session.getAttribute("src-app-type").toString());
} else {
	properties.put("src-app-type", "CUSTOM");		
}

if (request.getParameter("dst-sync-mode") != null) {
	//populate from request object
	//properties.put("license-file", request.getParameter("license-file"));
	properties.put("dst-sync-mode", request.getParameter("dst-sync-mode"));
	properties.put("num-device-processors", request.getParameter("num-device-processors"));
	properties.put("device-name-pattern", request.getParameter("device-name-pattern"));
	properties.put("device-id-pattern", request.getParameter("device-id-pattern"));
	properties.put("enable-replicas-for-telemetry-devices", request.getParameter("enable-replicas-for-telemetry-devices"));
	properties.put("disable-replicas-for-appender-devices", request.getParameter("disable-replicas-for-appender-devices"));
	properties.put("skip-bad-txn-files", request.getParameter("skip-bad-txn-files"));
	properties.put("failed-device-retry-interval-s", request.getParameter("failed-device-retry-interval-s"));
	properties.put("device-trace-level", request.getParameter("device-trace-level"));
	properties.put("enable-request-processor", request.getParameter("enable-request-processor"));
	properties.put("request-processor-port", request.getParameter("request-processor-port"));
	properties.put("enable-device-command-handler", request.getParameter("enable-device-command-handler"));
	properties.put("device-command-timeout-s", request.getParameter("device-command-timeout-s"));
	properties.put("jvm-arguments", request.getParameter("jvm-arguments"));	
} else {
	/*
		String licenseFile = "";
		Path defaultDataRoot = Path.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "workDir");
		if (properties.get("device-data-root").toString().equals(defaultDataRoot.toString())) {
			//Copy developer license to workDir and give this as a default license	
			String libPath = application.getRealPath("/WEB-INF/lib");
			Path srcLicFilePath = Path.of(libPath, "synclite_developer.lic");
			Path dstLicFilePath = Path.of(properties.get("device-data-root").toString(), "synclite_developer.lic");
			Files.copy(srcLicFilePath, dstLicFilePath, StandardCopyOption.REPLACE_EXISTING);
			licenseFile = dstLicFilePath.toString();
		}
		properties.put("license-file", licenseFile);
	*/
	if (properties.get("src-app-type").equals("SYNCLITE-DBREADER")) {
		properties.put("dst-sync-mode", "REPLICATION");
	} else {
		properties.put("dst-sync-mode", "CONSOLIDATION");
	}
	
	if (properties.get("src-app-type").equals("CUSTOM")) {
		properties.put("num-device-processors", Runtime.getRuntime().availableProcessors() * 2);
	} else {
		properties.put("num-device-processors", Runtime.getRuntime().availableProcessors());
	}
	properties.put("device-name-pattern", ".*");
	properties.put("device-id-pattern", ".*");
	properties.put("enable-replicas-for-telemetry-devices", "false");
	properties.put("disable-replicas-for-appender-devices", "true");	
	properties.put("skip-bad-txn-files", "false");	
	properties.put("failed-device-retry-interval-s", 30);
	properties.put("device-trace-level", "INFO");
	properties.put("enable-request-processor", "false");
	properties.put("enable-device-command-handler", "false");
	properties.put("device-command-timeout-s", "60");
	properties.put("jvm-arguments", "");
	//Read configs from conf file if they are present
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
				} else {
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

	if (properties.get("request-processor-port") == null) {
		int identifiedPort = 10100; //default value to use
		for (int port = 10000; port <= 10100; port++) {
			try (ServerSocket serverSocket = new ServerSocket(port)) {
				identifiedPort = port;
				break;
			} catch (Exception e) {
				// Port is already in use, try the next port
			}
		}
		properties.put("request-processor-port", identifiedPort);
	}
}
%>

<body onload="onChangeEnableRequestProcessor()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure SyncLite Consolidator</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateJobConfiguration"
			method="post">

			<table>
				<tbody>		
					<tr>
						<td>Work Directory</td>
						<td><input type="text" size=50 id="device-data-root"
							name="device-data-root"
							value="<%=properties.get("device-data-root")%>" readonly
							title="Specify a work directory for SyncLite Consolidator to hold all device directories."/>
						</td>
					</tr>
						<tr>
						<td>Source Application Type</td>
						<td><input type="text" id="src-app-type"
							name="src-app-type"
							value="<%=properties.get("src-app-type")%>" readonly
							title="Specify the type of applications which are synchronizing SyncLite devices with this instance of consolidator"/>
						</td>

					<tr>
						<td>Sync Mode</td>
						<td><select id="dst-sync-mode" name="dst-sync-mode" title="Specify data sync mode. CONSOLIDATION mode merges relational data received from all devices whereas REPLICATION mode replicates data from each device into a separate schema.">
								<%
								if (properties.get("dst-sync-mode").equals("CONSOLIDATION")) {
									out.println("<option value=\"CONSOLIDATION\" selected>Consolidation</option>");
								} else {
									out.println("<option value=\"CONSOLIDATION\">Consolidation</option>");
								}
								if (properties.get("dst-sync-mode").equals("REPLICATION")) {
									out.println("<option value=\"REPLICATION\" selected>Replication</option>");
								} else {
									out.println("<option value=\"REPLICATION\">Replication</option>");
								}
								%>
						</select></td>
					</tr>

					<tr>
						<td>Device Processors</td>
						<td><input type="number" id="num-device-processors"
							name="num-device-processors"
							value="<%=properties.get("num-device-processors")%>" 
							title="Specify number of device processor threads."/>
						</td>
					</tr>

					<tr>
						<td>Allowed Device Name Pattern</td>
						<td><input type="text" id="device-name-pattern"
							name="device-name-pattern"
							value="<%=properties.get("device-name-pattern")%>"
							title ="Specify a (Java) regular expression pattern of device names to allow consolidation of only a selected devices."/>
						</td>
					</tr>

					<tr>
						<td>Allowed Device ID Pattern</td>
						<td><input type="text" id="device-id-pattern"
							name="device-id-pattern"
							value="<%=properties.get("device-id-pattern")%>"
							title ="Specify a (Java) regular expression pattern of device IDs to allow consolidation of only a selected devices."/></td>
					</tr>

					<tr>
						<td>Enable Replicas For Streaming/Telemetry Devices</td>
						<td>
							<select id="enable-replicas-for-telemetry-devices"
							name="enable-replicas-for-telemetry-devices"
							value="<%=properties.get("enable-replicas-for-telemetry-devices")%>" 
							title="Specify if a replicas must be enabled for telemetry devices. By default, replicas are disabled for telemetry devices."/>						
								<%
								if (properties.get("enable-replicas-for-telemetry-devices").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("enable-replicas-for-telemetry-devices").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</td>
					</tr>
					<tr>
						<td>Disable Replicas For Appender Devices</td>
						<td>
							<select id="disable-replicas-for-appender-devices"
							name="disable-replicas-for-appender-devices"
							value="<%=properties.get("disable-replicas-for-appender-devices")%>" 
							title="Specify if a replicas must be disabled for appender devices. By default, replicas are enabled for appender devices."/>						
								<%
								if (properties.get("disable-replicas-for-appender-devices").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("disable-replicas-for-appender-devices").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</td>
					</tr>
					<tr>
						<td>Skip Missing/Corrupt Transaction Files</td>
						<td>
							<select id="skip-bad-txn-files"
							name="skip-bad-txn-files"
							value="<%=properties.get("skip-bad-txn-files")%>" 
							title="Specify if missing/corrupt SyncLite transactionl files which are reffered by the incoming sqllog files should be skipped to continue consolidation/replication without failing."/>						
								<%
								if (properties.get("skip-bad-txn-files").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("skip-bad-txn-files").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</td>
					</tr>
					<tr>
						<td>Failed Device Retry Interval (s)</td>
						<td><input type="number" id="failed-device-retry-interval-s"
							name="failed-device-retry-interval-s"
							value="<%=properties.get("failed-device-retry-interval-s")%>" 
							title="Specify a minimum backoff interval in seconds after which consolidation will be retried for devices for which failure was encountered."/></td>
					</tr>
					
					<tr>
						<td>Consolidator Trace Level</td>
						<td><select id="device-trace-level" name="device-trace-level" title="Specify consolidator trace level. DEBUG level indicates exhaustive tracing, ERROR level indicates only error reporting and INFO level indicates tracing of important events including errors in the trace files.">
								<%
								if (properties.get("device-trace-level").equals("ERROR")) {
									out.println("<option value=\"ERROR\" selected>ERROR</option>");
								} else {
									out.println("<option value=\"ERROR\">ERROR</option>");
								}

								if (properties.get("device-trace-level").equals("INFO")) {
									out.println("<option value=\"INFO\" selected>INFO</option>");
								} else {
									out.println("<option value=\"INFO\">INFO</option>");
								}

								if (properties.get("device-trace-level").equals("DEBUG")) {
									out.println("<option value=\"DEBUG\" selected>DEBUG</option>");
								} else {
									out.println("<option value=\"DEBUG\">DEBUG</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Enable Online Request Processor</td>
						<td><select id="enable-request-processor"
							name="enable-request-processor" onchange="onChangeEnableRequestProcessor()" title="Specify if online request processor should be enabled in the running job.">
								<%
								if (properties.get("enable-request-processor").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("enable-request-processor").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>
					<tr>
						<td>Online Request Processor Port</td>
						<td><input type="number" id="request-processor-port"
							name="request-processor-port"
							value="<%=properties.get("request-processor-port")%>"
							title ="Port to used by the online request processor running inside the consolidator job."/></td>
					</tr>

					<%
						out.println("<tr>");
						out.println("<td>Enable Device Command Handler</td>");
						out.println("<td>");				
						out.println("<select id=\"enable-device-command-handler\" name=\"enable-device-command-handler\" onchange = \"onChangeDeviceCommandHandlerEnabled()\"  title=\"Specify if device command handler should be enabled. This gives an ability to send commands to devices from consolidator through Manage Devices page. Make sure that it is enabled on the devices end as well.\">");
						if (properties.get("enable-device-command-handler").equals("true")) {
							out.println("<option value=\"true\" selected>true</option>");
						} else {
							out.println("<option value=\"true\">true</option>");
						}
						if (properties.get("enable-device-command-handler").equals("false")) {
							out.println("<option value=\"false\" selected>false</option>");
						} else {
							out.println("<option value=\"false\">false</option>");
						}
						out.println("</select>");
						out.println("</td>");
						out.println("</tr>");
					%>

					<tr>
						<td>Device Command Timeout (s)</td>
						<td><input type="number" id="device-command-timeout-s"
							name="device-command-timeout-s"
							value="<%=properties.get("device-command-timeout-s")%>"
							title ="Timeout period (in seconds) after which a dispatced command may get timed out if not ptocessed by the device."/></td>
					</tr>

					<tr>
						<td>JVM Arguments</td>
						<td><input type="text" size=50 id="jvm-arguments"
							name="jvm-arguments"
							value="<%=properties.get("jvm-arguments")%>"
							title ="Specify JVM arguments which should be used while starting the consolidator job. e.g. For setting initial and max heap size as 8GB, you can specify -Xms8g -Xmx8g"/></td>
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
