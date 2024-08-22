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
function onChangeEnablePrometheusStatisticsPublisher() {
	 var enablePrometheusStatisticsPublisher = document.getElementById("enable-prometheus-statistics-publisher").value;
	 if (enablePrometheusStatisticsPublisher.toString() === "true") {
		document.getElementById("prometheus-push-gateway-url").disabled = false;
		document.getElementById("prometheus-statistics-publisher-interval-s").disabled = false;
	 } else {
		document.getElementById("prometheus-push-gateway-url").disabled = true;
		document.getElementById("prometheus-statistics-publisher-interval-s").disabled = true;
	 }
}
</script>
<title>Configure Job Monitor</title>
</head>
<%
String errorMsg = request.getParameter("errorMsg");
HashMap<String, Object> properties = new HashMap<String, Object>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

Path statsPath = Path.of(properties.get("device-data-root").toString(), "synclite_consolidator_statistics.db");
if (request.getParameter("update-statistics-interval-s") != null) {
	//populate from request object
	properties.put("synclite-statistics-file-path", statsPath);
	properties.put("update-statistics-interval-s", request.getParameter("update-statistics-interval-s"));
	properties.put("enable-prometheus-statistics-publisher", request.getParameter("enable-prometheus-statistics-publisher"));
	properties.put("prometheus-push-gateway-url", request.getParameter("prometheus-push-gateway-url"));
	properties.put("prometheus-statistics-publisher-interval-s", request.getParameter("prometheus-statistics-publisher-interval-s"));
} else {
	properties.put("synclite-statistics-file-path", statsPath);
	properties.put("update-statistics-interval-s", "1");
	properties.put("enable-prometheus-statistics-publisher", "false");
	properties.put("prometheus-push-gateway-url", "http://localhost:9091");
	properties.put("prometheus-statistics-publisher-interval-s", "60");
	//Read configs from syncJobs.props if they are present
	BufferedReader reader = null;
	Path propsPath = Path.of(properties.get("device-data-root").toString(), "synclite_consolidator.conf");
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
	} catch (Exception e){ 
		if (reader != null) {
			reader.close();
		}
		throw e;
	} 
}

%>
<body onload="onChangeEnablePrometheusStatisticsPublisher()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure SyncLite Consolidator Job Monitor</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateJobMonitorConfiguration"
			method="post">

			<table>
				<tbody>
					<tr>
						<td>SyncLite Statistics File Path </td>
						<td><input type="text" size=80 id="synclite-statistics-file-path"
							name="device-data-root"
							value="<%=properties.get("synclite-statistics-file-path")%>" readonly
							title="Path to a SQLite db file containing latest SyncLite Consolidator statistics job statistics for the running job" disabled/>
						</td>
					</tr>

					<tr>
						<td>Statistics Update Interval (S)</td>
						<td><input type="number" id="update-statistics-interval-s"
							name="update-statistics-interval-s"
							value="<%=properties.get("update-statistics-interval-s")%>" 
							title="Specify interval in seconds at which the SyncLite Consolidator job should refresh statistics in the statistics db file."/></td>
					</tr>
					
					<tr>
						<td>Enable Prometheus Statistics Publisher</td>
						<td><select id="enable-prometheus-statistics-publisher" name="enable-prometheus-statistics-publisher" onchange="onChangeEnablePrometheusStatisticsPublisher()"  title="Specify if Prometheus statistics publisher should be enabled.">
								<%
								if (properties.get("enable-prometheus-statistics-publisher").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("enable-prometheus-statistics-publisher").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>
					
					<tr>
						<td>Prometheus Push Gateway URL</td>
						<td><input type="text" size=30 id="prometheus-push-gateway-url"
							name="prometheus-push-gateway-url"
							value="<%=properties.get("prometheus-push-gateway-url")%>" readonly
							title="Prometheus push gateway URL to publish the statistics"/>
						</td>
					</tr>

					<tr>
						<td>Prometheus Statistics Publisher Interval (S)</td>
						<td><input type="number" id="prometheus-statistics-publisher-interval-s"
							name="prometheus-statistics-publisher-interval-s"
							value="<%=properties.get("prometheus-statistics-publisher-interval-s")%>" 
							title="Specify interval in seconds at which the SyncLite Consolidator job should publish statistics to the configured Prometheus endpoint."/></td>
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