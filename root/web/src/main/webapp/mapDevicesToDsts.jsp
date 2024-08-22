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

<title>Map SyncLite Devices To Destination DB</title>
</head>
<body onload="onChangePatternType()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Map Devices To Destination Databases</h2>
		<%

		Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());

		String errorMsg = request.getParameter("errorMsg");
		HashMap<String, String> properties = new HashMap<String, String>();
		if (request.getParameter("map-devices-to-dst-pattern-type") != null) {
			properties.put("map-devices-to-dst-pattern-type", request.getParameter("map-devices-to-dst-pattern-type"));
			for (int dstIndex=1 ; dstIndex <= numDestinations ; ++dstIndex) {
				properties.put("map-devices-to-dst-pattern-" + dstIndex, request.getParameter("map-devices-to-dst-pattern-" + dstIndex));
			}			
			properties.put("default-dst-index-for-unmapped-devices", request.getParameter("default-dst-index-for-unmapped-devices"));					
		} else {
			properties.put("map-devices-to-dst-pattern-type", "DEVICE_NAME_PATTERN");
			for (int dstIndex=1 ; dstIndex <= numDestinations ; ++dstIndex) {
				properties.put("map-devices-to-dst-pattern-" + dstIndex, ".*");
			}			
			properties.put("default-dst-index-for-unmapped-devices", "1");					

			//Read configs from conf file if they are present
			String deviceDataRoot = session.getAttribute("device-data-root").toString();
			Path propsPath = Path.of(deviceDataRoot, "synclite_consolidator.conf");
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
			} catch (Exception e){ 
				if (reader != null) {
					reader.close();
				}
				throw e;
			}
		}			

		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		} 
		%>

		<form action="${pageContext.request.contextPath}/mapDevicesToDsts" method="post">
			<table>
				<tbody>
					<tr>
						<td>Map Devices By</td>
						<td><select id="map-devices-to-dst-pattern-type" name="map-devices-to-dst-pattern-type" onchange=onChangePatternType() title="Specify whether the devices to be mapped are specified by device names or device IDs">
								<%
								if (properties.get("map-devices-to-dst-pattern-type").equals("DEVICE_NAME_PATTERN")) {
									out.println("<option value=\"DEVICE_NAME_PATTERN\" selected>DEVICE_NAME_PATTERN</option>");
								} else {
									out.println("<option value=\"DEVICE_NAME_PATTERN\">DEVICE_NAME_PATTERN</option>");
								}								
								if (properties.get("map-devices-to-dst-pattern-type").equals("DEVICE_ID_PATTERN")) {
									out.println("<option value=\"DEVICE_ID_PATTERN\" selected>DEVICE_ID_PATTERN</option>");
								} else {
									out.println("<option value=\"DEVICE_ID_PATTERN\">DEVICE_ID_PATTERN</option>");
								}
								%>
							</select>
						</td>
					</tr>
					
					<%
						for (int dstIndex=1 ; dstIndex <= numDestinations ; ++dstIndex) {
							out.println("<tr>");
							out.println("<td>");
							String dstDBName = "Destination DB " + dstIndex + " : " + request.getSession().getAttribute("dst-alias-" + dstIndex).toString() + " (" +  request.getSession().getAttribute("dst-type-name-" + dstIndex).toString() + ")";
							out.println(dstDBName);
							out.println("</td>");
							out.println("<td>");
							out.println("<textarea name=\"map-devices-to-dst-pattern-" + dstIndex +  "\" id=\"map-devices-to-dst-pattern-"  + dstIndex  + "\" rows=\"4\" cols=\"103\" style=\"color:blue;\" title =\"Specify a (Java) regular expression pattern of device names/IDs\">" + properties.get("map-devices-to-dst-pattern-" + dstIndex) + "</textarea>");
							out.println("</td>");
							out.println("</tr>");
						}
					%>
					
					<tr>
						<td>Default Destination DB For Unmapped Devices</td>
						<td>
						<select id="default-dst-index-for-unmapped-devices" name="default-dst-index-for-unmapped-devices"  title="Specify default destination database which should be used to consolidate all unmapped devices">
						<%
							for (int idx = 1 ; idx <= numDestinations ; ++idx) {
								String dstName = "Destination DB " + idx + " : " + request.getSession().getAttribute("dst-alias-" + idx).toString() + " (" +  request.getSession().getAttribute("dst-type-name-" + idx).toString() + ")";
								if (properties.get("default-dst-index-for-unmapped-devices").equals(idx)) {
									out.println("<option value=\"" + idx + "\" selected>" + dstName + "</option>");
								} else {
									out.println("<option value=\"" + idx + "\">" + dstName + "</option>");
								}
							}
						%>
						</select>
						</td>
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