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
function onChangePatternType() {
	var manageDevicesPatternType = document.getElementById("manage-devices-pattern-type").value;
	document.getElementById("manage-devices-name-list").disabled = true;
	document.getElementById("manage-devices-name-pattern").disabled = true;
	document.getElementById("manage-devices-id-list").disabled = true;
	document.getElementById("manage-devices-id-pattern").disabled = true;

	if (manageDevicesPatternType.toString() === "DEVICE_NAME_LIST") {
		document.getElementById("manage-devices-name-list").disabled = false;
	} else if (manageDevicesPatternType.toString() === "DEVICE_NAME_PATTERN") {
		document.getElementById("manage-devices-name-pattern").disabled = false;
	} else if (manageDevicesPatternType.toString() === "DEVICE_ID_LIST") {
		document.getElementById("manage-devices-id-list").disabled = false;
	} else if (manageDevicesPatternType.toString() === "DEVICE_ID_PATTERN") {
		document.getElementById("manage-devices-id-pattern").disabled = false;
	}	
}

</script>

<title>Manage SyncLite Devices</title>
</head>
<body onload="onChangePatternType()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Manage Devices</h2>
		<%
		if (session.getAttribute("job-status") == null) {
			out.println("<h4 style=\"color: red;\"> Please configure and start/load a consolidator job.</h4>");
			throw new javax.servlet.jsp.SkipPageException();						
		}

		if (session.getAttribute("syncite-consolidator-job-starter-scheduler") != null) {
			out.println("<h4 style=\"color: red;\"> Please stop the consolidator job scheduler to proceed with this operation.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}

		String errorMsg = request.getParameter("errorMsg");
		String successMsg = request.getParameter("successMsg");
		String manageDevicesPatternType = "DEVICE_NAME_PATTERN";
		String manageDevicesNameList = "";
		String manageDevicesNamePattern = "";
		String manageDevicesIDList = "";
		String manageDevicesIDPattern = "";
		String manageDevicesOperationType = "ENABLE_DEVICES";
		String deviceCommand = "";
		String deviceCommandDetails = "";

		String removeDevicesFromDst = "false";
		String removeDevicesConfirmation = "";
		String enableDeviceCommandHandler = "false";
		
		if (request.getSession().getAttribute("enable-device-command-handler") != null) {
			enableDeviceCommandHandler = request.getSession().getAttribute("enable-device-command-handler").toString();
		}
		
		if (request.getParameter("manage-devices-operation-type") != null) {
			manageDevicesOperationType = request.getParameter("manage-devices-operation-type");
		}

		if (request.getParameter("manage-devices-name-pattern") != null) {
			manageDevicesNamePattern = request.getParameter("manage-devices-name-pattern");
		}

		if (request.getParameter("manage-devices-id-pattern") != null) {
			manageDevicesIDPattern = request.getParameter("manage-devices-id-pattern");
		}

		if (request.getParameter("remove-devices-from-dst") != null) {
			removeDevicesFromDst = request.getParameter("remove-devices-from-dst");
		}

		if (request.getParameter("remove-devices-confirmation") != null) {
			removeDevicesConfirmation = request.getParameter("remove-devices-confirmation");
		}

		if (request.getParameter("manage-devices-pattern-type") != null) {
			manageDevicesPatternType = request.getParameter("manage-devices-pattern-type");
		}

		if (request.getParameter("device-command") != null) {
			deviceCommand = request.getParameter("device-command");
		}

		if (request.getParameter("device-command-details") != null) {
			deviceCommandDetails = request.getParameter("device-command-details");
		}

		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		} else if (successMsg != null) {
			out.println("<h4 style=\"color: blue;\">" + successMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/manageDevices" method="post">
			<table>
				<tbody>
					<tr>
						<td>Device Operation</td>
						<td><select id="manage-devices-operation-type" name="manage-devices-operation-type" onchange="this.form.action='manageDevices.jsp'; this.form.submit();" title="Specify operation type for devices.">
								<%
								if (enableDeviceCommandHandler.equals("true")) {
									if (manageDevicesOperationType.equals("COMMAND_DEVICES")) {
										out.println("<option value=\"COMMAND_DEVICES\" selected>Command Devices</option>");
									} else {
										out.println("<option value=\"COMMAND_DEVICES\">Command Devices</option>");
									}
								}
								if (manageDevicesOperationType.equals("DISABLE_DEVICES")) {
									out.println("<option value=\"DISABLE_DEVICES\" selected>Disable Devices</option>");
								} else {
									out.println("<option value=\"DISABLE_DEVICES\">Disable Devices</option>");
								}								
								if (manageDevicesOperationType.equals("ENABLE_DEVICES")) {
									out.println("<option value=\"ENABLE_DEVICES\" selected>Enable Devices</option>");
								} else {
									out.println("<option value=\"ENABLE_DEVICES\">Enable Devices</option>");
								}								
								if (manageDevicesOperationType.equals("REINITIALIZE_DEVICES")) {
									out.println("<option value=\"REINITIALIZE_DEVICES\" selected>Reinitialize Devices</option>");
								} else {
									out.println("<option value=\"REINITIALIZE_DEVICES\">Reinitialize Devices</option>");
								}
								if (manageDevicesOperationType.equals("REMOVE_DEVICES")) {
									out.println("<option value=\"REMOVE_DEVICES\" selected>Remove Devices</option>");
								} else {
									out.println("<option value=\"REMOVE_DEVICES\">Remove Devices</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Specify Devices To Be Managed By</td>
						<td><select id="manage-devices-pattern-type" name="manage-devices-pattern-type" onchange="onChangePatternType()" title="Specify whether the devices to be operated are specified by device names or device IDs">
								<%
								if (manageDevicesPatternType.equals("DEVICE_NAME_LIST")) {
									out.println("<option value=\"DEVICE_NAME_LIST\" selected>DEVICE_NAME_LIST</option>");
								} else {
									out.println("<option value=\"DEVICE_NAME_LIST\">DEVICE_NAME_LIST</option>");
								}								
								if (manageDevicesPatternType.equals("DEVICE_NAME_PATTERN")) {
									out.println("<option value=\"DEVICE_NAME_PATTERN\" selected>DEVICE_NAME_PATTERN</option>");
								} else {
									out.println("<option value=\"DEVICE_NAME_PATTERN\">DEVICE_NAME_PATTERN</option>");
								}
								if (manageDevicesPatternType.equals("DEVICE_ID_LIST")) {
									out.println("<option value=\"DEVICE_ID_LIST\" selected>DEVICE_ID_LIST</option>");
								} else {
									out.println("<option value=\"DEVICE_ID_LIST\">DEVICE_ID_LIST</option>");
								}								
								if (manageDevicesPatternType.equals("DEVICE_ID_PATTERN")) {
									out.println("<option value=\"DEVICE_ID_PATTERN\" selected>DEVICE_ID_PATTERN</option>");
								} else {
									out.println("<option value=\"DEVICE_ID_PATTERN\">DEVICE_ID_PATTERN</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Device Name List </td>					
						<td>
							<textarea name="manage-devices-name-list" id="manage-devices-name-list" rows="4" cols="103" style="color:blue;" title ="Specify commna separated list of device names to manage devices"><%=manageDevicesNameList%></textarea>
						</td>
					</tr>
					
					<tr>
						<td>Device Name Pattern</td>					
						<td>
							<textarea name="manage-devices-name-pattern" id="manage-devices-name-pattern" rows="4" cols="103" style="color:blue;" title ="Specify a (Java) regular expression pattern of device names to manage devices"><%=manageDevicesNamePattern%></textarea>
						</td>
					</tr>

					<tr>
						<td>Device ID List </td>					
						<td>
							<textarea name="manage-devices-id-list" id="manage-devices-id-list" rows="4" cols="103" style="color:blue;" title ="Specify commna separated list of device IDs to manage devices"><%=manageDevicesIDList%></textarea>
						</td>
					</tr>

					<tr>
						<td>Device ID Pattern</td>
						<td>
							<textarea name="manage-devices-id-pattern" id="manage-devices-id-pattern" rows="4" cols="103" style="color:blue;" title ="Specify a (Java) regular expression pattern of device IDs to manage devices"><%=manageDevicesIDPattern%></textarea>
						</td>
					</tr>

					<%
						if (manageDevicesOperationType.equals("REMOVE_DEVICES")) {
							out.println("<tr>");
							out.println("<td>Cleanup Device Data From Destination DB</td>");
							out.println("<td>");
							out.println("<select id=\"remove-device-from-dst\" name=\"remove-devices-from-dst\" title=\"Specify if the data for the devices to be removed should be cleaned up from destination database.\">");
							if (removeDevicesFromDst.equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}								
							if (removeDevicesFromDst.equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							out.println("</select>");
							out.println("</td>");
							out.println("</tr>");

							out.println("<tr>");
							out.println("<td>Confirm Device Removal</td>");
							out.println("<td>");
							out.println("<input type=\"text\" id =\"remove-devices-confirmation\" name=\"remove-devices-confirmation\" value=\"" + removeDevicesConfirmation + "\" title=\"Type Remove Devices to confirm device remove\">");
							out.println("</td>");
							out.println("</tr>");
						}
					
						if (manageDevicesOperationType.equals("COMMAND_DEVICES")) {
							out.println("<tr>");
							out.println("<td>Device Command</td>");
							out.println("<td>");
							out.println("<input type=\"text\" id =\"device-command\" name=\"device-command\" value=\"" + deviceCommand + "\" title=\"Type device command to send to qualifying devices\">");
							out.println("</td>");
							out.println("</tr>");							

							out.println("<tr>");
							out.println("<td>Device Command Details</td>");
							out.println("<td>");
							out.println("<textarea name=\"device-command-details\" id =\"device-command-details\" rows=\"8\" cols=\"103\" title=\"Enter device command details, these will be put into the command file which will be delivered to qualifying devices.\">" + deviceCommandDetails +"</textarea>");
							out.println("</td>");
							out.println("</tr>");
						}
					%>							
				</tbody>				
			</table>
			<center>
				<button type="submit" name="apply">Apply</button>
			</center>			
		</form>
	</div>
</body>
</html>