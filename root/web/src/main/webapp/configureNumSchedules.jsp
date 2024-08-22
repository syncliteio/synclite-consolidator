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
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.HashMap"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Configure SyncLite Consolidator Job Scheduler</title>
</head>


<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Configure Consolidator Job Schedules</h2>
		<%
			if ((session.getAttribute("job-status") == null) || (session.getAttribute("device-data-root") == null)) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load consolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}
		
			String deviceDataRoot = session.getAttribute("device-data-root").toString();
			String errorMsg = request.getParameter("errorMsg");

			if (errorMsg != null) {
				out.println("<h4 style=\"color: red;\">Failed to load job : " + errorMsg + "</h4>");
			}

			String numSchedules = "1";
			if (request.getParameter("synclite-consolidator-scheduler-num-schedules") != null) {
				numSchedules = request.getParameter("synclite-consolidator-scheduler-num-schedules");
			} else {
				//Read configs from syncJobs.props if they are present
				Path propsPath = Path.of(deviceDataRoot, "synclite_consolidator_scheduler.conf");
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
								line = reader.readLine();
								continue;
							}
							String tokenName = tokens[0].trim().toLowerCase();
							String tokenValue = line.substring(line.indexOf("=") + 1, line.length()).trim();
							if (tokenName.equals("synclite-consoidator-scheduler-num-schedules")) {
								numSchedules = tokenValue;
								break;
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
	
		<form action="${pageContext.request.contextPath}/validateNumSchedules" method="post">
			<table>
				<tbody>
					<tr>
						<td>Number of Schedules</td>
						<td><input type="number" size=5 id="synclite-consolidator-scheduler-num-schedules"
							name="synclite-consolidator-scheduler-num-schedules"
							value="<%=numSchedules%>"
							title="Specify number of schedules"/>
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