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
				out.println("<h4 style=\"color: red;\"> Please configure and start/load cosolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}
		
			String deviceDataRoot = session.getAttribute("device-data-root").toString();
			String errorMsg = request.getParameter("errorMsg");
			if (errorMsg != null) {
				out.println("<h4 style=\"color: red;\">Failed to configure SyncLite consolidator job scheduler : " + errorMsg + "</h4>");
			}

			Integer numSchedules = 1;
			if (session.getAttribute("synclite-consolidator-scheduler-num-schedules") != null) {
				numSchedules = Integer.valueOf(session.getAttribute("synclite-consolidator-scheduler-num-schedules").toString());
			}
			
			HashMap<String, String> properties = new HashMap<String, String>();		
			for (int idx = 1; idx <= numSchedules; ++idx) {
				if (request.getParameter("synclite-consolidator-scheduler-start-hour-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-start-hour-" + idx, request.getParameter("synclite-consolidator-scheduler-start-hour-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-start-hour-" + idx, "9");
				}
		
				if (request.getParameter("synclite-consolidator-scheduler-start-minute-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-start-minute-" + idx, request.getParameter("synclite-consolidator-scheduler-start-minute-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-start-minute-" + idx, "0");
				}
		
				if (request.getParameter("synclite-consolidator-scheduler-end-hour-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-end-hour-" + idx, request.getParameter("synclite-consolidator-scheduler-end-hour-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-end-hour-" + idx, "18");
				}
		
				if (request.getParameter("synclite-consolidator-scheduler-end-minute-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-end-minute-" + idx, request.getParameter("synclite-consolidator-scheduler-end-minute-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-end-minute-" + idx, "0");
				}
		
				if (request.getParameter("synclite-consolidator-scheduler-job-run-duration-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-job-run-duration-" + idx, request.getParameter("synclite-consolidator-scheduler-job-run-duration-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-job-run-duration-" + idx, "0");
				}

				if (request.getParameter("synclite-consolidator-scheduler-job-run-duration-unit-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-job-run-duration-unit-" + idx, request.getParameter("synclite-consolidator-scheduler-job-run-duration-unit-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-job-run-duration-unit-" + idx, "MINUTES");
				}

				if (request.getParameter("synclite-consolidator-scheduler-job-run-interval-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-job-run-interval-" + idx, request.getParameter("synclite-consolidator-scheduler-job-run-interval-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-job-run-interval-" + idx, "0");
				}

				if (request.getParameter("synclite-consolidator-scheduler-job-run-interval-unit-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-job-run-interval-unit-" + idx, request.getParameter("synclite-consolidator-scheduler-job-run-interval-unit-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-job-run-interval-unit-" + idx, "MINUTES");
				}

				if (request.getParameter("synclite-consolidator-scheduler-job-type-" + idx) != null) {
					properties.put("synclite-consolidator-scheduler-job-type-" + idx, request.getParameter("synclite-consolidator-scheduler-job-type-" + idx));
				} else {
					properties.put("synclite-consolidator-scheduler-job-type-" + idx, "READ");
				}
			}	
			
			if (request.getParameter("synclite-consolidator-scheduler-start-hour-1") == null) {
				//Read configs from conf file if they are present
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
		
		<form action="${pageContext.request.contextPath}/configureScheduler" method="post">
			<table>
			<tr></tr>
				<tr>
					<th>ID</th>
					<th>Start At (HH:MM)</th>
					<th>End At (HH:MM)</th>
					<th>Run Duration</th>
					<th>Run Interval</th>
					<th>Job Type</th>
				</tr>
			<%
				for (int idx = 1; idx <= numSchedules; ++idx) {
					out.println("<tr>");
					out.println("<td>" + idx + "</td>");

					out.println("<td>");
					out.print("<input type=\"text\" size=\"2\" id=\"synclite-consolidator-scheduler-start-hour-" + idx + "\" name=\"synclite-consolidator-scheduler-start-hour-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-start-hour-" + idx) + "\" title=\"Specify SyncLite consolidator scheduler start hour.\"/>");
					out.print(":<input type=\"text\" size=\"2\" id=\"synclite-consolidator-scheduler-start-minute-" + idx + "\" name=\"synclite-consolidator-scheduler-start-minute-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-start-minute-" + idx) + "\" title=\"Specify SyncLite consolidator scheduler start minute.\"/>");				
					out.println("</td>");

					out.println("<td>");
					out.print("<input type=\"text\" size=\"2\" id=\"synclite-consolidator-scheduler-end-hour-" + idx + "\" name=\"synclite-consolidator-scheduler-end-hour-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-end-hour-" + idx) + "\" title=\"Specify SyncLite consolidator scheduler end hour.\"/>");
					out.print(":<input type=\"text\" size=\"2\" id=\"synclite-consolidator-scheduler-end-minute-" + idx + "\" name=\"synclite-consolidator-scheduler-end-minute-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-end-minute-" + idx) + "\" title=\"Specify SyncLite consolidator scheduler end minute.\"/>");				
					out.println("</td>");

					out.println("<td>");
					out.println("<input type=\"text\" size=\"4\" id=\"synclite-consolidator-scheduler-job-run-duration-" + idx + "\" name=\"synclite-consolidator-scheduler-job-run-duration-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-job-run-duration-" + idx) + "\" title=\"Specify SyncLite consolidator job run duration. Value 0 indicates keep the job running once started until stopped.\"/>");
					out.println("<select id=\"synclite-consolidator-scheduler-job-run-duration-unit-" + idx +  "\" name=\"synclite-consolidator-scheduler-job-run-duration-unit-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-job-run-duration-unit-" + idx) + "\" title=\"Select consolidator job run duration unit\">");
					if (properties.get("synclite-consolidator-scheduler-job-run-duration-unit-" + idx).equals("SECONDS")) {
						out.println("<option value=\"SECONDS\" selected>SECONDS</option>");
					} else {
						out.println("<option value=\"SECONDS\">SECONDS</option>");
					}
					if (properties.get("synclite-consolidator-scheduler-job-run-duration-unit-" + idx).equals("MINUTES")) {
						out.println("<option value=\"MINUTES\" selected>MINUTES</option>");
					} else {
						out.println("<option value=\"MINUTES\">MINUTES</option>");
					}
					if (properties.get("synclite-consolidator-scheduler-job-run-duration-unit-" + idx).equals("HOURS")) {
						out.println("<option value=\"HOURS\" selected>HOURS</option>");
					} else {
						out.println("<option value=\"HOURS\">HOURS</option>");
					}
					out.println("</td>");

					out.println("<td>");
					out.println("<input type=\"text\" size=\"4\" id=\"synclite-consolidator-scheduler-job-run-interval-" + idx + "\" name=\"synclite-consolidator-scheduler-job-run-interval-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-job-run-interval-" + idx) + "\" title=\"Specify SyncLite consolidator job run interval. Value 0 indicates no periodic starting of the job.\"/>");
					out.println("<select id=\"synclite-consolidator-scheduler-job-run-interval-unit-" + idx +  "\" name=\"synclite-consolidator-scheduler-job-run-interval-unit-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-job-run-interval-unit-" + idx) + "\" title=\"Select consolidator job run interval unit\">");
					if (properties.get("synclite-consolidator-scheduler-job-run-interval-unit-" + idx).equals("SECONDS")) {
						out.println("<option value=\"SECONDS\" selected>SECONDS</option>");
					} else {
						out.println("<option value=\"SECONDS\">SECONDS</option>");
					}
					if (properties.get("synclite-consolidator-scheduler-job-run-interval-unit-" + idx).equals("MINUTES")) {
						out.println("<option value=\"MINUTES\" selected>MINUTES</option>");
					} else {
						out.println("<option value=\"MINUTES\">MINUTES</option>");
					}
					if (properties.get("synclite-consolidator-scheduler-job-run-interval-unit-" + idx).equals("HOURS")) {
						out.println("<option value=\"HOURS\" selected>HOURS</option>");
					} else {
						out.println("<option value=\"HOURS\">HOURS</option>");
					}
					out.println("</td>");

					out.println("<td>");
					out.println("<select id=\"synclite-consolidator-scheduler-job-type-" + idx +  "\" name=\"synclite-consolidator-scheduler-job-type-" + idx + "\" value=\"" + properties.get("synclite-consolidator-scheduler-job-type-" + idx) + "\" title=\"Select consolidator job type to schedule.\">");
					if (properties.get("synclite-consolidator-scheduler-job-type-" + idx).equals("READ")) {
						out.println("<option value=\"SYNC\" selected>SYNC</option>");
					} else {
						out.println("<option value=\"SYNC\">SYNC</option>");
					}
					out.println("</td>");
					out.println("</tr>");
				}
				%>
			</table>
			
			<center>
				<button type="submit" name="next">Schedule</button>
			</center>			
		</form>
	</div>
</body>
</html>