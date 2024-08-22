<%@page import="java.nio.charset.Charset"%>
<%@page import="java.net.URLEncoder"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.nio.file.Files"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<%@page import="org.apache.commons.io.FileUtils"%>
<%@page import="java.time.Instant"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.ZoneId"%>
<%@page import="java.time.format.DateTimeFormatter"%>

<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<title>SyncLite Consolidator Job Scheduler History</title>
</head>

<body>
	<%@include file="html/menu.html"%>	
	<div class="main">
		<h2>Job Scheduler History</h2>
		<%
			if (session.getAttribute("job-status") == null) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load a consolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();						
			}
			Path statsFilePath = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator_scheduler_statistics.db");		
			if (!Files.exists(statsFilePath)) {
				out.println("<h4 style=\"color: red;\">Job Scheduler not configured.</h4>");
				throw new javax.servlet.jsp.SkipPageException();				
			}

			Long numRecords = 100L;
			if (request.getParameter("numRecords") != null) {
				try {
					numRecords = Long.valueOf(request.getParameter("numRecords").trim());
				} catch (NumberFormatException e) {
					numRecords = 100L;
				}
			}			

			String showScheduleDetails = "false";
			if (request.getParameter("showScheduleDetails") != null) {
				showScheduleDetails = request.getParameter("showScheduleDetails").toString();
			}

			Class.forName("org.sqlite.JDBC");
		%>
			<center>
				<form name="tableForm" id="tableForm" method="post">
					<table>
						<tr>
							<td>
								Showing Most Recent <input type="number" name = "numRecords" id = "numRecords" value = <%= numRecords%>> Records						 
							</td>
							<td>
								Show Schedule Details 
								<select id="showScheduleDetails" name = "showScheduleDetails"  value = <%= showScheduleDetails%>>
								<% 
									if (showScheduleDetails.equals("true")) {
										out.println("<option value=\"true\" selected>true</option>");
									} else {
										out.println("<option value=\"true\">true</option>");
									}
									if (showScheduleDetails.equals("false")) {
										out.println("<option value=\"false\" selected>false</option>");
									} else {
										out.println("<option value=\"false\">false</option>");
									}
								%>	
								</select>						 
							</td>
							<td>				
								<input type="button" name="Go" id="Go" value="Go" onclick = "this.form.submit()">
							</td>
						</tr>
					</table>
					<table>
					<tr></tr>
					<tr>
						<th>ID</th>
						<th>Schedule No</th>
						<% 
							if (showScheduleDetails.equals("true")) {
								out.println("<th>Schedule Start</th>");
								out.println("<th>Schedule End</th>");
								out.println("<th>Job Run Interval(s)</th>");
								out.println("<th>Job Run Duration(s)</th>");
							}
						%>
						<th>Job Start Event</th>
						<th>Start Status</th>
						<th>Job Stop Event</th>
						<th>Stop Status</th>
					</tr>
					<%
						String sql = "select schedule_index, schedule_start_time, schedule_end_time, job_run_interval_s, job_run_duration_s, job_start_time, job_start_status, job_start_status_description, job_stop_time, job_stop_status, job_stop_status_description FROM statistics ORDER BY trigger_id DESC LIMIT " + numRecords;
						long idx = 1;
					    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
						try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
							try (Statement stmt = conn.createStatement()) {
								try (ResultSet rs = stmt.executeQuery(sql)) {
									while (rs.next()) {
										out.println("<tr>");
										out.println("<td>" + idx + "</td>");
										out.println("<td>" + rs.getString("schedule_index") + "</td>");

										if (showScheduleDetails.equals("true")) {
											long scheduleStartTime = Long.valueOf(rs.getString("schedule_start_time"));
											String scheduleStartTimeStr = "";
											if (scheduleStartTime > 0) {
												LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(scheduleStartTime), ZoneId.systemDefault());											 
												scheduleStartTimeStr= localDateTime.format(formatter);
											}										
											out.println("<td>" + scheduleStartTimeStr + "</td>");

											long scheduleEndTime = Long.valueOf(rs.getString("schedule_end_time"));
											String scheduleEndTimeStr = "";
											if (scheduleEndTime > 0) {
												LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(scheduleEndTime), ZoneId.systemDefault());											 
												scheduleEndTimeStr= localDateTime.format(formatter);
											}										
											out.println("<td>" + scheduleEndTimeStr + "</td>");

											out.println("<td>" + rs.getString("job_run_interval_s") + "</td>");
											out.println("<td>" + rs.getString("job_run_duration_s") + "</td>");
										}		
										long jobStartTime = Long.valueOf(rs.getString("job_start_time"));
										String jobStartTimeStr = "";
										if (jobStartTime > 0) {
											LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(jobStartTime), ZoneId.systemDefault());											 
											jobStartTimeStr= localDateTime.format(formatter);
										}
										out.println("<td>" + jobStartTimeStr + "</td>");
										
										String startStatusDesc = rs.getString("job_start_status_description");
										if (startStatusDesc.isBlank()) {
											out.println("<td>" + rs.getString("job_start_status") + "</td>");
										} else {
											out.println("<td>" + rs.getString("job_start_status") + "(" + startStatusDesc + ")" + "</td>");
										}
										
										long jobEndTime = Long.valueOf(rs.getString("job_stop_time"));
										String jobEndTimeStr = "";
										if (jobEndTime > 0) {
											LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(jobEndTime), ZoneId.systemDefault());											 
											jobEndTimeStr= localDateTime.format(formatter);
										}										
										out.println("<td>" + jobEndTimeStr + "</td>");

										String stopStatusDesc = rs.getString("job_stop_status_description");
										if (stopStatusDesc.isBlank()) {
											out.println("<td>" + rs.getString("job_stop_status") + "</td>");
										} else {
											out.println("<td>" + rs.getString("job_stop_status") + "(" + stopStatusDesc + ")" + "</td>");
										}										
										++idx;
									}
								}
							}
						} catch(Exception e) {
							out.println("<h4 style=\"color: red;\">Failed to read scheduler statistics : " + e.getMessage() + ". Please refresh the page.</h4>");
						}
					%>
			</table>			
		</center>
	</div>
</body>
</html>	