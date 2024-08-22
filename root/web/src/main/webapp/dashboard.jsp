<%@page import="org.apache.commons.io.FileUtils"%>
<%@page import="java.time.ZoneId"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.ZonedDateTime"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.InputStreamReader"%>
<%@page import="java.nio.file.Files"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>SyncLite Consolidator Dashboard</title>

<script type="text/javascript">

function autoRefreshSetTimeout() {
    const refreshInterval = parseInt(document.getElementById("refresh-interval").value);
    
    if (!isNaN(refreshInterval)) {
    	const val = refreshInterval * 1000;
    	if (val === 0) {
    		const timeoutObj = setTimeout("autoRefresh()", 1000);
    		clearTimeout(timeoutObj);    		
    	} else {    		
    		setTimeout("autoRefresh()", val);
    	}
	}	
}

function autoRefresh() {
	document.forms['dashboardForm'].submit();
}

</script>
</head>

<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>	
	<div class="main">
		<h2>SyncLite Consolidator Dashboard</h2>
		<%
			if ((session.getAttribute("job-status") == null) || (session.getAttribute("device-data-root") == null)) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load a consolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}		
		
		
			Path statsFilePath = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator_statistics.db");		
			if (!Files.exists(statsFilePath)) {
				out.println("<h4 style=\"color: red;\"> Statistics file for the consolidator job is missing.</h4>");
				
				Path exceptionFile = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator_exception.trace");
				Path traceFile = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator.trace");						

				out.println("Please click on Dashboard and if the error repeats for more than 10 seconds then check the contents of : <br><br>");
				out.println("Exception file : " + exceptionFile + "<br>");
				out.println("Trace file : " + traceFile + "<br>");

				throw new javax.servlet.jsp.SkipPageException();
			}
		%>

		<center>
			<%-- response.setIntHeader("Refresh", 2); --%>
			<table>
				<tbody>
					<%
						String deviceDataRoot = session.getAttribute("device-data-root").toString();
						int refreshInterval = 5;
						if (request.getParameter("refresh-interval") != null) {
							try {
								refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
							} catch (Exception e) {
								refreshInterval = 5;
							}
						}
						//Get current job PID if running
						long currentJobPID = 0;
						Process jpsProc;
						if (! System.getProperty("os.name").startsWith("Windows")) {
							String javaHome = System.getenv("JAVA_HOME");			
							String scriptPath = "jps";
							if (javaHome != null) {
								scriptPath = javaHome + "/bin/jps";
							} else {
								scriptPath = "jps";
							}
							String[] cmdArray = {scriptPath, "-l", "-m"};
							jpsProc = Runtime.getRuntime().exec(cmdArray);
						} else {
							String javaHome = System.getenv("JAVA_HOME");			
							String scriptPath = "jps";
							if (javaHome != null) {
								scriptPath = javaHome + "\\bin\\jps";
							} else {
								scriptPath = "jps";
							}
							String[] cmdArray = {scriptPath, "-l", "-m"};
							jpsProc = Runtime.getRuntime().exec(cmdArray);
						}
						BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
						String line = stdout.readLine();
						while (line != null) {
							if (line.contains("com.synclite.consolidator.Main") && line.contains(deviceDataRoot)) {
								currentJobPID = Long.valueOf(line.split(" ")[0]);
							}
							line = stdout.readLine();
						}			

	                	Class.forName("org.sqlite.JDBC");
				                
						try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
							try (Statement stat = conn.createStatement()) {
								try (ResultSet rs = stat.executeQuery("select header, detected_devices, registered_devices, initialized_devices, failed_devices, total_consolidated_tables, total_log_segments_applied, total_processed_log_size, total_processed_oper_count, total_processed_txn_count, latency, last_heartbeat_time, last_job_start_time from dashboard;")) {
									if (rs.next()) {
										String jobType = "SYNC";
										if (session.getAttribute("job-status") != null) {
											if (session.getAttribute("job-status").toString().equals("STARTED")) {
												if (session.getAttribute("job-type") != null) {
													jobType = session.getAttribute("job-type").toString();
												}
											}
										}

										long lastHeartbeatTimeMillis = Long.valueOf(rs.getString("last_heartbeat_time"));
										long lastHeartbeatDuration = (System.currentTimeMillis() - lastHeartbeatTimeMillis)
												/ 1000;

										
										String jobName = "UNKNOWN";
										if (session.getAttribute("job-name") != null) {
											jobName = session.getAttribute("job-name").toString();
										}
										String lastKnownJobProcessStatus = "RUNNING";
										if (currentJobPID == 0) {
											lastKnownJobProcessStatus = "STOPPED";
										} else {
											if (jobType.equals("SYNC")) {
												//Job hung checking is only relevant for SYNC job type.
												if (lastHeartbeatDuration > 30) {
													lastKnownJobProcessStatus = "WAITING";
												}
											}
										}
										out.println("<tr>");
										out.println("<td>" + rs.getString("header") + "</td>");
										out.println("<td>");
										out.println(
												"<form name=\"dashboardForm\" method=\"post\" action=\"dashboard.jsp\">");
										out.println("<div class=\"pagination\">");
										out.println("REFRESH IN ");
										out.println(
												"<input type=\"text\" id=\"refresh-interval\" name=\"refresh-interval\" value =\""
														+ refreshInterval
														+ "\" size=\"1\" onchange=\"autoRefreshSetTimeout()\">");
										out.println(" SECONDS");
										out.println("</div>");
										out.println("</form>");
										out.println("</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Consolidator Job Name </td>");
										out.println("<td>" + jobName + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Job Process Status </td>");
										out.println("<td><a href=\"jobTrace.jsp\">" + lastKnownJobProcessStatus + "</a></td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Job Process ID </td>");
										out.println("<td>" + currentJobPID + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Job Type </td>");
										out.println("<td>" + jobType + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Dectected Devices </td>");
										out.println("<td><a href=\"devices.jsp\">" + rs.getString("detected_devices")
												+ "</a></td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Registered Devices </td>");
										out.println("<td>" + rs.getString("registered_devices") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Initialized Devices </td>");
										out.println("<td>" + rs.getString("initialized_devices") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Failed Devices </td>");
										out.println("<td>" + rs.getString("failed_devices") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Total Consolidated Tables </td>");
										out.println("<td>" + rs.getString("total_consolidated_tables") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Total Log Segments Applied</td>");
										out.println("<td>" + rs.getString("total_log_segments_applied") + "</td>");
										out.println("</tr>");
										out.println("<td> Total Processed Log Size </td>");
										String sizeStr = FileUtils
												.byteCountToDisplaySize(rs.getLong("total_processed_log_size"));
										out.println("<td>" + sizeStr + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Total Processed Operation Count</td>");
										out.println("<td>" + rs.getString("total_processed_oper_count") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Total Processed Transaction Count</td>");
										out.println("<td>" + rs.getString("total_processed_txn_count") + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Latency</td>");
										out.println("<td>" + Double.valueOf(rs.getString("latency")) / 1000.0
												+ " Seconds</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Job Elapsed Time </td>");
										String elapsedTimeStr = "0 Seconds";
										if (lastKnownJobProcessStatus.equals("RUNNING")) {								
											long jobStartTime = rs.getLong("last_job_start_time");
											long elapsedTime = (System.currentTimeMillis() - jobStartTime) / 1000L;
											long elapsedTimeDays = 0L;
											long elapsedTimeHours = 0L;
											long elapsedTimeMinutes = 0L;
											long elapsedTimeSeconds = 0L;

											elapsedTimeStr = "";
											if (elapsedTime > 86400) {
												elapsedTimeDays = elapsedTime / 86400;
												if (elapsedTimeDays > 0) {
													if (elapsedTimeDays == 1) {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeDays + " Day ";
													} else {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeDays + " Days ";
													}
												}
											}

											if (elapsedTime > 3600) {
												elapsedTimeHours = (elapsedTime % 86400) / 3600;
												if (elapsedTimeHours > 0) {
													if (elapsedTimeHours == 1) {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeHours + " Hour ";
													} else {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeHours + " Hours ";
													}
												}
											}

											if (elapsedTime > 60) {
												elapsedTimeMinutes = (elapsedTime % 3600) / 60;
												if (elapsedTimeMinutes > 0) {
													if (elapsedTimeMinutes == 1) {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeMinutes + " Minute ";
													} else {
														elapsedTimeStr = elapsedTimeStr + elapsedTimeMinutes + " Minutes ";
													}

												}
											}
											elapsedTimeSeconds = elapsedTime % 60;
											if (elapsedTimeSeconds == 1) {
												elapsedTimeStr = elapsedTimeStr + elapsedTimeSeconds + " Second";
											} else {
												elapsedTimeStr = elapsedTimeStr + elapsedTimeSeconds + " Seconds";
											}
										}
										out.println("<td>" + elapsedTimeStr + "</td>");
										out.println("</tr>");
										out.println("<tr>");
										out.println("<td> Job Last Heartbeat Time </td>");
										//out.println("<td>" +  </td>");
										String lastHeartbeatTimeStr = LocalDateTime
												.ofInstant(Instant.ofEpochMilli(lastHeartbeatTimeMillis),
														ZoneId.systemDefault())
												.toString();
										lastHeartbeatTimeStr = lastHeartbeatTimeStr.replace("T", " ");
										out.println("<td>" + lastHeartbeatTimeStr + "</td>");
										out.println("</tr>");
									}
								}
							}
						} catch (Exception e) {
							out.println("<h4 style=\"color: red;\">Failed to read consolidator job statistics. Please refresh the page.</h4>");
						}
					%>
				</tbody>
			</table>
		</center>
	</div>
</body>
</html>