<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.io.File"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.List"%>
<%@page import="java.util.ArrayList"%>
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
<title>Reset Consolidator Job</title>
</head>


<%
String errorMsg = request.getParameter("errorMsg");

String keepJobConfiguration = "true";
if (request.getParameter("keep-job-configuration") != null) {
	keepJobConfiguration = request.getParameter("keep-job-configuration");
} 

%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Reset Consolidator Job</h2>
		<%
		if ((session.getAttribute("job-status") == null) || (session.getAttribute("device-data-root") == null)) {
			out.println("<h4 style=\"color: red;\"> Please configure and start/load a Consolidator job.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}

		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/resetJob" method="post">
			<table>
				<tbody>
					<tr>
						<td>
							Please note that resetting a job implies restarting data consolidation from scratch upon reconfiguration and restarting the job.<br>
							Please clean up your staging area manually if using a staging storage apart from Local File System (FS) or Local SFTP.
						</td>
					</tr>

					<tr>
						<td>Keep Job Configuration</td>
						<td><select id="keep-job-configuration" name="keep-job-configuration" value="<%=keepJobConfiguration%>" title="Specify if the job configuration should be preserved.">
								<%
								if (keepJobConfiguration.equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}

								if (keepJobConfiguration.equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>				
			</table>
			<center>
				<button type="submit" name="reset">Reset Job</button>
			</center>
		</form>
	</div>
</body>
</html>