<%@page import="java.nio.file.Path"%>
<%@page import="java.nio.file.Files"%>

<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>SyncLite Consolidator Job Trace</title>
</head>

<body">
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Edit SyncLite Consolidator Configurations</h2>
		<%		
			String errorMsg = request.getParameter("errorMsg");	
			if (errorMsg != null) {
				out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
			}

			if (session.getAttribute("device-data-root") == null) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load SyncLite consolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}
			
			String configurations = "";
			Path confFile = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator.conf");
			try { 
				configurations = Files.readString(confFile);
			} catch(Exception e) {
				out.println("<h4 style=\"color: red;\"> Failed to load configuration file : " +  confFile +"</h4>");
				throw new javax.servlet.jsp.SkipPageException();
			}
		%>

		<form name="confForm" method="post" action="editConfigurations">
			<table>
			<tr>
			<td>
				Notes:<br>
				1. Please stop the running job before editing configurations.<br>
				2. It is recommended to use the "Configure and Start" workflow for editing configurations. This page is only to facilitate quickly changing strategic configurations.<br>
			</td>
			</tr>
			<tr>
			<td>							
				<div class="pagination">
					Configuration File : <%=confFile.toString() %>
				</div>
			</td>
			</tr>
			<tr>
			<td>
			<textarea name="confs" id ="confs" style="width: 90%; height: 60vh;"><%=configurations.toString()%></textarea>
			</td>
			</tr>
			</table>
			<center>
				<button type="submit" name="save">Save</button>
			</center>
		</form>
	</div>
</body>
</html>