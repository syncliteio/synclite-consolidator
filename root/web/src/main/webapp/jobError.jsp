<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Job Status</title>
</head>
<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Job Status</h2>
		<%
		String jobType = request.getParameter("jobType");
		String errorMsg = request.getParameter("errorMsg");		
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">Failed to execute " + jobType + " job : " + errorMsg + "</h4>");
		}
		%>
	</div>
</body>	
</html>