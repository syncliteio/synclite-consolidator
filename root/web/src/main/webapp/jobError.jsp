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