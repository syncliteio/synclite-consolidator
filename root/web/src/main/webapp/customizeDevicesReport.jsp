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

<%@page import="java.util.HashMap"%>

<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Customize Devices Report</title>
</head>
<%
	String errorMsg = null;
	HashMap checkStatus = new HashMap<String, String>();
	checkStatus.put("true", "checked");
	checkStatus.put("false", "");
	boolean atLeastOneSelected = false;
	if (request.getParameter("select-fields") != null) {
		if (request.getParameter("show-devices-uuid") != null) {
			session.setAttribute("show-devices-uuid", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-uuid", "false");
		}

		if (request.getParameter("show-devices-name") != null) {				
			session.setAttribute("show-devices-name", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-name", "false");
		}
		
		if (request.getParameter("show-devices-type") != null) {
			session.setAttribute("show-devices-type", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-type", "false");
		}

		if (request.getParameter("show-devices-status") != null) {				
			session.setAttribute("show-devices-status", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-status", "false");	
		}
		
		if (request.getParameter("show-devices-dbalias") != null) {
			session.setAttribute("show-devices-dbalias", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-dbalias", "false");
		}

		if (request.getParameter("show-devices-applied-log-segments") != null) {
			session.setAttribute("show-devices-applied-log-segments", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-applied-log-segments", "false");
		}

		if (request.getParameter("show-devices-processed-log-size") != null) {
			session.setAttribute("show-devices-processed-log-size", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-processed-log-size", "false");
		}

		if (request.getParameter("show-devices-processed-oper-count") != null) {
			session.setAttribute("show-devices-processed-oper-count", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-processed-oper-count", "false");
		}

		if (request.getParameter("show-devices-processed-txn-count") != null) {
			session.setAttribute("show-devices-processed-txn-count", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-processed-txn-count", "false");
		}

		if (request.getParameter("show-devices-latency") != null) {
			session.setAttribute("show-devices-latency", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-latency", "false");
		}				

		if (request.getParameter("show-devices-last-consolidated-commit-id") != null) {
			session.setAttribute("show-devices-last-consolidated-commit-id", "true");
			atLeastOneSelected = true;
		} else {
			session.setAttribute("show-devices-last-consolidated-commit-id", "false");
		}				

		if (atLeastOneSelected) {
			out.println("Redirecting");
			RequestDispatcher rd = request.getRequestDispatcher("devices.jsp");
			rd.forward(request, response);
		} else {
			errorMsg = "Please select at least one field.";
			session.setAttribute("show-devices-uuid", "true");
			session.setAttribute("show-devices-name", "true");
			if ((request.getSession().getAttribute("src-app-type") != null) && !request.getSession().getAttribute("src-app-type").equals("SYNCLITE-DBREADER")) {
				session.setAttribute("show-devices-type", "true");
			} else {
				session.setAttribute("show-devices-type", "false");
			}
			session.setAttribute("show-devices-status", "true");
			session.setAttribute("show-devices-dbalias", "true");
			session.setAttribute("show-devices-applied-log-segments", "true");
			session.setAttribute("show-devices-processed-log-size", "false");
			session.setAttribute("show-devices-processed-oper-count", "false");
			session.setAttribute("show-devices-processed-txn-count", "false");
			session.setAttribute("show-devices-latency", "true");
			session.setAttribute("show-devices-last-consolidated-commit-id", "true");
		}
	} else {
		if (session.getAttribute("show-devices-uuid") == null) {
			session.setAttribute("show-devices-uuid", "true");
		} else {
			session.setAttribute("show-devices-uuid", session.getAttribute("show-devices-uuid").toString());
		}

		if (session.getAttribute("show-devices-name") == null) {				
			session.setAttribute("show-devices-name", "true");
		} else {
			session.setAttribute("show-devices-name", session.getAttribute("show-devices-name").toString());
		}
		
		if (session.getAttribute("show-devices-type") == null) {
			session.setAttribute("show-devices-type", "true");
		} else {
			session.setAttribute("show-devices-type", session.getAttribute("show-devices-type").toString());
		}

		if (session.getAttribute("show-devices-status") == null) {				
			session.setAttribute("show-devices-status", "true");
		} else {
			session.setAttribute("show-devices-status", session.getAttribute("show-devices-status").toString());	
		}
		
		if (session.getAttribute("show-devices-dbalias") == null) {
			session.setAttribute("show-devices-dbalias", "true");
		} else {
			session.setAttribute("show-devices-dbalias", session.getAttribute("show-devices-dbalias").toString());
		}

		if (session.getAttribute("show-devices-applied-log-segments") == null) {
			session.setAttribute("show-devices-applied-log-segments", "true");
		} else {
			session.setAttribute("show-devices-applied-log-segments", session.getAttribute("show-devices-applied-log-segments").toString());
		}

		if (session.getAttribute("show-devices-processed-log-size") == null) {
			session.setAttribute("show-devices-processed-log-size", "false");
		} else {
			session.setAttribute("show-devices-processed-log-size", session.getAttribute("show-devices-processed-log-size").toString());
		}

		if (session.getAttribute("show-devices-processed-oper-count") == null) {
			session.setAttribute("show-devices-processed-oper-count", "false");
		} else {
			session.setAttribute("show-devices-processed-oper-count", session.getAttribute("show-devices-processed-oper-count").toString());
		}

		if (session.getAttribute("show-devices-processed-txn-count") == null) {
			session.setAttribute("show-devices-processed-txn-count", "false");
		} else {
			session.setAttribute("show-devices-processed-txn-count", session.getAttribute("show-devices-processed-txn-count").toString());
		}

		if (session.getAttribute("show-devices-latency") == null) {
			session.setAttribute("show-devices-latency", "true");
		} else {
			session.setAttribute("show-devices-latency", session.getAttribute("show-devices-latency").toString());
		}

		if (session.getAttribute("show-devices-last-consolidated-commit-id") == null) {
			session.setAttribute("show-devices-last-consolidated-commit-id", "true");
		} else {
			session.setAttribute("show-devices-last-consolidated-commit-id", session.getAttribute("show-devices-last-consolidated-commit-id").toString());
		}

	}
%>		

<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Customize Devices Report</h2>
		<% 
			if (errorMsg != null) {
				out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
			}
		%>
		<form name="customizeDeviceForm" id="customizeDeviceForm" method="post">	
			<table>
				<tr>
					<td>
						Select fields to include in Devices report
					</td>
				</tr>
				<tr>
					<td>
						Device UUID				
					</td>
					<td>
						<input type="checkbox" name="show-devices-uuid" value="<%=session.getAttribute("show-devices-uuid").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-uuid"))%>>					
					</td>			
				</tr>
				<tr>
					<td>
						Device Name				
					</td>
					<td>
						<input type="checkbox" name="show-devices-name" value="<%=session.getAttribute("show-devices-name").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-name"))%>>		
					</td>
				</tr>
				<tr>
					<td>
						Device Type				
					</td>
					<td>
						<input type="checkbox" name="show-devices-type" value="<%=session.getAttribute("show-devices-type").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-type"))%>>		
					</td>			
				</tr>
				<tr>
					<td>
						Device Status				
					</td>
					<td>
						<input type="checkbox" name="show-devices-status" value="<%=session.getAttribute("show-devices-status").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-status"))%>>
					</td>			
				</tr>
				<tr>
					<td>
						Destination DB Alias				
					</td>
					<td>
						<input type="checkbox" name="show-devices-dbalias" value="<%=session.getAttribute("show-devices-dbalias").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-dbalias"))%>>
					</td>			
				</tr>
				<tr>
					<td>
						Applied Log Segments				
					</td>
					<td>
						<input type="checkbox" name="show-devices-applied-log-segments" value="<%=session.getAttribute("show-devices-applied-log-segments").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-applied-log-segments"))%>>
					</td>
				</tr>
				<tr>
					<td>
						Processed Log Size				
					</td>
					<td>
						<input type="checkbox" name="show-devices-processed-log-size" value="<%=session.getAttribute("show-devices-processed-log-size").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-processed-log-size"))%>>
					</td>
				</tr>
				<tr>
					<td>
						Processed Operation Count				
					</td>
					<td>
						<input type="checkbox" name="show-devices-processed-oper-count" value="<%=session.getAttribute("show-devices-processed-oper-count").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-processed-oper-count"))%>>
					</td>
				</tr>
				<tr>
					<td>
						Processed Transaction Count				
					</td>
					<td>
						<input type="checkbox" name="show-devices-processed-txn-count" value="<%=session.getAttribute("show-devices-processed-txn-count").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-processed-txn-count"))%>>
					</td>
				</tr>
				<tr>
					<td>
						Device Consolidation Latency
					</td>
					<td>
						<input type="checkbox" name="show-devices-latency" value="<%=session.getAttribute("show-devices-latency").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-latency"))%>>	
					</td>
				</tr>
				<tr>
					<td>
						Last Consolidated SyncLite TS
					</td>
					<td>
						<input type="checkbox" name="show-devices-last-consolidated-commit-id" value="<%=session.getAttribute("show-devices-last-consolidated-commit-id").toString()%>" <%=checkStatus.get(session.getAttribute("show-devices-last-consolidated-commit-id"))%>>	
					</td>
				</tr>

			</table>
			<center>
				<input type="hidden" name="select-fields" id="select-fields" value="select-fields">
				<button type="submit" name="apply" id="apply" value="Apply">Apply</button>
			</center>				
		</form>
	</div>
</body>
</html>