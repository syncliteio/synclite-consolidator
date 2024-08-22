<%@page import="java.nio.file.Files"%>
<%@page import="java.nio.file.Path"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Specify Work Directory</title>
</head>


<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		
		<h2>Select Destination Database</h2>

		<%
		if ((session.getAttribute("job-status") == null) || (session.getAttribute("device-data-root") == null)) {
			out.println("<h4 style=\"color: red;\"> Please configure and start/load a job.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}
		
		String dstIndex = request.getParameter("dst-index");
		if (dstIndex != null) {
			request.setAttribute("dst-index", dstIndex);
			RequestDispatcher rd = request.getRequestDispatcher("queryDestinationDB.jsp");
			rd.forward(request, response);
		}
		%>
	
		<form method="post">
			<table>
				<tbody>
					<tr>
						<td>Destination Database</td>
						<td>
						<td>
						<select id="dst-index" name="dst-index"  title="Select destination database to query">
						<%
							Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());
							for (int idx = 1 ; idx <= numDestinations ; ++idx) {
								String dstName = "Destination DB " + idx + " : " + request.getSession().getAttribute("dst-alias-" + idx).toString() + " (" +  request.getSession().getAttribute("dst-type-name-" + idx).toString() + ")";
								if (idx == 1) {
									out.println("<option value=\"" + idx + "\" selected>" + dstName + "</option>");
								} else {
									out.println("<option value=\"" + idx + "\">" + dstName + "</option>");
								}
							}
						%>
						</select>
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