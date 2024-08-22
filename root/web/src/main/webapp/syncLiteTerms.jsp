<%@page import="java.nio.file.Files"%>
<%@page import="java.nio.file.Path"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>SyncLite - End User License Agreement</title>
</head>

<%
String syncLiteTerms ="";
if (request.getParameter("synclite-terms") != null) {
	//RequestDispatcher rd = request.getRequestDispatcher("selectWorkDirectory.jsp");
	//rd.forward(request, response);
	response.sendRedirect("selectWorkDirectory.jsp");
} else {
	String libPath = application.getRealPath("/WEB-INF/lib");
	Path termsFilePath = Path.of(libPath, "synclite_end_user_license_agreement.txt");
	syncLiteTerms = Files.readString(termsFilePath);
}
%>

<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>SyncLite - End User License Agreement</h2>
		<form method="post">
			<table>
				<tbody>
					<tr>
						<td><textarea name="synclite-terms" id="workload" rows="40" cols="160" readonly><%=syncLiteTerms%></textarea></td>
					</tr>
				</tbody>
			</table>
			<center>
				<button type="submit" name="agree" id="agree">Agree</button>
			</center>			
		</form>
	</div>
</body>
</html>