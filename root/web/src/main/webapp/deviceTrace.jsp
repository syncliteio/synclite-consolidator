<%@page import="java.nio.file.Path"%>
<%@page import="org.apache.commons.io.input.ReversedLinesFileReader"%>
<%@page import="java.nio.charset.Charset"%>
<%@page import="java.util.Stack"%>
<%@page import="java.net.URLEncoder"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

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
	    
	    document.getElementById("tracearea").scrollTop = document.getElementById("tracearea").scrollHeight;
	}

	function autoRefresh() {
		document.forms['traceForm'].submit();
	}

</script>

<%
	String deviceUUID = request.getParameter("uuid");
	String deviceName = request.getParameter("name");
	String devicePath = request.getParameter("path");
	String deviceTraceFileName = "synclite_device.trace";
	String deviceTraceURL = "deviceTrace.jsp?uuid=" + URLEncoder.encode(deviceUUID, Charset.defaultCharset()) + "&name=" + URLEncoder.encode(deviceName, Charset.defaultCharset()) + "&path=" + URLEncoder.encode(devicePath, Charset.defaultCharset());
%>

<title>SyncLite Consolidator Job Trace</title>
</head>
<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>SyncLite Consolidator Device Trace </h2>
		<%
			if (session.getAttribute("job-status") == null) {
					out.println("<h4 style=\"color: red;\"> Please configure and start/load a job.</h4>");
					throw new javax.servlet.jsp.SkipPageException();		
			}
				
			Path deviceTraceFilePath = Path.of(devicePath, deviceTraceFileName);
			
			StringBuilder traces = new StringBuilder();
			Stack<String> lines = new Stack<String>(); 
			ReversedLinesFileReader reader = null;
			try {
				String line;
				int lineCnt = 0;
				reader = new ReversedLinesFileReader(deviceTraceFilePath.toFile(), Charset.defaultCharset()); 
				while (((line = reader.readLine()) != null) && (lineCnt < 100)) {
					lines.push(line);
					++lineCnt;
				}
				reader.close();			
			} catch (Exception e) {
				if (reader != null) {
					reader.close();
				}
				throw e;
			}			
			while (!lines.empty()) {
				traces.append(lines.pop());
				traces.append("\n");
			}			
		%>

		<%
			int refreshInterval = 5;
			if (request.getParameter("refresh-interval") != null) {
				try {
					refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
				} catch (Exception e) {
					refreshInterval = 5;
				}
			}
		%>

		<form name="traceForm" method="post" action="<%=deviceTraceURL%>">
			<input type ="hidden" name="path" id="path" value="<%=devicePath%>">
			<table>
			<tr>
			<td>							
				<div class="pagination">
					Device Trace file : <%=deviceTraceFilePath.toString()%> (last 100 lines) <span style="float:right;"> REFRESH IN 
					<input type="text" id="refresh-interval" name="refresh-interval" value ="<%=refreshInterval%>" size="1" onchange="autoRefreshSetTimeout()">
					SECONDS </span>
				</div>
			</td>
			</tr>
			<tr>
			<td>
			<textarea name="tracearea" id ="tracearea" readonly style="width: 100%; height: 80vh;"><%=traces.toString()%></textarea>
			</td>
			</tr>
		</form>
	</div>
</body>
</html>