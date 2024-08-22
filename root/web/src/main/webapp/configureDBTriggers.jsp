<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.Map"%>
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

<%
String errorMsg = request.getParameter("errorMsg");
HashMap<String, String> properties = new HashMap<String, String>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

Integer dstIndex = 1;

if (request.getParameter("dstIndex") != null) {
	dstIndex = Integer.valueOf(request.getParameter("dstIndex"));
} else if (request.getParameter("dst-index") != null) {
	dstIndex = Integer.valueOf(request.getParameter("dst-index"));
}

Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());
properties.put("dst-type-" + dstIndex, session.getAttribute("dst-type-" + dstIndex).toString());
properties.put("dst-type-name-" + dstIndex, session.getAttribute("dst-type-name-" + dstIndex).toString());
properties.put("dst-alias-" + dstIndex, session.getAttribute("dst-alias-" + dstIndex).toString());

if (request.getParameter("dst-enable-triggers-" + dstIndex) != null) {
	properties.put("dst-enable-triggers-" + dstIndex, request.getParameter("dst-enable-triggers-" + dstIndex));
} else {
	properties.put("dst-enable-triggers-" + dstIndex, "false");
}

if (request.getParameter("dst-triggers-" + dstIndex) != null) {
	properties.put("dst-triggers-" + dstIndex, request.getParameter("dst-triggers-" + dstIndex));
} else {
	properties.put("dst-triggers-" + dstIndex, "");
}

if (request.getParameter("dst-triggers-" + dstIndex) == null) {
	//Read configs from syncJob.props if they are present

	Path propsPath = Path.of(properties.get("device-data-root").toString(), "synclite_consolidator.conf");
	BufferedReader reader = null;
	try {
		if (Files.exists(propsPath)) {
	reader = new BufferedReader(new FileReader(propsPath.toFile()));
	String line = reader.readLine();
	while (line != null) {
		line = line.trim();
		if (line.trim().isEmpty()) {
			line = reader.readLine();
			continue;
		}
		if (line.startsWith("#")) {
			line = reader.readLine();
			continue;
		}
		String[] tokens = line.split("=");
		if (tokens.length < 2) {
			if (tokens.length == 1) {
				if (!line.startsWith("=")) {
					properties.put(tokens[0].trim().toLowerCase(),
							line.substring(line.indexOf("=") + 1, line.length()).trim());
				}
			}
		} else {
			properties.put(tokens[0].trim().toLowerCase(),
					line.substring(line.indexOf("=") + 1, line.length()).trim());
		}
		line = reader.readLine();
	}
	reader.close();
		}
	} catch (Exception e) {
		if (reader != null) {
	reader.close();
		}
		throw e;
	}

	int idx = 0;
	if (properties.get("dst-triggers-file-" + dstIndex) != null) {
		Path triggersFile = Path.of(properties.get("dst-triggers-file-" + dstIndex).toString());
		if (Files.exists(triggersFile)) {
			String valueMappings = Files.readString(triggersFile);
			properties.put("dst-triggers-" + dstIndex, valueMappings);
		}
	}
}

String triggerPlaceHolderText = "{\n" +
"  \"tables\": [\n" +
"    {\n" +
"      \"dst_table_name\": \"dst_tab1\",\n" +
"      \"trigger_statements\": [\n" +
"        \"UPDATE dst_tab1 SET col1 = 'dst_value_1' WHERE col2 = 'src_value_3';\",\n" +
"        \"UPDATE dst_tab1 SET col2 = 'dst_value_3' WHERE col1 = 'src_value_1';\"\n" +
"      ]\n" +
"    },\n" +
"    {\n" +
"      \"dst_table_name\": \"dst_tab2\",\n" +
"      \"trigger_statements\": [\n" +
"        \"DELETE FROM dst_tab2 WHERE col1 = 'src_value_a';\",\n" +
"        \"INSERT INTO dst_tab3 (col1) SELECT col1 FROM dst_tab2 WHERE col1 = 'src_value_b';\"\n" +
"      ]\n" +
"    }\n" +
"  ]\n" +
"}";



triggerPlaceHolderText = triggerPlaceHolderText.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
		.replace("\"", "&quot;").replace("'", "&#39;");

String disabledStr = "";
if (properties.get("dst-enable-triggers-" + dstIndex).equals("false")) {
	disabledStr = "disabled";
}
%>

<script type="text/javascript">
function resetFields() {
    var elementIdToKeep = "dst-enable-triggers-<%=dstIndex%>";
    var allTextInputs = document.querySelectorAll("input[type='text'], textarea");

    for (var i = 0; i < allTextInputs.length; i++) {
        var currentElement = allTextInputs[i];
        if (currentElement.id !== elementIdToKeep) {
            currentElement.parentNode.removeChild(currentElement);
        }
    }
}
</script>
<title>Configure Trigger Statements <%=dstIndex%>></title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<%	
		if (numDestinations == 1) {
			out.println("<h2>Configure Trigger Statements</h2>");
		} else {
			out.println("<h2>Configure Trigger Statements (Destination DB " + dstIndex + " : " + properties.get("dst-alias-" + dstIndex) + ")</h2>");
		}		
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>
		<form action="${pageContext.request.contextPath}/validateDBTriggers" method="post">
			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
		
			<table>
				<tbody>
					<tr>
						<td>Enable Triggers</td>
						<td>
							<select id="dst-enable-triggers-<%=dstIndex%>" name="dst-enable-triggers-<%=dstIndex%>"  onchange="this.form.action='configureDBTriggers.jsp'; resetFields(); this.form.submit();"   title="Specify if values read from source need to be mapped to different values at destination">
							<%
								if (properties.get("dst-enable-triggers-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-enable-triggers-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
							%>
							</select>
						</td>
					</tr>
					<tr>
						<td>Triggers</td>
						<td><textarea name="dst-triggers-<%=dstIndex%>" id="dst-triggers-<%=dstIndex%>" value="<%=properties.get("dst-triggers-" + dstIndex)%>" 
						rows="27" cols="80" placeholder="<%=triggerPlaceHolderText%>"  title="Specify trigger statements per destination table in JSON format. SyncLite Consolidator will invoke trigger statements on each table as part of the same database transaction which is used to consolidate data into a destination database." <%=disabledStr%>><%=properties.get("dst-triggers-" + dstIndex)%></textarea></td>
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