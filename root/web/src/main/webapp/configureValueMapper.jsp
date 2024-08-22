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

if (request.getParameter("dst-enable-value-mapper-" + dstIndex) != null) {
	properties.put("dst-enable-value-mapper-" + dstIndex, request.getParameter("dst-enable-value-mapper-" + dstIndex));
} else {
	properties.put("dst-enable-value-mapper-" + dstIndex, "false");
}

if (request.getParameter("dst-value-mappings-" + dstIndex) != null) {
	properties.put("dst-value-mappings-" + dstIndex, request.getParameter("dst-value-mappings-" + dstIndex));
} else {
	properties.put("dst-value-mappings-" + dstIndex, "");
}

if (request.getParameter("dst-enable-value-mapper-" + dstIndex) == null) {
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
	if (properties.get("dst-value-mappings-file-" + dstIndex) != null) {
		Path valueMappingsFile = Path.of(properties.get("dst-value-mappings-file-" + dstIndex).toString());
		if (Files.exists(valueMappingsFile)) {
			String valueMappings = Files.readString(valueMappingsFile);
			properties.put("dst-value-mappings-" + dstIndex, valueMappings);
		}
	}
}

String valueMapperPlaceHolderText = "{\n" + "  \"tables\": [\n" + "    {\n" + "      \"src_table_name\": \"tab1\",\n"
		+ "      \"columns\": [\n" + "        {\n" + "          \"src_column_name\": \"col1\",\n"
		+ "          \"value_mappings\": {\n" + "            \"src_value_1\": \"dst_value_1\",\n"
		+ "            \"src_value_2\": \"dst_value_2\"\n" + "          }\n" + "        },\n" + "        {\n"
		+ "          \"src_column_name\": \"col2\",\n" + "          \"value_mappings\": {\n"
		+ "            \"src_value_3\": \"dst_value_3\",\n" + "            \"src_value_4\": \"dst_value_4\"\n"
		+ "          }\n" + "        }\n" + "      ]\n" + "    },\n" + "    {\n" + "      \"src_table_name\": \"tab2\",\n"
		+ "      \"columns\": [\n" + "        {\n" + "          \"src_column_name\": \"col1\",\n"
		+ "          \"value_mappings\": {\n" + "            \"src_value_a\": \"dst_value_a\",\n"
		+ "            \"src_value_b\": \"dst_value_b\"\n" + "          }\n" + "        }\n" + "      ]\n" + "    }\n"
		+ "  ]\n" + "}";

valueMapperPlaceHolderText = valueMapperPlaceHolderText.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
		.replace("\"", "&quot;").replace("'", "&#39;");

String disabledStr = "";
if (properties.get("dst-enable-value-mapper-" + dstIndex).equals("false")) {
	disabledStr = "disabled";
}
%>

<script type="text/javascript">
function resetFields() {
    var elementIdToKeep = "dst-enable-value-mapper-<%=dstIndex%>";
    var allTextInputs = document.querySelectorAll("input[type='text'], textarea");

    for (var i = 0; i < allTextInputs.length; i++) {
        var currentElement = allTextInputs[i];
        if (currentElement.id !== elementIdToKeep) {
            currentElement.parentNode.removeChild(currentElement);
        }
    }
}
</script>
<title>Configure Value Mapper <%=dstIndex%>></title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<%	
		if (numDestinations == 1) {
			out.println("<h2>Configure Value Mapper </h2>");
		} else {
			out.println("<h2>Configure Value Mapper (Destination DB " + dstIndex + " : " + properties.get("dst-alias-" + dstIndex) + ")</h2>");
		}		
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>
		<form action="${pageContext.request.contextPath}/validateValueMapper" method="post">
			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
		
			<table>
				<tbody>
					<tr>
						<td>Enable Value Mapper</td>
						<td>
							<select id="dst-enable-value-mapper-<%=dstIndex%>" name="dst-enable-value-mapper-<%=dstIndex%>"  onchange="this.form.action='configureValueMapper.jsp'; resetFields(); this.form.submit();"   title="Specify if values read from source need to be mapped to different values at destination">
							<%
								if (properties.get("dst-enable-value-mapper-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-enable-value-mapper-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
							%>
							</select>
						</td>
					</tr>
					<tr>
						<td>Value Mappings</td>
						<td><textarea name="dst-value-mappings-<%=dstIndex%>" id="dst-value-mappings-<%=dstIndex%>" value="<%=properties.get("dst-value-mappings-" + dstIndex)%>" 
						rows="27" cols="80" placeholder="<%=valueMapperPlaceHolderText%>"  title="Specify value mappings in JSON format" <%=disabledStr%>><%=properties.get("dst-value-mappings-" + dstIndex)%></textarea></td>
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