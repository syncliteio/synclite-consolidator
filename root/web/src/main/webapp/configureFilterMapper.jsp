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
HashMap<String, Object> properties = new HashMap<String, Object>();

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

if (request.getParameter("dst-enable-filter-mapper-rules-" + dstIndex) != null) {
	properties.put("dst-enable-filter-mapper-rules-" + dstIndex, request.getParameter("dst-enable-filter-mapper-rules-" + dstIndex));
} else {
	properties.put("dst-enable-filter-mapper-rules-" + dstIndex, "false");
}

if (request.getParameter("dst-allow-unspecified-tables-" + dstIndex) != null) {
	properties.put("dst-allow-unspecified-tables-" + dstIndex, request.getParameter("dst-allow-unspecified-tables-" + dstIndex));
} else {
	properties.put("dst-allow-unspecified-tables-" + dstIndex, "true");
}

if (request.getParameter("dst-allow-unspecified-columns-" + dstIndex) != null) {
	properties.put("dst-allow-unspecified-columns-" + dstIndex, request.getParameter("dst-allow-unspecified-columns-" + dstIndex));
} else {
	properties.put("dst-allow-unspecified-columns-" + dstIndex, "true");
}

if (request.getParameter("dst-filter-mapper-rules-" + dstIndex) != null) {
	properties.put("dst-filter-mapper-rules-" + dstIndex, request.getParameter("dst-filter-mapper-rules-" + dstIndex));
} else {
	properties.put("dst-filter-mapper-rules-" + dstIndex, "");
}

if (request.getParameter("dst-enable-filter-mapper-rules-" + dstIndex) == null) {
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
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (!line.startsWith("=")) {								
							properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
						}
					}
				}  else {
					properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				}					
				line = reader.readLine();
			}
			reader.close();
		}
	} catch (Exception e){ 
		if (reader != null) {
			reader.close();
		}
		throw e;
	}
	
	if (properties.get("dst-filter-mapper-rules-file-" + dstIndex) != null) {
		Path filterMapperRulesFilePath = Path.of(properties.get("dst-filter-mapper-rules-file-" + dstIndex).toString());
		if (Files.exists(filterMapperRulesFilePath)) {
			String readFilterMapperRules = Files.readString(filterMapperRulesFilePath);
			if (readFilterMapperRules != null) {
				properties.put("dst-filter-mapper-rules-" + dstIndex, readFilterMapperRules);
			}
		}
	}
}

String filterMapperPlaceHolderText = "src_table1 = true\n" +
		"src_table2 = false\n" +
		"src_table3 = true\n" +
		"src_table3.col1 = true\n" +
		"src_table3.col2 = true\n" + 
		"src_table3.col3 = false\n" +
		"src_table4 = dst_table400\n" +
		"src_table4.col1 = col401\n" +
		"src_table4.col2 = false\n";

String disabledStr = "";
if (properties.get("dst-enable-filter-mapper-rules-" + dstIndex).equals("false")) {
	disabledStr = "disabled";
}
%>

<script type="text/javascript">
function resetFields() {
	var element = document.getElementById("dst-allow-unspecified-tables-<%=dstIndex%>");		
	if (element) {
	  element.parentNode.removeChild(element);
	}
	var element = document.getElementById("dst-allow-unspecified-columns-<%=dstIndex%>");		
	if (element) {
	  element.parentNode.removeChild(element);
	}
	element = document.getElementById("dst-filter-mapper-rules-<%=dstIndex%>");		
	if (element) {
	  element.parentNode.removeChild(element);
	}
}
</script>
<title>Configure Table/Column Filter/Mapper <%=dstIndex%>></title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<%	
		if (numDestinations == 1) {
			out.println("<h2>Configure Table/Column Filter/Mapper </h2>");
		} else {
			out.println("<h2>Configure Table/Column Filter/Mapper (Destination DB " + dstIndex + " : " + properties.get("dst-alias-" + dstIndex) + ")</h2>");
		}		
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>
		<form action="${pageContext.request.contextPath}/validateFilterMapper" method="post">
			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
		
			<table>
				<tbody>
					<tr>
						<td>Enable Table/Column Filter/Mapper Rules</td>
						<td><select id="dst-enable-filter-mapper-rules-<%=dstIndex%>" name="dst-enable-filter-mapper-rules-<%=dstIndex%>"  onchange="this.form.action='configureFilterMapper.jsp'; resetFields(); this.form.submit();" title="Specify if table/column filtering/mapping is to be enabled">
							<%
								if (properties.get("dst-enable-filter-mapper-rules-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-enable-filter-mapper-rules-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
							%>
						</td>	
					</tr>
					<tr>
						<td>Allow Unspecified Tables</td>
						<td><select id="dst-allow-unspecified-tables-<%=dstIndex%>" name="dst-allow-unspecified-tables-<%=dstIndex%>" <%=disabledStr%>>
							<%
								if (properties.get("dst-allow-unspecified-tables-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-allow-unspecified-tables-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
							%>
						</td>	
					</tr>
					<tr>
						<td>Allow Unspecified Columns</td>
						<td><select id="dst-allow-unspecified-columns-<%=dstIndex%>" name="dst-allow-unspecified-columns-<%=dstIndex%>" <%=disabledStr%>>
							<%
								if (properties.get("dst-allow-unspecified-columns-" + dstIndex).equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dst-allow-unspecified-columns-" + dstIndex).equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
							%>
						</td>	
					</tr>
					<tr>
						<td>Specify Filter/Mapper Rules</td>
						<td><textarea name="dst-filter-mapper-rules-<%=dstIndex%>" id="dst-filter-mapper-rules-<%=dstIndex%>" value="<%=properties.get("dst-filter-mapper-rules-" + dstIndex)%>" 
						rows="27" cols="80" placeholder="<%=filterMapperPlaceHolderText%>" title="Specify filter/mapper configurations for tables/columns" <%=disabledStr%>><%=properties.get("dst-filter-mapper-rules-" + dstIndex)%></textarea></td>
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