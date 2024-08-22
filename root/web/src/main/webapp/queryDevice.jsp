<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.io.InputStreamReader"%>
<%@page import="java.io.FileWriter"%>
<%@page import="org.sqlite.*"%>
<%@page import="java.util.ArrayList"%>
<%@page import="java.util.List"%>
<%@page import="java.util.Properties"%>


<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Query SyncLite Device Replica</title>
</head>

	<%@include file="html/menu.html"%>

<body>

<%!
 public final String getFirstSelectSql(String sql) {
	StringBuilder currentSqlBuilder = new StringBuilder();
	char[] inputChars = sql.toCharArray();
	boolean insideSingleQuotedString = false;
	boolean insideDoubleQuotedString = false;
	for (int i=0; i < inputChars.length; ++i) {
		if (insideSingleQuotedString) {
			if (inputChars[i] == '\'') {
				//Check the next char to set if this is end of single quoted string
				//or is an escape char for a single quote
				currentSqlBuilder.append(inputChars[i]);
				if ((i+1) < inputChars.length) {
					if (inputChars[i+1] == '\'') {
						currentSqlBuilder.append(inputChars[i+1]);
						++i;
					} else {
						insideSingleQuotedString = false;
					}
				}
			} else {
				currentSqlBuilder.append(inputChars[i]);
			}
		} else if (insideDoubleQuotedString) {
			if (inputChars[i] == '\"') {
				//Check the next char to set if this is end of single quoted string
				//or is an escape char for a single quote
				currentSqlBuilder.append(inputChars[i]);
				if (i+1 < inputChars.length) {
					if (inputChars[i+1] == '\"') {
						currentSqlBuilder.append(inputChars[i+1]);
						++i;
					} else {
						insideDoubleQuotedString = false;
					}
				}
			} else {
				currentSqlBuilder.append(inputChars[i]);
			}
		} else {
			if (inputChars[i] == ';') {
				String nextSql = currentSqlBuilder.toString();
				if (!nextSql.isBlank()) {					
					if (nextSql.trim().toLowerCase().startsWith("select")) {
						return nextSql;	
					} else {
						return null;
					}					
				}
				currentSqlBuilder = new StringBuilder();
			} else {
				currentSqlBuilder.append(inputChars[i]);
				if (inputChars[i] == '\'') {
					insideSingleQuotedString = true;
				} else if (inputChars[i] == '\"') {
					insideDoubleQuotedString = true;
				}
			}
		}
	}

	String nextSql = currentSqlBuilder.toString();
	if (!nextSql.isBlank()) {
		if (nextSql.trim().toLowerCase().startsWith("select")) {
			return nextSql;	
		} else {
			return null;
		}					
	}
	return null;
}

%>
<%

	String runStatus = "";
	String runStatusDetails = "";
	String devicePath = "";
	String workload = "";
	String sql = "";

	if (request.getParameter("devicePath") != null) {
		devicePath = request.getParameter("devicePath").toString();
	}

	if (request.getParameter("run") != null) {	
		if (request.getParameter("workload") != null) {
		
			workload = request.getParameter("workload").toString();
			sql = workload;
		}
		if (devicePath.trim().isEmpty()) {
			runStatus = "FAILED";
			runStatusDetails = "Please specify a valid device path";
		} else if (sql.trim().isEmpty()) {
			runStatus = "FAILED";
			runStatusDetails = "Please specify a valid SQL query";
			
		}
	}
%>

	<div class="main">
		<h2>Query Device Replica</h2>
		<%
		//out.println("status : " + runStatus);
		if (runStatus != null) {
			if (runStatus.equals("SUCCESS")) {
				out.println("<h4 style=\"color: blue;\"> SUCCESS </h4>");
			} else if (runStatus.equals("FAILED")) {
				out.println("<h4 style=\"color: red;\"> Query execution failed with error : "
				+ runStatusDetails.replace("<", "&lt;").replace(">", "&gt;") + "</h4>");
			}
		}
		%>

		<form name="queryForm" id="queryForm" method="get">
			<table>
				<tbody>
					<tr></tr>
					<tr>
						<td>Device Path</td>
						<td><input type="text" size = "105" id="devicePath" name="devicePath"
							value="<%=devicePath%>" /></td>
					</tr>

					<tr>
						<td>Query</td>
						<td><textarea name="workload" id="workload" rows="4" placeholder="SELECT * FROM t1" 
								cols="103" style="color:blue;"><%=sql%></textarea></td>
					</tr>

							<%
							long rowCnt = 0;
							if (!sql.trim().isEmpty()) {
								String url = "jdbc:sqlite:" + devicePath;

								Class.forName("org.sqlite.JDBC");
								Properties cfg = new Properties();
								cfg.setProperty("open_mode", "1");  //1 == readonly
										
								try (Connection conn = DriverManager.getConnection(url, cfg)) {
									try (Statement stmt = conn.createStatement()) {
										//String firstSelectSql = getFirstSelectSql(sql);
										if ((sql != null) && (!sql.isBlank())) {			
											try (ResultSet rs = stmt.executeQuery(sql)) {
												if (rs != null) {												
													out.println("<tr>");
													out.println("<td>Result</td>");
													out.println("<td>(Top 1000 rows)</td>");
													out.println("</tr>");
													out.println("<tr>");
													out.println("<td></td>");
													out.println("<td>");
	
													ResultSetMetaData rsMetadata = rs.getMetaData();
													int colCount = rsMetadata.getColumnCount();
													out.println("<div class=\"container\">");
	
													out.println("<table>");
													out.println("<tbody>");
													  
													out.println("<tr>");
													for (int j = 1; j <= colCount; ++j) {
														String colDisplayName = rsMetadata.getColumnName(j);
														out.println("<th>");
														out.print(colDisplayName);
														out.println("</th>");
													}
													out.println("</tr>");
	
													while (rs.next()) {
														if (rowCnt >= 1000) {
															++rowCnt;
															continue;
														}
														out.println("<tr>");
														for (int k = 1; k <= colCount; ++k) {
															out.println("<td>");
															out.println(rs.getString(k));
															out.println("</td>");
														}
														out.println("</tr>");
														++rowCnt;
													}
													out.println("</tbody>");
													out.println("</table>");
													out.println("</div>");
													out.println("</td>");
													out.println("</tr>");
												}
											}
										} else {
											runStatus = "FAILED";
											runStatusDetails = "No SQL statement specified.";											
										}
									}								
								} catch (Exception e) {
									runStatus = "FAILED";
									runStatusDetails = "Query execution failed with exception : " + e.getMessage();
								}							
							}
							%>
					<%
						if (rowCnt > 0) {
							out.println("<tr>");
							out.println("<td></td>");							
							out.println("<td>");
							out.println(rowCnt + " rows");
							out.println("</td>");
							out.println("</tr>");
						}
						if (runStatus.equals("FAILED")) {
							out.println("<tr>");
							out.println("<td></td>");							
							out.println("<td><h4 style=\"color: red;\">");
							out.println(runStatusDetails);
							out.println("</h4></td>");
							out.println("</tr>");
							
						}
					%>
				</tbody>
			</table>
			<center>
				<input type="submit" id="run" name="run" value="Query">
			</center>
		</form>
	</div>
</body>
</html>