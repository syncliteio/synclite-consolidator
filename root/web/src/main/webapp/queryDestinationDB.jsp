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
<%@page import= "org.zeromq.SocketType" %>
<%@page import= "org.zeromq.ZMQ" %>
<%@page import= "org.zeromq.ZContext" %>



<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1" name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<script type="text/javascript">
function checkSpace() {
	var workload = document.getElementById("workload").value;
	if ((workload.charAt(workload.length-1) === ' ') || (workload.charAt(workload.length-1) === '\t') || (workload.endsWith('\n'))) {
		highlightSyntax();
	}
}

function highlightSyntax() {
	const sqlKeywords = ['select', 'from', 'where', 'inner', 'outer', 'left', 'right', 'join', 'and', 'or', 'like', 'sum', 'count', 'avg', 'max', 'min', 'order', 'by', 'group', 'having', 'into', 'insert', '<>', '=', '>', '>=' , '<' , '<=', 'like', 'update', 'delete', 'window', 'add', 'constraint', 'all', 'alter', 'column', 'table', 'any', 'as', 'asc', 'desc', 'database', 'schema', 'between', 'case', 'check', 'create', 'index', 'view', 'replace', 'unique', 'constraint', 'default', 'distinct', 'drop', 'exists', 'not', 'foreign', 'key', 'in', 'is', 'null', 'limit', 'top', 'primary', 'rownum', 'set', 'truncate', 'union', 'values', 'on'];
	var workload = document.getElementById("workload").value;
	document.getElementById("workload").innerHTML = "";
	const words = workload.split(" ");
	for (i = 0; i < words.length; i++)
    {
		var wordSpan = document.createElement('span');
		wordSpan.innerHTML = words[i] + ' ';
		var result = sqlKeywords.indexOf(words[i].toLowerCase());
		if (result !== -1) {
			wordSpan.style.color = "blue";			
		} else {
			wordSpan.style.color = "black";			
		}
		document.getElementById("workload").appendChild(wordSpan);
    }

	 //document.getElementById("workload").removeChild(document.getElementById("workload").childNodes[0]);
	//document.getElementById("workload").value = workload;
	alert(document.getElementById("workload").innerHTML);	
}
</script>
<title>Query Destination Database</title>
</head>

<%@include file="html/menu.html"%>

<body>

<%!
	
	public void loadDriver(String dstType) throws Exception {
	
	try {
		switch(dstType) {
		case "DUCKDB":
			Class.forName("org.duckdb.DuckDBDriver");
			break;		
		case "MSSQL":
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			break;
		case "MYSQL":
			Class.forName("com.mysql.cj.jdbc.Driver");
			break;			
		case "POSTGRESQL" :
			Class.forName("org.postgresql.Driver");
			break;
		case "SNOWFLAKE":
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
			break;
		case "SQLITE":
			Class.forName("org.sqlite.JDBC");
			break;
		}	
	} catch (ClassNotFoundException e) {
		throw new Exception("Failed to load appropriate driver for destination database : " + dstType);
	}
	}

	public String getFirstSql(String sql) {
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
	                if (i+1 < inputChars.length) {
	                    if (inputChars[i+1] == '\'') {
	                        currentSqlBuilder.append(inputChars[i+1]);
	                    } else {
	                        currentSqlBuilder.append(inputChars[i+1]);
	                        insideSingleQuotedString = false;
	                    }
	                    ++i;
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
	                    } else {
	                        currentSqlBuilder.append(inputChars[i+1]);
	                        insideDoubleQuotedString = false;
	                    }
	                    ++i;
	                }
	            } else {
	                currentSqlBuilder.append(inputChars[i]);
	            }
	        } else {
	            if (inputChars[i] == ';') {
	                String nextSql = currentSqlBuilder.toString();
	                if (!nextSql.isBlank()) {
	                    return nextSql;
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
	        return nextSql;
	    }
	    return "";
	}
%>

<div class="main">

<%	
	Integer dstIndex = 1;
	if (request.getParameter("dst-index") != null) {
		dstIndex = Integer.valueOf(request.getParameter("dst-index"));
	}
	Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());

	if (session.getAttribute("job-status") == null) {
		out.print("<h2>Query Destination Database</h2>");
		out.println("<h4 style=\"color: red;\"> Please configure and start/load consolidator job.</h4>");
		throw new javax.servlet.jsp.SkipPageException();		
	}

	String dstConnectionStr = null;
	String dstUser = null;
	String dstPassword = null;
	String dstTypeName = null;
	String dstType = null;
	String dstAlias = null;
	String dstDatabase = null;
	String dstSchema = null;
	String placeHolderText = "SELECT * FROM t1";
	if (session.getAttribute("dst-type-" + dstIndex) == null) {
		out.print("<h2>Query Destination Database</h2>");
		out.println("<h4 style=\"color: red;\"> Please configure and start/load a job.</h4>");
		throw new javax.servlet.jsp.SkipPageException();		
	} else {
		dstType = session.getAttribute("dst-type-" + dstIndex).toString();
		dstTypeName = session.getAttribute("dst-type-name-" + dstIndex).toString();
		dstAlias = session.getAttribute("dst-alias-" + dstIndex).toString();
		dstConnectionStr = session.getAttribute("dst-connection-string-" + dstIndex).toString();
		if (session.getAttribute("dst-user-" + dstIndex) != null) {
			dstUser = session.getAttribute("dst-user-" + dstIndex).toString();
		}
		if (session.getAttribute("dst-password-" + dstIndex) != null) {
			dstPassword = session.getAttribute("dst-password-" + dstIndex).toString();
		}

		String placeHolderTablePrefix = "";
		if (session.getAttribute("dst-database-" + dstIndex) != null) {
			dstDatabase = session.getAttribute("dst-database-" + dstIndex).toString();
			placeHolderTablePrefix = dstDatabase + ".";
		}
		if (session.getAttribute("dst-schema-" + dstIndex) != null) {
			dstSchema = session.getAttribute("dst-schema-" + dstIndex).toString();
			placeHolderTablePrefix = placeHolderTablePrefix + dstSchema + ".";
		}
		placeHolderTablePrefix = placeHolderTablePrefix + "t1";
		placeHolderText = "SELECT * FROM " + placeHolderTablePrefix;
	}

	String runStatus = "";
	String runStatusDetails = "";
	String workload = "";
	String sql = "";

	if (request.getParameter("run") != null) {	
		if (request.getParameter("workload") != null) {
			workload = request.getParameter("workload").toString();
			sql = getFirstSql(workload);
		}	
		if (sql.trim().isEmpty()) {
			runStatus = "FAILED";
			runStatusDetails = "Please specify a valid SQL query";
		} else {
			if (!sql.trim().toUpperCase().startsWith("SELECT")) {
				runStatus = "FAILED";
				runStatusDetails = "Please specify a valid SQL query which starts with SELECT";
			}
		}
	}


	//out.println("status : " + runStatus);
	if (runStatus != null) {
		if (runStatus.equals("SUCCESS")) {
			out.println("<h4 style=\"color: blue;\"> SUCCESS </h4>");
		} else if (runStatus.equals("FAILED")) {
			out.println("<h4 style=\"color: red;\"> Query execution failed with error : "
			+ runStatusDetails.replace("<", "&lt;").replace(">", "&gt;") + "</h4>");
		}
	}

	if (numDestinations > 1) {
		out.println("<h2>Query Destination Database " + dstIndex + " : " + dstAlias + " ("+ dstTypeName + ")</h2>");
	} else {
		out.println("<h2>Query Destination Database : " + dstAlias + " ("+ dstTypeName + ")</h2>");
	}
	
%>

		<form name="queryForm" id="queryForm" method="get">
			<input type="hidden" id ="dst-index" name="dst-index" value="<%=dstIndex%>">
			<table>
				<tbody>
					<tr>
						<td>Query</td>
						<td><textarea name="workload" id="workload" rows="4" cols="103" placeholder="<%=placeHolderText%>" style="color:blue;"><%=sql%></textarea></td>
					</tr>
							<%
							long rowCnt = 0;
							if (!sql.trim().isEmpty()) {
								if (!dstType.equals("DUCKDB")) {
									loadDriver(dstType);
									Properties cfg = new Properties();								
									if (dstType.equals("SQLITE")) {
										cfg.setProperty("open_mode", "1");  //1 == readonly								
									}
									
									if (dstUser != null) {
										cfg.setProperty("user", dstUser);
										cfg.setProperty("password", dstPassword);
									}
									try (Connection conn = DriverManager.getConnection(dstConnectionStr, cfg)) {
										if (!dstType.equals("SQLITE") && !dstType.equals("DUCKDB")) {
											conn.setReadOnly(true);
										}
										try (Statement stmt = conn.createStatement()) {
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
										}
								
									} catch (Exception e) {
										runStatus = "FAILED";
										runStatusDetails = "Query execution failed with exception : " + e.getMessage();
									}									
									
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

								} else {
									//For DuckDB, send a request to core as two processes cannot access DuckDB
									
								      try (ZContext context = new ZContext()) {								
							      		//  Socket to talk to server
							            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
										// Set the ZAP_DOMAIN option to accept requests only from localhost
								        socket.setZAPDomain("tcp://localhost");
							      		int port = Integer.valueOf(session.getAttribute("dst-duckdb-reader-port-" + dstIndex).toString());
							            socket.connect("tcp://localhost:" + port);
										String req = "GetHTMLOutput " + sql;
						                socket.send(req.getBytes(ZMQ.CHARSET), 0);
						
						                byte[] reply = socket.recv(0);
						                out.print(new String(reply, ZMQ.CHARSET));
						                socket.close();
							        }							
								}
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
