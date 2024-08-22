<%@page import="java.nio.charset.Charset"%>
<%@page import="java.net.URLEncoder"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.nio.file.Files"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<%@page import="org.apache.commons.io.FileUtils"%>
<%@page import="java.time.Instant"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.LocalDate"%>
<%@page import="java.time.ZoneId"%>
<%@page import="java.time.format.DateTimeFormatter"%>

<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<%
	String deviceUUID = request.getParameter("uuid");
	String deviceName = request.getParameter("name");
	String devicePath = null;
	String deviceType = null;
%>
<title>SyncLite Device Statistics</title>
</head>
<script type="text/javascript">

	function processSort(sortColumn) {
		if (sortColumn==document.tableForm.sortColumn.value) {
			if (document.tableForm.sortOrder.value=="asc") {
				document.tableForm.sortOrder.value="desc";
			} else {
				document.tableForm.sortOrder.value="asc";
			}
		} else {
			document.tableForm.sortColumn.value=sortColumn;
			document.tableForm.sortOrder.value="asc";
		}
		document.tableForm.submit();
	}
	
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
	}

	function autoRefresh() {
		document.forms['tableForm'].submit();
	}

</script>

<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>	
	<div class="main">
		<h2>Device Statistics</h2>
		<%
			if (session.getAttribute("job-status") == null) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load a consolidator job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();						
			}
			Path statsFilePath = Path.of(session.getAttribute("device-data-root").toString(), "synclite_consolidator_statistics.db");		
			if (!Files.exists(statsFilePath)) {
				out.println("<h4 style=\"color: red;\"> Statistics file for the consolidator job is missing .</h4>");
				throw new javax.servlet.jsp.SkipPageException();				
			}
		%>
		<center>
			<table>
				<tbody>
					<tr></tr>
					<tr>
						<th>UUID</th>
						<th>Name</th>
						<th>Type</th>
						<th>Sync Status</th>
						<th>Description</th>
						<th>Replica Path</th>
						<th>Replica Size</th>
						<th>Destination DB</th>
					</tr>

					<%
					Class.forName("org.sqlite.JDBC");		
					boolean hasResult = false;
					try(Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
						try(Statement stat = conn.createStatement()) {
							try(ResultSet rs = stat.executeQuery("select synclite_device_id, synclite_device_name, synclite_device_type, status, status_description, path, database_name, destination_database_alias, log_segments_applied, processed_log_size, processed_oper_count, processed_txn_count, latency, last_consolidated_commit_id from device_status where synclite_device_id = '" + deviceUUID + "' and synclite_device_name = '" + deviceName + "'")) {
								hasResult = false;
								Path deviceFullPath = null;
								if (rs.next()) {
									hasResult = true;
									String deviceTraceURL = "deviceTrace.jsp?uuid=" + URLEncoder.encode(rs.getString("synclite_device_id"), Charset.defaultCharset()) + "&name=" + URLEncoder.encode(rs.getString("synclite_device_name"), Charset.defaultCharset()) + "&path=" + URLEncoder.encode(rs.getString("path"), Charset.defaultCharset());
									deviceFullPath = Path.of(rs.getString("path"), rs.getString("database_name") + ".synclite.backup");
									String queryDeviceURL = "queryDevice.jsp?devicePath=" + URLEncoder.encode(deviceFullPath.toString());
									out.println("<tr>");
									out.println("<td>" + rs.getString("synclite_device_id") + "</td>");
									out.println("<td>" + rs.getString("synclite_device_name") + "</td>");
									out.println("<td>" + rs.getString("synclite_device_type") + "</td>");
									out.println("<td><a href=\"" + deviceTraceURL + "\">" + rs.getString("status") + "</a></td>");
									out.println("<td>" + rs.getString("status_description") + "</td>");
									out.println("<td><a href=\"" + queryDeviceURL + "\">" + rs.getString("path") + "</a></td>");
									long replicaSize = Files.size(deviceFullPath);
									String replicaSizeStr = FileUtils.byteCountToDisplaySize(replicaSize);
									out.println("<td>" + replicaSizeStr + "</td>");
									out.println("<td>" + rs.getString("destination_database_alias") + "</td>");
									out.println("</tr>");
									devicePath = rs.getString("path");
									deviceType = rs.getString("synclite_device_type");
								}
								out.println("</tbody>");
								out.println("</table>");
								out.println("<table>");
								out.println("<tbody>");
								out.println("<tr></tr>");
								out.println("<tr>");
								out.println("<th>Applied log segments</th>");
								out.println("<th>Processed Log Size</th>");
								out.println("<th>Processed Operation Count</th>");
								out.println("<th>Processed Transaction Count</th>");
								out.println("<th>Last Applied SyncLite TS</th>");
								out.println("<th>Latency(s)</th>");
								out.println("</tr>");

								if (hasResult) {
									out.println("<tr>");
									out.println("<td>" + rs.getString("log_segments_applied") + "</td>");
				                    String sizeStr = FileUtils.byteCountToDisplaySize(rs.getLong("processed_log_size"));
									out.println("<td>" + sizeStr + "</td>");
									out.println("<td>" + rs.getString("processed_oper_count") + "</td>");
									out.println("<td>" + rs.getString("processed_txn_count") + "</td>");
	
								    DateTimeFormatter fullDateTimeFormatterMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
								    DateTimeFormatter timeFormatterMillis = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
								    LocalDate today = LocalDate.now();
									String lastConsolidatedChangeTSStr = "";
									long lastConsolidatedChangeTS = rs.getLong("last_consolidated_commit_id");
									if (lastConsolidatedChangeTS > 0) {
										LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastConsolidatedChangeTS), ZoneId.systemDefault());											 
									    if (localDateTime.toLocalDate().isEqual(today)) {
									    	lastConsolidatedChangeTSStr = localDateTime.format(timeFormatterMillis);
									    } else {
									    	lastConsolidatedChangeTSStr = localDateTime.format(fullDateTimeFormatterMillis);
									    }
										
									} 
									out.println("<td>" + lastConsolidatedChangeTSStr + "</td>");

									out.println("<td>" + (Double.valueOf(rs.getString("latency")) / 1000.0) + "</td>");
									out.println("</tr>");
								}
							}
						}
					} catch(Exception e) {
						out.println("<h4 style=\"color: red;\">Failed to read consolidator job statistics. Please refresh the page.</h4>");
						throw new javax.servlet.jsp.SkipPageException();						
					}
					%>
				</tbody>
			</table>

			</center>
			<%
			
				int refreshInterval = 5;
				if (request.getParameter("refresh-interval") != null) {
					try {
						refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
					} catch (Exception e) {
						refreshInterval = 5;
					}
				}

				Path deviceStatsFilePath = Path.of(devicePath, "synclite_device_statistics.db");
				if (!Files.exists(deviceStatsFilePath)) {
					out.println("<h4 style=\"color: red;\"> Device statistics file for this device is not available yet : " + deviceStatsFilePath + " </h4>");
					throw new javax.servlet.jsp.SkipPageException();
				}
			
				Long numTablesPerPage = 30L;	
				if (request.getParameter("numTablesPerPage") != null) {
					try {
						numTablesPerPage = Long.valueOf(request.getParameter("numTablesPerPage").trim());
						if (numTablesPerPage <= 0) {
							numTablesPerPage = 30L;
						}
					} catch (NumberFormatException e) {
						numTablesPerPage = 30L;
					}
				}
				
				Long pageNumber = null;
				if (request.getParameter("pageNumber") != null) {
					try {
						pageNumber = Long.valueOf(request.getParameter("pageNumber").trim());
					} catch (NumberFormatException e) {
						pageNumber = null;
					}
				}			

			String dstDBAlias = request.getParameter("dstDBAlias");			

			if (dstDBAlias == null) {
				dstDBAlias = "";
			} else {
				dstDBAlias = dstDBAlias.trim();
			}

			String tableName = request.getParameter("tableName");			

			if (tableName == null) {
				tableName = "";
			} else {
				tableName = tableName.trim();
			}

			String sortColumn = "table_name";
			if (request.getParameter("sortColumn") != null) {
				sortColumn = request.getParameter("sortColumn");
			}
			
			String sortOrder= "asc";
			if (request.getParameter("sortOrder") != null) {
				sortOrder = request.getParameter("sortOrder");
			}					
	
			String whereClause = "where 1=1";			
		
			if (!dstDBAlias.isEmpty()) {
				whereClause += " and dst_alias like '" + dstDBAlias.replace("*", "%") + "'";
			}
			if (!tableName.isEmpty()) {
				whereClause += " and table_name like '" + tableName.replace("*", "%") + "'";
			}

			Long numTables = 0L;
			Class.forName("org.sqlite.JDBC");
			try (Connection deviceConn = DriverManager.getConnection("jdbc:sqlite:" + Path.of(devicePath, "synclite_device_statistics.db"))) {
				try (Statement deviceStats = deviceConn.createStatement()) {
					try (ResultSet countRS = deviceStats.executeQuery("select count(*) from table_statistics " + whereClause)) {
						numTables = countRS.getLong(1);						
					}
				}
			} catch(Exception e) {
				out.println("<h4 style=\"color: red;\">Failed to read consolidator job statistics. Please refresh the page.</h4>");
				throw new javax.servlet.jsp.SkipPageException();
			}
		
			Long numPages = numTables / numTablesPerPage;
			
			if ((numTables % numTablesPerPage) > 0) {
				numPages += 1;
			}
			
			if (numPages == 0) {
				numPages = 1L;
			}
			
			Long prevPageNumber;
			Long nextPageNumber;
			if (pageNumber == null) {
				pageNumber = 1L;
				prevPageNumber = 1L;
				nextPageNumber = 2L;
			} else {
				if (pageNumber > numPages) {
					pageNumber = numPages;
				}				
				if (pageNumber <= 0) {
					pageNumber = 1L;
				}
				prevPageNumber = pageNumber - 1;
				nextPageNumber = pageNumber + 1;		
			}			
			if (nextPageNumber > numPages) {
				nextPageNumber = numPages;
			}
			if (prevPageNumber <= 0) {
				prevPageNumber = 1L;
			}
			
			Long numTablesOnThisPage = numTablesPerPage;
			if (pageNumber == numPages) {
				//This is the last page, we may have less records
				numTablesOnThisPage = numTables - ((pageNumber - 1) * numTablesPerPage);
			}
			
			Long startOffset = (pageNumber - 1) * numTablesPerPage;			
			%>
			<h2>Table Statistics</h2>
			<center>
				<form name="tableForm" id="tableForm" method="post">
					<input type="hidden" name ="sortColumn" id="sortColumn" value=<%=sortColumn%>>
					<input type="hidden" name ="sortOrder" id="sortOrder" value="<%=sortOrder%>">
					<table>
						<tr>
							<td>
								Dst DB Alias <input type="text" size="36" name = "dstDBAlias" id = "dstDBAlias" value = <%= dstDBAlias%>>						 
							</td>
							<td>
								Table Name <input type="text" size="36" name = "tableName" id = "tableName" value = <%= tableName%>>						 
							</td>
							<td>							
								<input type="button" name="Go" id="Go" value="Go" onclick = "this.form.submit()">
							</td>
						</tr>
					</table>
					<table>
					<tr>
						<td>
							<div class="pagination">
								<%
									if (pageNumber == 1) {
										//out.println("<a href=\"deviceStatistics.jsp?pageNumber=" + prevPageNumber + "\" class=\"disabled\">PREVIOUS | </a>");
										out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick = \"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\" disabled>");
									} else {
										//out.println("<a href=\"deviceStatistics.jsp?pageNumber=" + prevPageNumber + "\">PREVIOUS | </a>");
										out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick = \"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\">");
									}
									
									out.println("PAGE <input type=\"text\" size=2  name=\"pageNumber\" id=\"pageNumber\" value=\"" + pageNumber + "\"> OF " + numPages);
									out.println("<input type=\"button\" name=\"Go\" id=\"Go\" value=\"Go\" onclick = \"this.form.submit()\">");
	
									if (pageNumber == numPages) {
										//out.println("<a href=\"devices.jsp?pageNumber=" + nextPageNumber + "\" class=\"disabled\"> | NEXT</a>");
										out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick = \"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\" disabled>");
									} else {
										//out.println("<a href=\"devices.jsp?pageNumber=" + nextPageNumber + "\"> | NEXT</a>");
										out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick = \"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\">");
									}
								%>
							</div>						
						</td>
						<td>
							<div class="pagination">				
								<input type="text" size=2  name="numTablesPerPage" id="numTablesPerPage" value = <%=numTablesPerPage%>> PER PAGE
								<input type="button" name="Go" id="Go" value="Go" onclick = "this.form.submit()">
							</div>						
						</td>
						<td></td>
						<td></td>
						<td align="right">
							<div class="pagination">
								SHOWING <b><%=numTablesOnThisPage%></b> OUT OF <b><%=numTables%></b> TABLES
							</div>
						</td>
						<td>
							<div class="pagination">
	                			REFRESH IN
	                			<input type="text" id="refresh-interval" name="refresh-interval" value ="<%=refreshInterval%>" size="1" onchange="autoRefreshSetTimeout()"/>
	                			SECONDS
	                		</div>										
						</td>						
						
					</tr>			
			</table>			
			<table>
					<tr></tr>
					<tr>
						<th onclick="processSort('dst_alias');">
							<%
								if (sortColumn.equals("dst_alias")) {
									if (sortOrder.equals("asc")) {
										out.println("Dst DB<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Dst DB<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Dst DB");
								}
							%>							
						</th>
						<th onclick="processSort('table_name');">
							<%
								if (sortColumn.equals("table_name")) {
									if (sortOrder.equals("asc")) {
										out.println("Table<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Table<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Table");
								}
							%>							
						</th>
						<th onclick="processSort('initial_rows');">							
							<%
								if (sortColumn.equals("initial_rows")) {
									if (sortOrder.equals("asc")) {
										out.println("Initial Rows<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Initial Rows<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Initial Rows");
								}
							%>							
						</th>
						<th onclick="processSort('insert_rows');">
							<%
								if (sortColumn.equals("insert_rows")) {
									if (sortOrder.equals("asc")) {
										out.println("Insert/Upsert/Replace<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Insert/Upsert/Replace<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Insert/Upsert/Replace");
								}
							%>
						</th>
						<th onclick="processSort('update_rows');">
							<%
								if (sortColumn.equals("update_rows")) {
									if (sortOrder.equals("asc")) {
										out.println("Update<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Update<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Update");
								}
							%>
						</th>	
						<th onclick="processSort('delete_rows');">
							<%
								if (sortColumn.equals("delete_rows")) {
									if (sortOrder.equals("asc")) {
										out.println("Delete<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Delete<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Delete");
								}
							%>
						</th>							
						<th onclick="processSort('add_column');">
							<%
								if (sortColumn.equals("add_column")) {
									if (sortOrder.equals("asc")) {
										out.println("Add Column<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Add Column<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Add Column");
								}
							%>
						</th>
						<th onclick="processSort('drop_column');">
							<%
								if (sortColumn.equals("drop_column")) {
									if (sortOrder.equals("asc")) {
										out.println("Drop Column<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Drop Column<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Drop Column");
								}
							%>
						</th>
						<th onclick="processSort('rename_column');">
							<%
								if (sortColumn.equals("rename_column")) {
									if (sortOrder.equals("asc")) {
										out.println("Rename Column<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Rename Column<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Rename Column");
								}
							%>
						</th>
						<th onclick="processSort('create_table');">
							<%
								if (sortColumn.equals("create_table")) {
									if (sortOrder.equals("asc")) {
										out.println("Create Table<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Create Table<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Create Table");
								}
							%>
						</th>
						<th onclick="processSort('drop_table');">
							<%
								if (sortColumn.equals("drop_table")) {
									if (sortOrder.equals("asc")) {
										out.println("Drop Table<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Drop Table<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Drop Table");
								}
							%>
						</th>
						<th onclick="processSort('rename_table');">
							<%
								if (sortColumn.equals("rename_table")) {
									if (sortOrder.equals("asc")) {
										out.println("Rename Table<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
									} else {
										out.println("Rename Table<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
									}
								} else {
									out.println("Rename Table");
								}
							%>
						</th>
					</tr>
					<%
						try (Connection deviceConn = DriverManager.getConnection("jdbc:sqlite:" + Path.of(devicePath, "synclite_device_statistics.db"))) {
							try (Statement deviceStats = deviceConn.createStatement()) {
								try (ResultSet deviceRS = deviceStats.executeQuery("select dst_alias, table_name, initial_rows, insert_rows, update_rows, delete_rows, add_column, drop_column, rename_column, create_table, drop_table, rename_table from table_statistics " + whereClause + " order by " + sortColumn + " " + sortOrder + " limit " + startOffset + ", " + numTablesPerPage)) {
									while (deviceRS.next()) {
										out.println("<tr>");
										out.println("<td>" + deviceRS.getString("dst_alias") + "</td>");
										out.println("<td>" + deviceRS.getString("table_name") + "</td>");
										out.println("<td>" + deviceRS.getString("initial_rows") + "</td>");
										out.println("<td>" + deviceRS.getString("insert_rows") + "</td>");
										out.println("<td>" + deviceRS.getString("update_rows") + "</td>");
										out.println("<td>" + deviceRS.getString("delete_rows") + "</td>");
										out.println("<td>" + deviceRS.getString("add_column") + "</td>");
										out.println("<td>" + deviceRS.getString("drop_column") + "</td>");
										out.println("<td>" + deviceRS.getString("rename_column") + "</td>");
										out.println("<td>" + deviceRS.getString("create_table") + "</td>");
										out.println("<td>" + deviceRS.getString("drop_table") + "</td>");
										out.println("<td>" + deviceRS.getString("rename_table") + "</td>");
										out.println("</tr>");
									}
								}
							}
						} catch(Exception e) {
							out.println("<h4 style=\"color: red;\">Failed to read consolidator job statistics. Please refresh the page.</h4>");
						}
					%>
			</table>			
		</center>
	</div>
</body>
</html>