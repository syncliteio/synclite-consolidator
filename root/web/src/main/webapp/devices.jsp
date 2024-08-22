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
<title>SyncLite Devices</title>
</head>

<script type="text/javascript">

	function processSort(sortColumn) {
		if (sortColumn==document.deviceForm.sortColumn.value) {
			if (document.deviceForm.sortOrder.value=="asc") {
				document.deviceForm.sortOrder.value="desc";
			} else {
				document.deviceForm.sortOrder.value="asc";
			}
		} else {
			document.deviceForm.sortColumn.value=sortColumn;
			document.deviceForm.sortOrder.value="asc";
		}
		document.deviceForm.submit();
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
		document.forms['deviceForm'].submit();
	}

</script>
<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Devices</h2>
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

			int refreshInterval = 5;
			if (request.getParameter("refresh-interval") != null) {
				try {
					refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
				} catch (Exception e) {
					refreshInterval = 5;
				}
			}

			Long numDevicesPerPage = 30L;	
			if (request.getParameter("numDevicesPerPage") != null) {
				try {
					numDevicesPerPage = Long.valueOf(request.getParameter("numDevicesPerPage").trim());
					if (numDevicesPerPage <= 0) {
						numDevicesPerPage = 30L;
					}
				} catch (NumberFormatException e) {
					numDevicesPerPage = 30L;
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
			
			String deviceUUID = request.getParameter("deviceUUID");			
			String deviceName = request.getParameter("deviceName");
			
			if (deviceUUID == null) {
				deviceUUID = "";
			} else {
				deviceUUID = deviceUUID.trim();
			}
			
			if (deviceName == null) {
				deviceName = "";
			} else {
				deviceName = deviceName.trim();
			}

			String deviceStatus = "ALL";			
			if (request.getParameter("deviceStatus") != null) {
				deviceStatus = request.getParameter("deviceStatus");
			}

			String sortColumn = "synclite_device_id";
			if (request.getParameter("sortColumn") != null) {
				sortColumn = request.getParameter("sortColumn");
			}
			
			String sortOrder= "asc";
			if (request.getParameter("sortOrder") != null) {
				sortOrder = request.getParameter("sortOrder");
			}
					
			
			String whereClause = " where 1=1";
			
			if (!deviceStatus.equals("ALL")) {
				whereClause += " and status = '" + deviceStatus + "'";
			}
			
			if (!deviceName.isEmpty()) {
				whereClause += " and synclite_device_name like '" + deviceName.replace("*", "%") + "'";
			}

			if (!deviceUUID.isEmpty()) {
				whereClause += " and synclite_device_id like '" + deviceUUID.replace("*", "%") + "'";
			}

			Long numDevices = 0L;
			Class.forName("org.sqlite.JDBC");					
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
				try (Statement stat = conn.createStatement()) {
					try (ResultSet countRS = stat.executeQuery("select count(*) from device_status " + whereClause)) {
						numDevices = countRS.getLong(1);
					}
				}
			}

			Long numPages = numDevices / numDevicesPerPage;

			if ((numDevices % numDevicesPerPage) > 0) {
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

			Long numDevicesOnThisPage = numDevicesPerPage;
			if (pageNumber == numPages) {
				//This is last page, we may have less records 
				numDevicesOnThisPage = numDevices - ((pageNumber - 1) * numDevicesPerPage);
			}

			Long startOffset = (pageNumber - 1) * numDevicesPerPage;
			
			//
			if (session.getAttribute("show-devices-type") == null) {
				if ((request.getSession().getAttribute("src-app-type") != null) && !request.getSession().getAttribute("src-app-type").equals("SYNCLITE-DBREADER")) {					
					//We can skip to show these fields by default when source is DBReader.
					session.setAttribute("show-devices-type", "true");
					session.setAttribute("show-devices-uuid", "true");
				} else {
					session.setAttribute("show-devices-type", "false");
					session.setAttribute("show-devices-uuid", "false");
				}
			}

		%>
		<center>
			<form name="deviceForm" id="deviceForm" method="post" action="devices.jsp">
				<input type="hidden" name ="sortColumn" id="sortColumn" value=<%=sortColumn%>>
				<input type="hidden" name ="sortOrder" id="sortOrder" value="<%=sortOrder%>">
				<table>
					<tr>
						<td>
							Device Sync Status
							<%
								out.println("<select id=\"deviceStatus\" name=\"deviceStatus\">");
								if (deviceStatus.equals("ALL")) {
									out.println("<option value=\"ALL\" selected>ALL</option>");
								} else {
									out.println("<option value=\"ALL\">ALL</option>");
								}
								if (deviceStatus.equals("UNREGISTERED")) {
									out.println("<option value=\"UNREGISTERED\" selected>UNREGISTERED</option>");
								} else {
									out.println("<option value=\"UNREGISTERED\">UNREGISTERED</option>");
								} 
								if (deviceStatus.equals("REGISTERED")) {
									out.println("<option value=\"REGISTERED\" selected>REGISTERED</option>");
								} else {
									out.println("<option value=\"REGISTERED\">REGISTERED</option>");
								}								
								if (deviceStatus.equals("REGISTRATION_FAILED")) {
									out.println("<option value=\"REGISTRATION_FAILED\" selected>REGISTRATION_FAILED</option>");
								} else {
									out.println("<option value=\"REGISTRATION_FAILED\">REGISTRATION_FAILED</option>");
								}
								if (deviceStatus.equals("SYNCING")) {
									out.println("<option value=\"SYNCING\" selected>SYNCING</option>");
								} else {
									out.println("<option value=\"SYNCING\">SYNCING</option>");
								}
								if (deviceStatus.equals("SYNCING_FAILED")) {
									out.println("<option value=\"SYNCING_FAILED\" selected>SYNCING_FAILED</option>");
								} else {
									out.println("<option value=\"SYNCING_FAILED\">SYNCING_FAILED</option>");
								}
								if (deviceStatus.equals("SUSPENDED")) {
									out.println("<option value=\"SUSPENDED\" selected>SUSPENDED</option>");
								} else {
									out.println("<option value=\"SUSPENDED\">SUSPENDED</option>");
								}
								if (deviceStatus.equals("DISABLED")) {
									out.println("<option value=\"DISABLED\" selected>DISABLED</option>");
								} else {
									out.println("<option value=\"DISABLED\">DISABLED</option>");
								}								
								out.println("</select>");
							%>
						</td>					
						<td>
							Device UUID
							<input type="text" size="36" name = "deviceUUID" id = "deviceUUID" value = <%= deviceUUID%>>						 
						</td>
						<td>
							Device Name
							<input type="text" size="36" name = "deviceName" id = "deviceName" value = <%= deviceName%>>						 
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
									//out.println("<a href=\"#\" class=\"disabled\">PREVIOUS |</a>");	
									out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick = \"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\" disabled>");

								} else {
									//out.println("<a href=\"#\" onclick =\"javascript: deviceForm.pageNumber=deviceForm.pageNumber - 1; deviceForm.submit();\">PREVIOUS | </a>");
									out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick = \"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\">");
								}
								
								out.println("PAGE <input type=\"text\" size=2  name=\"pageNumber\" id=\"pageNumber\" value=" + pageNumber + "> OF " + numPages);
								out.println("<input type=\"button\" name=\"Go\" id=\"Go\" value=\"Go\" onclick = \"this.form.submit()\">");

								if (pageNumber == numPages) {
									//out.println("<a href=\"#\" class=\"disabled\"> | NEXT</a>");
									out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick = \"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\" disabled>");
								} else {
									//out.println("<a href=\"#\" onclick =\"javascript: deviceForm.pageNumber=deviceForm.pageNumber + 1; deviceForm.submit();\"> | NEXT</a>");
									out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick = \"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\">");
								}
								%>
							</div>						
						</td>
						<td>
							<div class="pagination">				
								<input type="text" size=2  name="numDevicesPerPage" id="numDevicesPerPage" value = <%=numDevicesPerPage%>> PER PAGE
								<input type="button" name="Go" id="Go" value="Go" onclick = "this.form.submit()">
							</div>													
						</td>							
						<td></td>
						<td></td>
						<td align="right">
							<div class="pagination">
								SHOWING <b><%=numDevicesOnThisPage%></b> OUT OF <b><%=numDevices%></b> DEVICES
							</div>							
						</td>
						<td>
							<input type="button" name="customize" id="customize" value="Customize Report" onclick = "window.location.href = 'customizeDevicesReport.jsp'">
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
					<tbody>
						<tr>
						</tr>
						<tr>
							<%
								if ((session.getAttribute("show-devices-uuid") == null) || session.getAttribute("show-devices-uuid").toString().equals("true")) { 
									out.println("<th onclick=\"processSort('synclite_device_id');\">");
									if (sortColumn.equals("synclite_device_id")) {
										if (sortOrder.equals("asc")) {
											out.println("UUID<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
										} else {
											out.println("UUID<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
										}
									} else {
										out.println("UUID");
									}
									out.println("</th>");
								}
							%>							
							<%
								if ((session.getAttribute("show-devices-name") == null) || session.getAttribute("show-devices-name").toString().equals("true")) {
									out.println("<th onclick=\"processSort('synclite_device_name');\">");
									if (sortColumn.equals("synclite_device_name")) {
										if (sortOrder.equals("asc")) {
											out.println("Name<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
										} else {
											out.println("Name<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
										}
									} else {
										out.println("Name");
									}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-type") == null) || session.getAttribute("show-devices-type").toString().equals("true")) {
									out.println("<th onclick=\"processSort('synclite_device_type');\">");
									if (sortColumn.equals("synclite_device_type")) {
											if (sortOrder.equals("asc")) {
												out.println("Name<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Name<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Type");
										}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-status") == null) || session.getAttribute("show-devices-status").toString().equals("true")) {
									out.println("<th onclick=\"processSort('status');\">");
									if (sortColumn.equals("status")) {
											if (sortOrder.equals("asc")) {
												out.println("Status<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Status<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Sync Status");
										}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-dbalias") == null) || session.getAttribute("show-devices-dbalias").toString().equals("true")) {
									out.println("<th onclick=\"processSort('destination_database_alias');\">");
									if (sortColumn.equals("destination_database_alias")) {
										if (sortOrder.equals("asc")) {
											out.println("Destination DB<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
										} else {
											out.println("Destination DB<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
										}
									} else {
										out.println("Destination DB");
									}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-applied-log-segments") == null) || session.getAttribute("show-devices-applied-log-segments").toString().equals("true")) {
									out.println("<th onclick=\"processSort('log_segments_applied');\">");
									if (sortColumn.equals("log_segments_applied")) {
											if (sortOrder.equals("asc")) {
												out.println("Applied Log Segments<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Applied Log Segments<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Applied Log Segments");
										}
									out.println("</th>");
								}
							%>							
							<%
								if ((session.getAttribute("show-devices-processed-log-size") != null) && session.getAttribute("show-devices-processed-log-size").toString().equals("true")) {
									out.println("<th onclick=\"processSort('processed_log_size');\">");
									if (sortColumn.equals("processed_log_size")) {
											if (sortOrder.equals("asc")) {
												out.println("Processed Log Size<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Processed Log Size<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Processed Log Size");
										}
									out.println("</th>");
								}
							%>							
							<%
								if ((session.getAttribute("show-devices-processed-oper-count") != null) && session.getAttribute("show-devices-processed-oper-count").toString().equals("true")) {
									out.println("<th onclick=\"processSort('processed_oper_count');\">");
									if (sortColumn.equals("processed_oper_count")) {
											if (sortOrder.equals("asc")) {
												out.println("Processed Operation Count<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Processed Operation Count<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Processed Operation Count");
										}
									out.println("</th>");
								}
							%>							
							<%
								if ((session.getAttribute("show-devices-processed-txn-count") != null) && session.getAttribute("show-devices-processed-txn-count").toString().equals("true")) {
									out.println("<th onclick=\"processSort('processed_txn_count');\">");
									if (sortColumn.equals("processed_txn_count")) {
											if (sortOrder.equals("asc")) {
												out.println("Processed Transaction Count<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Processed Transcation Count<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Processed Transaction Count");
										}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-last-consolidated-commit-id") == null) || session.getAttribute("show-devices-last-consolidated-commit-id").toString().equals("true")) {	
									out.println("<th onclick=\"processSort('last_consolidated_commit_id');\">");
									if (sortColumn.equals("latency")) {
											if (sortOrder.equals("asc")) {
												out.println("Last Consolidated SyncLite TS<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Last Consolidated SyncLite TS<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Last Applied SyncLite TS");
										}
									out.println("</th>");
								}
							%>
							<%
								if ((session.getAttribute("show-devices-latency") == null) || session.getAttribute("show-devices-latency").toString().equals("true")) {	
									out.println("<th onclick=\"processSort('latency');\">");
									if (sortColumn.equals("latency")) {
											if (sortOrder.equals("asc")) {
												out.println("Latency(s)<img src=\"image/sortup.png\" style='height: 15px; width: 15px'>");
											} else {
												out.println("Latency(s)<img src=\"image/sortdown.png\" style='height: 15px; width: 15px'>");
											}
										} else {
											out.println("Latency(s)");
										}
									out.println("</th>");
								}
							%>

						</tr>

						<%
						try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
							try (Statement stat = conn.createStatement()) {
								try (ResultSet rs = stat.executeQuery("select synclite_device_id, synclite_device_name, synclite_device_type, status, destination_database_alias, log_segments_applied, processed_log_size, processed_oper_count, processed_txn_count, latency, last_consolidated_commit_id from device_status "
										+ whereClause + " order by " + sortColumn + " " + sortOrder + " limit " + startOffset + ", "
										+ numDevicesPerPage
								)) {
									while (rs.next()) {
										String deviceStatisticsURL = "deviceStatistics.jsp?uuid="
												+ URLEncoder.encode(rs.getString("synclite_device_id"), Charset.defaultCharset()) + "&name="
												+ URLEncoder.encode(rs.getString("synclite_device_name"), Charset.defaultCharset());
										out.println("<tr>");
										if ((session.getAttribute("show-devices-uuid") == null)
												|| session.getAttribute("show-devices-uuid").toString().equals("true")) {
											out.println("<td><a href=\"" + deviceStatisticsURL + "\">" + rs.getString("synclite_device_id")
													+ "</a></td>");
										}
		
										if ((session.getAttribute("show-devices-name") == null)
												|| session.getAttribute("show-devices-name").toString().equals("true")) {
											out.println("<td><a href=\"" + deviceStatisticsURL + "\">" + rs.getString("synclite_device_name")
													+ "</a></td>");
										}
		
										if ((session.getAttribute("show-devices-type") == null)
												|| session.getAttribute("show-devices-type").toString().equals("true")) {
											out.println("<td>" + rs.getString("synclite_device_type") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-status") == null)
												|| session.getAttribute("show-devices-status").toString().equals("true")) {
											out.println("<td>" + rs.getString("status") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-dbalias") == null)
												|| session.getAttribute("show-devices-dbalias").toString().equals("true")) {
											out.println("<td>" + rs.getString("destination_database_alias") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-applied-log-segments") == null)
												|| session.getAttribute("show-devices-applied-log-segments").toString().equals("true")) {
											out.println("<td>" + rs.getString("log_segments_applied") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-processed-log-size") != null)
												&& session.getAttribute("show-devices-processed-log-size").toString().equals("true")) {
											String sizeStr = FileUtils.byteCountToDisplaySize(rs.getLong("processed_log_size"));
											out.println("<td>" + sizeStr + "</td>");
										}
		
										if ((session.getAttribute("show-devices-processed-oper-count") != null)
												&& session.getAttribute("show-devices-processed-oper-count").toString().equals("true")) {
											out.println("<td>" + rs.getString("processed_oper_count") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-processed-txn-count") != null)
												&& session.getAttribute("show-devices-processed-txn-count").toString().equals("true")) {
											out.println("<td>" + rs.getString("processed_txn_count") + "</td>");
										}
		
										if ((session.getAttribute("show-devices-last-consolidated-commit-id") == null)
												|| session.getAttribute("show-devices-last-consolidated-commit-id").toString().equals("true")) {

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
										}

										if ((session.getAttribute("show-devices-latency") == null)
												|| session.getAttribute("show-devices-latency").toString().equals("true")) {
											out.println("<td>" + (Double.valueOf(rs.getString("latency")) / 1000.0) + "</td>");
										}
										out.println("</tr>");

									}
								}
							}
						} catch(Exception e) {
							out.println("<h4 style=\"color: red;\">Failed to read consolidator job statistics. Please refresh the page.</h4>");
						}
						%>
					</tbody>
				</table>
			</form>
				
		</center>
	</div>
</body>
</html>