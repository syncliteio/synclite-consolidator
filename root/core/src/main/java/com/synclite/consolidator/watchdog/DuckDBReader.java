/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.consolidator.watchdog;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.global.ConfLoader;


public class DuckDBReader extends Thread{

	private Logger tracer;
	private int dstIndex;
	public DuckDBReader(Logger tracer, int dstIndex) {
		this.tracer = tracer;
		this.dstIndex = dstIndex;
	}

	public void run() {
		try (ZContext context = new ZContext()) {
			//  Socket to talk to clients			
			ZMQ.Socket socket = context.createSocket(SocketType.REP);
	        socket.setZAPDomain("tcp://localhost");
			int port = ConfLoader.getInstance().getDstDuckDBReaderPortNumber(dstIndex);
			socket.bind("tcp://localhost:" + port);

			while (!Thread.currentThread().isInterrupted()) {
				String sql,format;
				try {
					byte[] packet = socket.recv(0);
					String req = new String(packet, ZMQ.CHARSET);
					
					try {
						String stripedCommand = req.strip();						
						int separatorIndex = stripedCommand.indexOf(" ");
						String command = stripedCommand.substring(0, separatorIndex);
						sql = stripedCommand.substring(separatorIndex, stripedCommand.length());

						format = "TEXT";
						if (command.equalsIgnoreCase("GetTextOutput")) {
							format = "TEXT";
						} else if (command.equalsIgnoreCase("GetHTMLOutput")) {
							format = "HTML";
						} else {
							String error = "Usage 1 : GetTextOutput <SQL>";
							error = error + "\n" + "Usage 2 : GetHTMLOutput <SQL>";
							tracer.error(error);
							socket.send(error.getBytes(ZMQ.CHARSET), 0);
							continue;							
						}
					} catch (Exception e) {
						String error = "Failed to parse received request  : " + req + " with exception : "+ e.getMessage();
						tracer.error(error);
						socket.send(error.getBytes(ZMQ.CHARSET), 0);
						continue;
					}

					StringBuilder response = new StringBuilder();

					if (format.equals("HTML")) {
						long rowCnt = 0;
						String errorMsg = "";
						Properties ro_prop = new Properties();
						ro_prop.setProperty("duckdb.read_only", "true");
						try (Connection conn = JDBCConnector.getInstance(dstIndex).connect()){
							try (Statement stmt = conn.createStatement()) {
								try (ResultSet rs = stmt.executeQuery(sql)) {						
									if (rs != null) {								
										response.append("<tr>");
										response.append("<td>Result</td>");
										response.append("<td>(Top 1000 rows)</td>");
										response.append("</tr>");
										response.append("<tr>");
										response.append("<td></td>");
										response.append("<td>");

										ResultSetMetaData rsMetadata = rs.getMetaData();
										int colCount = rsMetadata.getColumnCount();
										response.append("<div class=\"container\">");

										response.append("<table>");
										response.append("<tbody>");

										response.append("<tr>");
										for (int j = 1; j <= colCount; ++j) {
											String colDisplayName = rsMetadata.getColumnName(j);
											response.append("<th>");
											response.append(colDisplayName);
											response.append("</th>");
										}
										response.append("</tr>");

										while (rs.next()) {
											if (rowCnt >= 1000) {
												++rowCnt;
												continue;
											}
											response.append("<tr>");
											for (int k = 1; k <= colCount; ++k) {
												response.append("<td>");
												Object v = rs.getObject(k);
												if (v instanceof Blob) {
							    					byte[] bytes = ((Blob) v).getBinaryStream().readAllBytes();
							    					response.append(new String(bytes));
												} else if (v instanceof Clob) {
							    					byte[] bytes = ((Clob) v).getAsciiStream().readAllBytes();
							    					response.append(new String(bytes));
												} else if (v != null){													
													response.append(v.toString());
												} else {
													response.append("null");
												}
												response.append("</td>");
											}
											response.append("</tr>");
											++rowCnt;
										}
										response.append("</tbody>");
										response.append("</table>");
										response.append("</div>");
										response.append("</td>");
										response.append("</tr>");
									}
								}
							}
						} catch (SQLException e) {
							errorMsg = "Query failed with exception : " + e.getMessage();
							tracer.error("DuckDB query failed with exception : ", e);					
						}

						if (rowCnt > 0) {
							response.append("<tr>");
							response.append("<td></td>");							
							response.append("<td>");
							response.append(rowCnt + " rows");
							response.append("</td>");
							response.append("</tr>");
						}
						if (!errorMsg.isEmpty()) {
							response = new StringBuilder();
							response.append("<tr>");
							response.append("<td></td>");							
							response.append("<td><h4 style=\"color: red;\">");
							response.append(errorMsg);
							response.append("</h4></td>");
							response.append("</tr>");										
						}
					} else {
						long rowCnt = 0;
						String errorMsg = "";
						Properties ro_prop = new Properties();
						ro_prop.setProperty("duckdb.read_only", "true");
						try (Connection conn = JDBCConnector.getInstance(dstIndex).connect()){
							try (Statement stmt = conn.createStatement()) {
								try (ResultSet rs = stmt.executeQuery(sql)) {						
									if (rs != null) {								
										ResultSetMetaData rsMetadata = rs.getMetaData();
										int colCount = rsMetadata.getColumnCount();
										boolean first = true;
										for (int j = 1; j <= colCount; ++j) {
											if (first == false) {
												response.append("|");
											}
											String colDisplayName = rsMetadata.getColumnName(j);
											response.append(colDisplayName);
											first = false;
										}
										response.append("\n");

										while (rs.next()) {
											if (rowCnt >= 1000) {
												++rowCnt;
												continue;
											}
											first = true;
											for (int k = 1; k <= colCount; ++k) {
												if (first == false) {
													response.append("|");
												}
												Object v = rs.getObject(k);									
												
												if (v instanceof Blob) {
							    					byte[] bytes = ((Blob) v).getBinaryStream().readAllBytes();
							    					response.append(new String(bytes));
												} else if (v instanceof Clob) {
							    					byte[] bytes = ((Clob) v).getAsciiStream().readAllBytes();
							    					response.append(new String(bytes));						
												} else if (v != null){
													response.append(v.toString());
												} else {
													response.append("null");
												}
												first = false;
											}
											response.append("\n");
											++rowCnt;
										}
									}
								}
							}
						} catch (SQLException e) {
							errorMsg = "Query failed with exception : " + e.getMessage();
							tracer.error("DuckDB query failed with exception : ", e);					
						}

						if (!errorMsg.isEmpty()) {
							response.append(errorMsg);
						}

					}
					socket.send(response.toString().getBytes(ZMQ.CHARSET), 0);
				} catch (Exception e) {
					tracer.error("DuckDBReader encountered exception : ", e);					
				}
			}			
		} catch (Exception e) {
			tracer.error("DuckDB Reader failed to with exception : ", e);
		}
	}
}

