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

package com.synclite.consolidator.web;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SaveJobConfiguration
 */
@WebServlet("/loadJob")
public class LoadJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public LoadJob() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {

			String deviceDataRoot = request.getParameter("device-data-root");

			Path deviceDataRootPath;
			if ((deviceDataRoot == null) || deviceDataRoot.trim().isEmpty()) {
				throw new ServletException("\"Work Directory\" must be specified");
			} else {
				deviceDataRootPath = Path.of(deviceDataRoot);
				if (! Files.exists(deviceDataRootPath)) {
					throw new ServletException("Specified \"Work Directory\" : " + deviceDataRoot + " does not exist, please specify a valid \"Data Directory\"");
				}
			}

			if (! deviceDataRootPath.toFile().canRead()) {
				throw new ServletException("Specified \"Work Directory\" does not have read permission");
			}

			if (! deviceDataRootPath.toFile().canWrite()) {
				throw new ServletException("Specified \"Work Directory\" does not have write permission");
			}

			Path configPath = deviceDataRootPath.resolve("synclite_consolidator.conf");

			if (!Files.exists(configPath)) {
				throw new ServletException("Synclite Configuration file does not exist in specified \"Work Directory\". Please configure the job and start if it is was not previous configured and run.");
			}

			loadConsolidatorConfig(request, configPath);
			setAdditionalSessionVariables(request);

			//request.getRequestDispatcher("dashboard.jsp").forward(request, response);
			response.sendRedirect("dashboard.jsp");
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("loadJob.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}


	private void setAdditionalSessionVariables(HttpServletRequest request) {
		
		//Set dst-type-name-<dstIndex> 		
		int numDestinations =  Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
		for (int dstIndex = 1; dstIndex <= numDestinations; ++dstIndex) {
			request.getSession().setAttribute("dst-type-name-" + dstIndex, getDstName(request.getSession().getAttribute("dst-type-" + dstIndex).toString()));			
		}		
		request.getSession().setAttribute("job-status","STARTED");
		request.getSession().setAttribute("job-type","SYNC");
		//TODO fix start time for job being loaded
		request.getSession().setAttribute("job-start-time",System.currentTimeMillis());		
	}

	private final String getDstName(String dType) {
		switch (DstType.valueOf(dType)) {
		case CLICKHOUSE:
			return "ClickHouse";
		case DUCKDB:
			return "DuckDB";
		case FERRETDB:
			return "FerretDB";
		case APACHE_ICEBERG:
			return "Apache Iceberg";
		case MONGODB:
			return "MongoDB";		
		case MYSQL:
			return "MySQL";
		case MSSQL:
			return "Microsoft SQL Server";
		case POSTGRESQL:
			return "PostgreSQL";
		case SQLITE:
			return "SQLite";
		default:
			return dType.toString();
		}
	}

	private final void loadConsolidatorConfig(HttpServletRequest request, Path propsPath) throws ServletException {
		//HashMap properties = new HashMap<String, String>();
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
								request.getSession().setAttribute(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
							}
						}
					}  else {
						request.getSession().setAttribute(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());	
					}					
					line = reader.readLine();
				}
				reader.close();
			}
		} catch (Exception e){ 
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e1) {
				//Ignore
			}
			throw new ServletException("Failed to load consolidator config file : " + propsPath, e);
		} 

	}
}
