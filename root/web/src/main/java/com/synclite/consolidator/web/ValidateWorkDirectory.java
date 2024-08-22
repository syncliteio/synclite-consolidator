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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateWorkDirectory")
public class ValidateWorkDirectory extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateWorkDirectory() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {

			String jobName = request.getParameter("job-name");

			if (jobName != null) {
				//Check if specified jobName is in correct format
				
				if (jobName.length() > 16 ) {
					throw new ServletException("Job name must be upto 16 characters in length");
				}
				if (!jobName.matches("[a-zA-Z0-9-_]+")) {
					throw new ServletException("Specified job name is invalid. Allowed characters are alphanumeric characters or hyphens.");
				}		
			} else {
				jobName = "job1";
			}

			String deviceDataRoot = request.getParameter("device-data-root");
			
			Path deviceDataRootPath;
			if ((deviceDataRoot == null) || deviceDataRoot.trim().isEmpty()) {
				throw new ServletException("\"Data Directory\" must be specified");
			} else {
				deviceDataRootPath = Path.of(deviceDataRoot);	
		        try {
					if (! Files.exists(Path.of(deviceDataRoot))) {
						Files.createDirectories(Path.of(deviceDataRoot));
					}
				} catch (Exception e) {
					throw new ServletException("Failed to create directory : " + deviceDataRoot + " : " + e.getMessage(), e);
				}
				if (! Files.exists(deviceDataRootPath)) {
					throw new ServletException("Specified \"Data Directory\" : " + deviceDataRoot + " does not exist, please specify a valid \"Data Directory\"");
				}
			}

			if (! deviceDataRootPath.toFile().canRead()) {
				throw new ServletException("Specified \"Data Directory\" does not have read permission");
			}

			if (! deviceDataRootPath.toFile().canWrite()) {
				throw new ServletException("Specified \"Data Directory\" does not have write permission");
			}
			
			String srcAppType = request.getParameter("src-app-type");
			
			request.getSession().setAttribute("device-data-root",deviceDataRoot); 
			request.getSession().setAttribute("job-name", jobName);
			request.getSession().setAttribute("src-app-type", srcAppType);
			
			response.sendRedirect("configureJob.jsp");
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("selectWorkDirectory.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
