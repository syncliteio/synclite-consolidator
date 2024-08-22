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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateDestinationDB
 */
@WebServlet("/validateNumDestinations")
public class ValidateNumDestinations extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateNumDestinations() {
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

			String numDestinationsStr = request.getParameter("num-destinations");
			try {
				if (Integer.valueOf(numDestinationsStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Number Of Destination Databases\"");
				} else if (Integer.valueOf(numDestinationsStr) <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Number Of Destination Databases\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Number Of Destination Databases\"");
			}

			request.getSession().setAttribute("num-destinations", numDestinationsStr);
			
			//request.getRequestDispatcher("configureDestinationDB.jsp?dstIndex=1").forward(request, response);
			response.sendRedirect("configureDestinationDB.jsp?dstIndex=1");
		} catch (Exception e) {
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureNumDestinations.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private final void validateConnection(String dstConnectionString) throws ServletException {
		try(Connection conn = DriverManager.getConnection(dstConnectionString)) {
			//Do nothing
		} catch (SQLException e){
			throw new ServletException("Failed to connect to destination database. Please verify specified connection string." +  e.getMessage(), e);
		}
	}

	private final void vaidateDBPath(Path dbPath) throws ServletException {
		if (dbPath == null) {
			throw new ServletException("Invalid connection string specified. Please verify specified connection string.");
		}
		Path parentDir = dbPath.getParent();
		if (parentDir.toFile().exists()) {
			if (!parentDir.toFile().canWrite()) {
				throw new ServletException("The directory specified in the connection string is not writable : " + parentDir + ". Please verify specified connection string.");
			}
		} else {
			throw new ServletException("The directory specified in the connection string is invalid. Please verify specified connection string.");
		}		
	}
}
