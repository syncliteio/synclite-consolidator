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

import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Servlet implementation class ValidateDestinationDB
 */


@WebServlet("/validateValueMapper")
public class ValidateValueMapper extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateValueMapper() {
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
		Integer dstIndex = 1;
		try {

			Integer numDestinations = Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
			String dstIndexStr = request.getParameter("dst-index");
			try {
				dstIndex = Integer.valueOf(dstIndexStr);
				if (dstIndex == null) {
					throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
				} else if (dstIndex <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Destination Index\"");
				} else if (dstIndex > numDestinations) {
					throw new ServletException("\"Destination Index\"" + dstIndex + " exceeded configured \"Number Of Destinations Databases\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
			}

			String dstEnableValueMapperStr = request.getParameter("dst-enable-value-mapper-" + dstIndex);

			if (dstEnableValueMapperStr.equals("true")) {
				//Validate and persist
				
				String valueMappingsStr = request.getParameter("dst-value-mappings-" + dstIndex);
				if (valueMappingsStr.isBlank()) {
					throw new ServletException("You must specify value mappings with value mapper enabled");					
				}
				
				try {
		            new JSONObject(new JSONTokener(valueMappingsStr));
				} catch (Exception e) {
					throw new ServletException("Please specify value mappings in valid JSON format", e);
				}
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String valueMappingsFilePath = Path.of(deviceDataRoot, "synclite_value_mappings_" + dstIndexStr + ".json").toString();
				try {
					Files.writeString(Path.of(valueMappingsFilePath), valueMappingsStr);
				} catch (IOException e) {
					throw new ServletException("Failed to write value mappings in configuration file : " + valueMappingsFilePath, e);
				}
				request.getSession().setAttribute("dst-value-mappings-file-" + dstIndex, valueMappingsFilePath);
			} else {
				request.getSession().removeAttribute("dst-value-mappings-file-" + dstIndex);
			}
			request.getSession().setAttribute("dst-enable-value-mapper-" + dstIndex, dstEnableValueMapperStr);

			//request.getRequestDispatcher("configureDBWriter.jsp?dstIndex=" + dstIndex).forward(request, response);
			response.sendRedirect("configureDBTriggers.jsp?dstIndex=" + dstIndex);
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureValueMapper.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
