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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SaveJobConfiguration
 */
@WebServlet("/mapDevicesToDsts")
public class MapDevicesToDsts extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public MapDevicesToDsts() {
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
			String mapDevicesToDstPatternTypeStr = "";
			if (request.getParameter("map-devices-to-dst-pattern-type") != null) {
				mapDevicesToDstPatternTypeStr = request.getParameter("map-devices-to-dst-pattern-type");
				if (mapDevicesToDstPatternTypeStr.trim().isEmpty()) {
					throw new ServletException("Please specify \"Map Devices By\"");
				} 
			}

			Integer numDestinations = Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
			String mapDevicesToDstPatternStr[] = new String[numDestinations + 1];
			for (int dstIndex = 1 ; dstIndex <= numDestinations ; ++dstIndex) {
				if (request.getParameter("map-devices-to-dst-pattern-" + dstIndex) != null) {
					String pat = request.getParameter("map-devices-to-dst-pattern-" + dstIndex);					
					try {
						if (pat.trim().isEmpty()) {
							throw new ServletException("Please specify valid (Java) regular expression pattern in \"Device Name/ID Pattern\" for destination DB " + dstIndex);
						}				
						else if (Pattern.compile(pat) == null) {
							throw new ServletException("Please specify valid (Java) regular expression pattern in \"Device Name/ID Pattern\" for destination DB " + dstIndex);
						}
						mapDevicesToDstPatternStr[dstIndex] = pat;
					} catch (PatternSyntaxException e) {
						throw new ServletException("Please specify valid (Java) regular expression pattern in \"Device Name/ID Pattern\" for destination DB + " + dstIndex);
					}
				} else {
					throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device Name/ID Pattern\" for destination DB : " + dstIndex);
				}
			}

			String defaultDstForUnmappedDevicesStr; 
			if (request.getParameter("default-dst-index-for-unmapped-devices") != null) {
				defaultDstForUnmappedDevicesStr = request.getParameter("default-dst-index-for-unmapped-devices");				
				try {
					Integer parsed = Integer.parseInt(defaultDstForUnmappedDevicesStr);
					if ((parsed < 0) || (parsed > numDestinations)) {
						throw new ServletException("Please specify a valid numeric value between 1 to " + numDestinations + " for \"Default Destination DB For Unmapped Devices\"");
					}
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value between 1 to " + numDestinations + " for \"Default Destination DB For Unmapped Devices\"");
				}
			} else {
				throw new ServletException("Please specify \"Default Destination DB For Unmapped Devices\"");
			}
			
			request.getSession().setAttribute("map-devices-to-dst-pattern-type", mapDevicesToDstPatternTypeStr);			
			for (int dstIndex = 1; dstIndex <= numDestinations; ++dstIndex) {
				request.getSession().setAttribute("map-devices-to-dst-pattern-" + dstIndex, mapDevicesToDstPatternStr[dstIndex]);
			}
			request.getSession().setAttribute("default-dst-index-for-unmapped-devices", defaultDstForUnmappedDevicesStr);
			
			request.getRequestDispatcher("jobSummary.jsp").forward(request, response);
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("mapDevicesToDsts.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
}
