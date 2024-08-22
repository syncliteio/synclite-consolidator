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

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateDestinationDB
 */
@WebServlet("/validateNumSchedules")
public class ValidateNumSchedules extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateNumSchedules() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
			
			String numScedulesStr = request.getParameter("synclite-consolidator-scheduler-num-schedules");
			try {
				if (Integer.valueOf(numScedulesStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Number of Schedules\"");
				} else if (Integer.valueOf(numScedulesStr) <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Number of Schedules\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Number of Schedules\"");
			}

			request.getSession().setAttribute("synclite-consolidator-scheduler-num-schedules", numScedulesStr);
			
			//request.getRequestDispatcher("configureScheduler.jsp").forward(request, response);
			response.sendRedirect("configureScheduler.jsp");
		} catch (Exception e) {
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureNumSchedules.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
