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
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateJobMonitorConfiguration")
public class ValidateJobMonitorConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateJobMonitorConfiguration() {
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

			String updateStatisticsIntervalSStr = request.getParameter("update-statistics-interval-s");
			try {
				if (Long.valueOf(updateStatisticsIntervalSStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Statistics Update Interval\"");
				} else if (Long.valueOf(updateStatisticsIntervalSStr) <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Statistics Update Interval\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Statistics Update Interval\"");
			}

			String enablePrometheusStatisticsPublisherStr = "false";
			String prometheusPushGatewayURLStr = "";
			String prometheusStatisticsPublisherIntervalSStr = "";
			if (request.getParameter("enable-prometheus-statistics-publisher") != null) {
				enablePrometheusStatisticsPublisherStr = request.getParameter("enable-prometheus-statistics-publisher");
				try {
					if (Boolean.valueOf(enablePrometheusStatisticsPublisherStr) == null) {
						throw new ServletException("Please specify a valid boolean value for \"Enable Prometheus Statistics Publisher\"");
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid boolean value for \"Enable Prometheus Statistics Publisher\"");
				}
			}

			if (enablePrometheusStatisticsPublisherStr.equals("true")) {
				if (request.getParameter("prometheus-push-gateway-url") != null) {
					prometheusPushGatewayURLStr = request.getParameter("prometheus-push-gateway-url");
					if (prometheusPushGatewayURLStr.isBlank()) {
						throw new ServletException("Please specify a non-blank value for \"Prometheus Push Gateway URL\"");
					}					
					try {
						URL url = new URL(prometheusPushGatewayURLStr);
					} catch (Exception e) {
						throw new ServletException("Please specify a valid URL for \"Prometheus Push Gateway\" : ", e);
					}
				} else {
					throw new ServletException("Please specify \"Prometheus Push Gateway URL\"");
				}
				
				prometheusStatisticsPublisherIntervalSStr = request.getParameter("prometheus-statistics-publisher-interval-s");
				try {
					if (Long.valueOf(prometheusStatisticsPublisherIntervalSStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"Prometheus Statistics Publisher Interval\"");
					} else if (Long.valueOf(prometheusStatisticsPublisherIntervalSStr) <= 0 ) {
						throw new ServletException("Please specify a positive numeric value for \"Prometheus Statistics Publisher Interval\"");
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"Prometheus Statistics Publisher Interval\"");
				}				
			}
			
			request.getSession().setAttribute("update-statistics-interval-s",updateStatisticsIntervalSStr);
			request.getSession().setAttribute("enable-prometheus-statistics-publisher",enablePrometheusStatisticsPublisherStr);
			if (enablePrometheusStatisticsPublisherStr.equals("true")) {
				request.getSession().setAttribute("prometheus-push-gateway-url",prometheusPushGatewayURLStr);
				request.getSession().setAttribute("prometheus-statistics-publisher-interval-s",prometheusStatisticsPublisherIntervalSStr);
			} else {
				request.getSession().removeAttribute("prometheus-push-gateway-url");
				request.getSession().removeAttribute("prometheus-statistics-publisher-interval-s");
			}
			//request.getRequestDispatcher("configureDeviceStage.jsp").forward(request, response);
			response.sendRedirect("configureDeviceStage.jsp");
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureJobMonitor.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
