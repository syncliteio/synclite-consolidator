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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateJobConfiguration")
public class ValidateJobConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateJobConfiguration() {
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
				throw new ServletException("\"Data Directory\" must be specified");
			} else {
				deviceDataRootPath = Path.of(deviceDataRoot);
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

			/*
			String deviceUploadRoot = request.getParameter("device-upload-root");
			Path deviceUploadRootPath;
			if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
				throw new ServletException("\"Upload Directory\" must be specified");
			} else {
				deviceUploadRootPath = Path.of(deviceUploadRoot);
				
				//TODO : Ubuntu support
				if (! Files.exists(deviceUploadRootPath)) {
					//If user is using default path then try creating these directories
					try {
						Files.createDirectories(deviceUploadRootPath);
					} catch (Exception e) {
						//Ignore
					}
				}

				if (! Files.isDirectory(deviceUploadRootPath)) {
					throw new ServletException("Specified \"Upload Directory\" : " + deviceUploadRoot + " does not exist, please specify a valid \"Upload Directory\"");
				}

				if (! deviceDataRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"Upload Directory\" does not have read permission");
				}				
			}

			if (! deviceUploadRootPath.toFile().canRead()) {
				throw new ServletException("Specified \"Upload Directory\" does not have read permission");
			}
*/
			/*
			String licenseFile = request.getParameter("license-file");
			Path licenseFilePath;
			if ((licenseFile == null) || licenseFile.trim().isEmpty()) {
				throw new ServletException("\"License file\" must be specified");
			} else {
				licenseFilePath = Path.of(licenseFile);
				if (! Files.exists(licenseFilePath)) {
					throw new ServletException("Specified \"License File \" : " + licenseFilePath + " does not exist, please specify a valid \"License File\"");
				}
			}
			*/
			
			String dstSyncMode = request.getParameter("dst-sync-mode");
			
			String numDeviceProcessors = request.getParameter("num-device-processors");
			try {
				if (Long.valueOf(numDeviceProcessors) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Device Processors\"");
				} else if (Long.valueOf(numDeviceProcessors) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Device Processors\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Device Processors\"");
			}
		

			String deviceNamePattern = request.getParameter("device-name-pattern");			
			try {
				if (Pattern.compile(deviceNamePattern) == null) {
					throw new ServletException("Please specify valid pattern for \"Allowed Device Name Pattern\"");
				}				
			} catch (PatternSyntaxException e) {
				throw new ServletException("Please specify valid pattern for \"Allowed Device Name Pattern\"");
			}
			
			String deviceIDPattern = request.getParameter("device-id-pattern");
			try {
				if (Pattern.compile(deviceIDPattern) == null) {
					throw new ServletException("Please specify valid pattern for \"Allowed Device ID Pattern\"");
				}				
			} catch (PatternSyntaxException e) {
				throw new ServletException("Please specify valid pattern for \"Allowed Device ID Pattern\"");
			}
				

			String enableReplicasForTelemetryDevicesStr = "false";
			Boolean enableReplicasForTelemetryDevices = false;
			if (request.getParameter("enable-replicas-for-telemetry-devices") != null) {
				enableReplicasForTelemetryDevicesStr = request.getParameter("enable-replicas-for-telemetry-devices");
				try {
					if (Boolean.valueOf(enableReplicasForTelemetryDevices) == null) {
						throw new ServletException("Please specify a valid boolean value for \"Enable Replicas For Telemetry Devices\"");
					} else {
						enableReplicasForTelemetryDevices = Boolean.valueOf(enableReplicasForTelemetryDevicesStr);
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid boolean value for \"Enable Replicas For Telemetry Devices\"");
				}
			}

			String disableReplicasForAppenderDevicesStr = "false";
			if (request.getParameter("disable-replicas-for-appender-devices") != null) {
				disableReplicasForAppenderDevicesStr = request.getParameter("disable-replicas-for-appender-devices");
				try {
					if (Boolean.valueOf(disableReplicasForAppenderDevicesStr) == null) {
						throw new ServletException("Please specify a valid boolean value for \"Disable Replicas For Appender Devices\"");
					} 
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid boolean value for \"Disable Replicas For Appender Devices\"");
				}
			}

			String skipBadTxnFilesStr = "false";
			if (request.getParameter("skip-bad-txn-files") != null) {
				skipBadTxnFilesStr = request.getParameter("skip-bad-txn-files");
				try {
					if (Boolean.valueOf(skipBadTxnFilesStr) == null) {
						throw new ServletException("Please specify a valid boolean value for \"Skip Missing/Corrupt Transaction Files\"");
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid boolean value for \"Skip Missing/Corrupt Transaction Files\"");
				}
			}

			String failedDeviceRetryIntervalS = request.getParameter("failed-device-retry-interval-s");
			try {
				if (Long.valueOf(failedDeviceRetryIntervalS) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Failed Device Retry Interval\"");
				} else if (Long.valueOf(failedDeviceRetryIntervalS) <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Failed Device Retry Interval\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Failed Device Retry Interval\"");
			}

			String deviceTraceLevel = request.getParameter("device-trace-level");
			String jvmArguments = null;
			if (request.getParameter("jvm-arguments") != null) {
				if (!request.getParameter("jvm-arguments").isBlank()) {
					jvmArguments = request.getParameter("jvm-arguments");
				}
			}

			String enableRequestProcessorStr = request.getParameter("enable-request-processor");
			try {
				if (Boolean.valueOf(enableRequestProcessorStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Enable Online Request Processor\"");
				} 
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Enable Online Request Processor\"");
			}

			String requestProcessorPortStr = null;
			if (enableRequestProcessorStr.equals("true")) {
				requestProcessorPortStr = request.getParameter("request-processor-port");
				try {
					if (Integer.valueOf(requestProcessorPortStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"Online Request Processor Port\"");
					} else if (Integer.valueOf(requestProcessorPortStr) <= 0) {
						throw new ServletException("Please specify a positive numeric value for \"Online Request Processor Port\"");
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"Online Request Processor Port\"");
				}
			}

			String enableDeviceCommandHandlerStr = request.getParameter("enable-device-command-handler");
			try {
				if (Boolean.valueOf(enableDeviceCommandHandlerStr) == null) {
					throw new ServletException("Please specify a valid boolean value for \"Enable Device Command Handler\"");
				} 
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid boolean value for \"Enable Device Command Handler\"");
			}

			String deviceCommandTimeoutSStr = null;
			if (enableDeviceCommandHandlerStr.equals("true")) {
				deviceCommandTimeoutSStr = request.getParameter("device-command-timeout-s");
				try {
					if (Long.valueOf(deviceCommandTimeoutSStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"Device Command Timeout (s)\"");
					} else if (Integer.valueOf(deviceCommandTimeoutSStr) <= 0 ) {
						throw new ServletException("Please specify a positive numeric value for \"Device Command Timeout (s)\"");
					}
				} catch(NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"Device Command Timeout (s)\"");
				}
			}
			
			String srcAppType = request.getParameter("src-app-type");			
			request.getSession().setAttribute("device-data-root",deviceDataRoot); 
			//request.getSession().setAttribute("device-upload-root", deviceUploadRoot);
			request.getSession().setAttribute("src-app-type", srcAppType);
			//request.getSession().setAttribute("license-file",licenseFile); 
			request.getSession().setAttribute("dst-sync-mode",dstSyncMode);
			request.getSession().setAttribute("num-device-processors",numDeviceProcessors);
			request.getSession().setAttribute("device-name-pattern",deviceNamePattern);
			request.getSession().setAttribute("device-id-pattern",deviceNamePattern);
			request.getSession().setAttribute("enable-replicas-for-telemetry-devices",enableReplicasForTelemetryDevicesStr);
			request.getSession().setAttribute("disable-replicas-for-appender-devices",disableReplicasForAppenderDevicesStr);
			request.getSession().setAttribute("skip-bad-txn-files",skipBadTxnFilesStr);
			request.getSession().setAttribute("failed-device-retry-interval-s",failedDeviceRetryIntervalS);
			request.getSession().setAttribute("device-trace-level",deviceTraceLevel);
			if (jvmArguments != null) {
				request.getSession().setAttribute("jvm-arguments",jvmArguments);
			} else {
				request.getSession().removeAttribute("jvm-arguments");
			}
			request.getSession().setAttribute("enable-request-processor", enableRequestProcessorStr);
			if (enableRequestProcessorStr.equals("true")) {
				request.getSession().setAttribute("request-processor-port", requestProcessorPortStr);
			} else {
				request.getSession().removeAttribute("request-processor-port");
			}
			request.getSession().setAttribute("enable-device-command-handler", enableDeviceCommandHandlerStr);
			if (enableDeviceCommandHandlerStr.equals("true")) {
				request.getSession().setAttribute("device-command-timeout-s", deviceCommandTimeoutSStr);
			} else {
				request.getSession().removeAttribute("device-command-timeout-s");			
			}

			//request.getRequestDispatcher("configureJobMonitor.jsp").forward(request, response);
			response.sendRedirect("configureJobMonitor.jsp");
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureJob.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
