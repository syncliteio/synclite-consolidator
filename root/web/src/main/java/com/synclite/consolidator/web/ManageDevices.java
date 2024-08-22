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

import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
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
@WebServlet("/manageDevices")
public class ManageDevices extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ManageDevices() {
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

			StringBuilder builder = new StringBuilder();			

			//Validate if a job is loaded/started 
			if (request.getSession().getAttribute("device-data-root") == null) {
				throw new ServletException("Please load a SyncLite consolidator job first and then proceed to manage devices");
			}

			String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();

			String manageDevicesNameList = null;
			if (request.getParameter("manage-devices-name-list") != null) {
				manageDevicesNameList = request.getParameter("manage-devices-name-list");
				builder.append("manage-devices-name-list = ").append(manageDevicesNameList).append("\n");
			}
			
			String manageDevicesNamePattern = null;
			if (request.getParameter("manage-devices-name-pattern") != null) {
				manageDevicesNamePattern = request.getParameter("manage-devices-name-pattern");
				try {
					if (manageDevicesNamePattern.trim().isEmpty()) {
						throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device Name Pattern\"");
					}
					else if (Pattern.compile(manageDevicesNamePattern) == null) {
						throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device Name Pattern\"");
					}				
					builder.append("manage-devices-name-pattern = ").append(manageDevicesNamePattern).append("\n");
				} catch (PatternSyntaxException e) {
					throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device Name Pattern\"");
				}				
			}

			String manageDevicesIDList = null;
			if (request.getParameter("manage-devices-id-list") != null) {
				manageDevicesIDList = request.getParameter("manage-devices-id-list");
				builder.append("manage-devices-id-list = ").append(manageDevicesIDList).append("\n");
			}

			String manageDevicesIDPattern = null;
			if (request.getParameter("manage-devices-id-pattern") != null) {
				manageDevicesIDPattern = request.getParameter("manage-devices-id-pattern");
				try {
					if (manageDevicesIDPattern.trim().isEmpty()) {
						throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device ID Pattern\"");
					}				
					else if (Pattern.compile(manageDevicesIDPattern) == null) {
						throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device ID Pattern\"");
					}
					builder.append("manage-devices-id-pattern = ").append(manageDevicesIDPattern).append("\n");
				} catch (PatternSyntaxException e) {
					throw new ServletException("Please specify valid (Java) regular expression pattern for \"Device ID Pattern\"");
				}
			}
			
			if ((manageDevicesNameList == null) && (manageDevicesNamePattern == null) && (manageDevicesIDList == null) && (manageDevicesIDPattern == null)){
				throw new ServletException("Please specify one of the four : \"Device Name List\" or \"Device Name Pattern\" or \"Device ID List\" or \"Device ID Pattern\"");
			}

			String manageDevicesOperationType = "ENABLE_DEVICES";
			if (request.getParameter("manage-devices-operation-type") != null) {
				manageDevicesOperationType = request.getParameter("manage-devices-operation-type");
			}
			builder.append("manage-devices-operation-type = ").append(manageDevicesOperationType).append("\n");

			if (manageDevicesOperationType.equals("REMOVE_DEVICES")) {
				String removeDevicesFromDst = "false";
				if (request.getParameter("remove-devices-from-dst") != null) {
					removeDevicesFromDst = request.getParameter("remove-devices-from-dst");
					try {
						if (Boolean.valueOf(removeDevicesFromDst) == null) {
							throw new ServletException("Please specify a valid boolean value for \"Cleanup Devices From Destination DB\"");
						} 
						builder.append("remove-devices-from-dst = ").append(removeDevicesFromDst).append("\n");
					} catch (IllegalArgumentException e) {
						throw new ServletException("Please specify a valid boolean value for \"Cleanup Devices From Destination DB\"");
					}
				} else {
					throw new ServletException("Please specify a valid boolean value for \"Cleanup Devices From Destination DB\"");
				}

				String removeDevicesConfirmation = request.getParameter("remove-devices-confirmation");
				if (removeDevicesConfirmation.trim().isEmpty()) {
					throw new ServletException("Please type \"Remove Devices\" in the \"Confirm Remove Devices\" textbox to confirm device removal");
				} else if (!removeDevicesConfirmation.equals("Remove Devices")) {
					throw new ServletException("Please type \"Remove Devices\" in the \"Confirm Remove Devices\" textbox to confirm device removal");
				}
			}

			String deviceCommand = "";
			String deviceCommandDetails = "";
			if (manageDevicesOperationType.equals("COMMAND_DEVICES")) {
				deviceCommand = request.getParameter("device-command");
				
				if (deviceCommand == null) {
					throw new ServletException("Please specify \"Device Command\"");
				}
				
				if (deviceCommand.trim().isEmpty()) {
					throw new ServletException("Please specify \"Device Command\"");
				}

				if (! deviceCommand.matches("[a-zA-Z0-9]+")) {
					throw new ServletException("Please specify a valid \"Device Command\" which contains only alphanumeric characters");
				}
				
				if (deviceCommand.length() > 32) {
					throw new ServletException("Please specify a valid \"Device Command\" which contains maximum 32 alphanumeric characters");
				}
				
				builder.append("device-command = ").append(deviceCommand).append("\n");
				
				if (request.getParameter("device-command-details") != null) {
					deviceCommandDetails = request.getParameter("device-command-details");
					builder.append("device-command-details = ").append(deviceCommandDetails).append("\n");
				}
			}			
			
			//Validate if a job is running. If running then ask to stop first.
			long currentJobPID = 0;
			Process jpsProc;
			if (isWindows()) {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "\\bin\\jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			} else {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "/bin/jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			}

			BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
			String line = stdout.readLine();
			while (line != null) {
				if (line.contains("com.synclite.consolidator.Main") && line.contains(deviceDataRoot)) {
					currentJobPID = Long.valueOf(line.split(" ")[0]);
				}
				line = stdout.readLine();
			}

			boolean doOnlineRequest = false;
			String enableRequestProcessorStr = request.getSession().getAttribute("enable-request-processor").toString();
			
			if (currentJobPID > 0) {
				if (enableRequestProcessorStr.equals("true")) {
					if (manageDevicesOperationType.equals("COMMAND_DEVICES")) {
						//Do an online request as the job is running
						doOnlineRequest = true;
					} else {
						throw new ServletException("A SyncLite Job is running with process ID :" + currentJobPID + ". Please stop the running job first to proceed with device management.");
					}
				} else {
					throw new ServletException("A SyncLite Job is running with process ID :" + currentJobPID + ". Please stop the running job first to proceed with device management.");
				}
			}

			//doOnlineRequest = true;
			
			if (!doOnlineRequest) {
				//Offline request by starting a MANAGE_DEVICES job
				Path consolidatorConfFile = Path.of(deviceDataRoot).resolve("synclite_consolidator.conf");
				if (! Files.exists(consolidatorConfFile)) {
					throw new ServletException("SyncLite Consolidator config file not found : " + consolidatorConfFile + ". Please load a valid SyncLite Consolidator job first.");
				}

				//Create a synclite_consolidator_manage_devices.conf file with device removal details 
				Path manageDevicesConfPath = Path.of(deviceDataRoot).resolve("synclite_consolidator_manage_devices.conf");
				try {
					Files.writeString(manageDevicesConfPath, builder.toString());
				} catch (IOException e) {
					throw new ServletException("Failed to write SyncLite consolidator configurations into file : " + manageDevicesConfPath, e);
				}

				String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();
				String propsPath = Path.of(deviceDataRoot, "synclite_consolidator.conf").toString();

				//Start job 
				Process p;
				if (isWindows()) {
					String scriptName = "synclite-consolidator.bat";
					String scriptPath = Path.of(corePath, scriptName).toString();
					//String cmd = "\"" + scriptPath + "\"" + " sync " + " --work-dir " + "\"" + deviceDataRoot + "\"" + " --config " + "\"" + propsPath + "\"";
					String[] cmdArray = {scriptPath.toString(), "manage-devices", "--work-dir", deviceDataRoot, "--config", propsPath, "--manage-devices-config", manageDevicesConfPath.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				} else {				
					String scriptName = "synclite-consolidator.sh";
					Path scriptPath = Path.of(corePath, scriptName);

					//First add execute permission
					/*
					String [] command = {"/bin/chmod","+x", scriptPath};
					Runtime rt = Runtime.getRuntime();
					Process pr = rt.exec( command );
					pr.waitFor();
					 */

					// Get the current set of script permissions
					Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
					// Add the execute permission if it is not already set
					if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(scriptPath, perms);
					}

					String[] cmdArray = {scriptPath.toString(), "manage-devices", "--work-dir", deviceDataRoot, "--config", propsPath, "--manage-devices-config", manageDevicesConfPath.toString()};
					p = Runtime.getRuntime().exec(cmdArray);
				}
				//int exitCode = p.exitValue();
				//Thread.sleep(3000);
				Thread.sleep(5000);
				boolean processStatus = p.isAlive();
				if (!processStatus) {
					int exitCode = p.exitValue();
					if (exitCode == 0) {
						//Normal exit
						String successMsg = "Successfully executed " + manageDevicesOperationType + " operation on qualified devices";
						request.getRequestDispatcher("manageDevices.jsp?successMsg=" + successMsg).forward(request, response);
					} else {
						BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
						if ((line = procErr.readLine()) != null) {
							throw new ServletException("Failed to start manage devices consolidator job with exit code : " + exitCode + " and errors : " + line);
						}

						BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
						if ((line = procOut.readLine()) != null) {
							throw new ServletException("Failed to start manage devices consolidator job with exit code : " + exitCode + " and errors : " + line);
						}
						throw new ServletException("Failed to start manage devices consolidator job with exit value : " + exitCode);
					}
				} else {
					request.getSession().setAttribute("job-status","STARTED");
					request.getSession().setAttribute("job-type","MANAGE-DEVICES");
					request.getSession().setAttribute("job-start-time",System.currentTimeMillis());
					request.getRequestDispatcher("dashboard.jsp").forward(request, response);
				}
			} else {
				//Make an online request to running job for the COMMAND_DEVICES
				String reply = "ERROR: Unknown failure";
				if (manageDevicesOperationType.equals("COMMAND_DEVICES")) {
					try (ZContext context = new ZContext()) {								
						//  Socket to talk to server
						ZMQ.Socket socket = context.createSocket(SocketType.REQ);
						// Set the ZAP_DOMAIN option to accept requests only from localhost
				        socket.setZAPDomain("tcp://localhost");
						int port = Integer.valueOf(request.getSession().getAttribute("request-processor-port").toString());
						socket.connect("tcp://localhost:" + port);
						JSONObject jsonObj = new JSONObject();
						jsonObj.put("type", "COMMAND_DEVICES");

						if (manageDevicesNameList != null) {
							jsonObj.put("command-devices-name-list", manageDevicesNameList);
						}						
						if (manageDevicesNamePattern != null) {
							jsonObj.put("command-devices-name-pattern", manageDevicesNamePattern);
						}
						if (manageDevicesIDList != null) {
							jsonObj.put("command-devices-id-list", manageDevicesIDList);
						}						
						if (manageDevicesIDPattern != null) {
							jsonObj.put("command-devices-id-pattern", manageDevicesIDPattern);
						}
						jsonObj.put("device-command", deviceCommand);
						jsonObj.put("device-command-details", deviceCommandDetails);

						socket.send(jsonObj.toString().getBytes(ZMQ.CHARSET), 0);

						reply= new String(socket.recv(0), ZMQ.CHARSET);
		                socket.close();						
					}
				}
				if (reply.startsWith("SUCCESS")) {
					request.getRequestDispatcher("manageDevices.jsp?successMsg=" + reply).forward(request, response);
				} else {
					request.getRequestDispatcher("manageDevices.jsp?errorMsg=" + reply).forward(request, response);
				}
			}
			//System.out.println("process status " + processStatus);
			//System.out.println("process output line " + line);
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("manageDevices.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
}
