package com.synclite.consolidator.web;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SaveJobConfiguration
 */
@WebServlet("/editConfigurations")
public class EditConfigurations extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public EditConfigurations() {
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
		try {
			Path deviceDataRoot = Path.of(request.getSession().getAttribute("device-data-root").toString());
			Path confPath = deviceDataRoot.resolve("synclite_consolidator.conf");
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
				if (line.contains("com.synclite.consolidator.Main") && line.contains(deviceDataRoot.toString())) {
					currentJobPID = Long.valueOf(line.split(" ")[0]);
				}
				line = stdout.readLine();
			}
			if(currentJobPID != 0) {
				String errorMessage = "A job is already running with Process ID : " + currentJobPID + ". Please stop the job and then edit configurations.";
				request.getRequestDispatcher("editConfigurations.jsp?errorMsg=" + errorMessage).forward(request, response);
			}  else {
				String confs = request.getParameter("confs");

				//Iterate on all the lines and set all session variables.
				loadSession(request, confs);
				
				//Now dump the contents into the configuration file
				
				Files.writeString(confPath, confs, StandardOpenOption.TRUNCATE_EXISTING);
				response.sendRedirect("editConfigurations.jsp");
			}
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = "Failed to load session with updated configurations : " + e.getMessage();
			request.getRequestDispatcher("editConfigurations.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private final void loadSession(HttpServletRequest request, String configurations) throws IOException {
		//HashMap properties = new HashMap<String, String>();
		BufferedReader reader = null;
		reader = new BufferedReader(new StringReader(configurations));
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

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}
