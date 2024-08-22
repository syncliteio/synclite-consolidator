package com.synclite.consolidator.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class StopJob
 */
@WebServlet("/stopJob")
public class StopJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StopJob() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			//Get current job PID if running

			if (request.getSession().getAttribute("device-data-root") == null) {
				response.sendRedirect("syncLiteTerms.jsp");			
			} else {
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
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
				//stdout.close();

				//Kill job if found

				if(currentJobPID > 0) {
					if (isWindows()) {
						Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
					} else {
						Runtime.getRuntime().exec("kill -9 " + currentJobPID);
					}
				}
				request.getSession().setAttribute("job-status","STOPPED");
				request.getRequestDispatcher("dashboard.jsp").forward(request, response);
			}
		} catch(Exception e) {
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("jobError.jsp?jobType=StopJob&errorMsg=" + errorMsg).forward(request, response);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}
