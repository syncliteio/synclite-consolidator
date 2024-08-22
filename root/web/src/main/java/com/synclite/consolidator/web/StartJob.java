package com.synclite.consolidator.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class StartJob
 */
@WebServlet("/startJob")
public class StartJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StartJob() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			if (request.getSession().getAttribute("device-data-root") == null) {
				response.sendRedirect("syncLiteTerms.jsp");			
			} else {
				String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String propsPath = Path.of(request.getSession().getAttribute("device-data-root").toString(), "synclite_consolidator.conf").toString();

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
				//Start if the job is not found
				if(currentJobPID == 0) {
					if (request.getSession().getAttribute("device-data-root") != null)  {
						//Get env variable 
						String jvmArgs = "";
						if (request.getSession().getAttribute("jvm-arguments") != null) {
							jvmArgs = request.getSession().getAttribute("jvm-arguments").toString();
						}

						Process p;
						if (isWindows()) {
							String scriptName = "synclite-consolidator.bat";
							String scriptPath = Path.of(corePath, scriptName).toString();

							if (!jvmArgs.isBlank()) {
								try {
									//Delete and re-create a file variables.bat under scriptPath and set the variable JVM_ARGS
									Path varFilePath = Path.of(corePath, "synclite-variables.bat");
									if (Files.exists(varFilePath)) {
										Files.delete(varFilePath);
									}
									String varString = "set \"JVM_ARGS=" + jvmArgs + "\""; 
									Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
								} catch (Exception e) {
									throw new ServletException("Failed to write jvm-arguments to variables.bat file", e);
								}
							}

							String[] cmdArray = {scriptPath, "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
							p = Runtime.getRuntime().exec(cmdArray);						

						} else {
							String scriptName = "synclite-consolidator.sh";
							Path scriptPath = Path.of(corePath, scriptName);

							if (!jvmArgs.isBlank()) {
								try {
									//Delete and re-create a file variables.sh under scriptPath and set the variable JVM_ARGS
									Path varFilePath = Path.of(corePath, "synclite-variables.sh");
									String varString = "JVM_ARGS=\"" + jvmArgs + "\"";
									if (Files.exists(varFilePath)) {
										Files.delete(varFilePath);
									}
									Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
									Set<PosixFilePermission> perms = Files.getPosixFilePermissions(varFilePath);
									perms.add(PosixFilePermission.OWNER_EXECUTE);
									Files.setPosixFilePermissions(varFilePath, perms);
								} catch (Exception e) {
									throw new ServletException("Failed to write jvm-arguments to variables.sh file", e);
								}
							}

							// Get the current set of script permissions
							Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
							// Add the execute permission if it is not already set
							if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
								perms.add(PosixFilePermission.OWNER_EXECUTE);
								Files.setPosixFilePermissions(scriptPath, perms);
							}

							String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
							p = Runtime.getRuntime().exec(cmdArray);					
						}
						request.getSession().setAttribute("job-status","STARTED");
						request.getSession().setAttribute("job-type","SYNC");
						request.getSession().setAttribute("job-start-time",System.currentTimeMillis());
						request.getRequestDispatcher("dashboard.jsp").forward(request, response);
					}
					else {
						request.getRequestDispatcher("syncLiteTerms.jsp").forward(request, response);				
					}
				}
				else {
					request.getRequestDispatcher("dashboard.jsp").forward(request, response);
				}
			}
		} catch(Exception e) {
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("jobError.jsp?jobType=StartJob&errorMsg=" + errorMsg).forward(request, response);
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
