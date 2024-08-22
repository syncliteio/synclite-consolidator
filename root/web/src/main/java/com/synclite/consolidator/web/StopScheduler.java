package com.synclite.consolidator.web;

import java.io.IOException;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.quartz.Scheduler;

/**
 * Servlet implementation class StartJob
 */
@WebServlet("/stopScheduler")
public class StopScheduler extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StopScheduler() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			if (request.getSession().getAttribute("device-data-root") == null) {
				response.sendRedirect("syncLiteTerms.jsp");
			} else {

				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String jobName = request.getSession().getAttribute("job-name").toString();

				initTracer(Path.of(deviceDataRoot));

				Scheduler scheduler = (Scheduler) request.getSession().getAttribute("syncite-consolidator-job-starter-scheduler-" + jobName);
				if (scheduler != null) {
					if (!scheduler.isShutdown()) {
						scheduler.shutdown();
					}
					request.getSession().setAttribute("syncite-consolidator-job-starter-scheduler-" + jobName, null);
				}

				Scheduler stopperScheduler = (Scheduler) request.getSession().getAttribute("syncite-consolidator-job-stopper-scheduler-" + jobName);
				if (stopperScheduler != null) {
					if (!stopperScheduler.isShutdown()) {
						stopperScheduler.shutdown();
					}
					request.getSession().setAttribute("syncite-consolidator-job-stopper-scheduler-" + jobName, null);
				}

				request.getRequestDispatcher("dashboard.jsp").forward(request, response);
			}
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			this.globalTracer.error("Failed to stop job scheduler : " + e.getMessage(), e);
			request.getRequestDispatcher("jobError.jsp?jobType=StopReadJobScheduler&errorMsg=" + errorMsg).forward(request, response);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ValidateJobConfiguration.class);
		if (this.globalTracer.getAppender("consolidatorTracer") == null) {
			globalTracer.setLevel(Level.INFO);
			RollingFileAppender fa = new RollingFileAppender();
			fa.setName("consolidatorTracer");
			fa.setFile(workDir.resolve("synclite_consolidator.trace").toString());
			fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
			fa.setMaxBackupIndex(10);
			fa.setAppend(true);
			fa.activateOptions();
			globalTracer.addAppender(fa);
		}
	}
}
