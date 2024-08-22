package com.synclite.consolidator.web;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.quartz.DailyTimeIntervalScheduleBuilder;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Servlet implementation class StartJob
 */
@WebServlet("/configureScheduler")
public class ConfigureScheduler extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public ConfigureScheduler () {
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

				String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String jobName = request.getSession().getAttribute("job-name").toString();
				Path confPath = Path.of(deviceDataRoot, "synclite_consolidator_scheduler.conf");				
				String schedulerStatsPath = Path.of(deviceDataRoot, "synclite_consolidator_scheduler_statistics.db").toString();

				initTracer(Path.of(deviceDataRoot));

				setupSchedulerStatsFile(schedulerStatsPath);
				
				Integer numSchedules = Integer.valueOf(request.getSession().getAttribute("synclite-consolidator-scheduler-num-schedules").toString());
				HashMap<String, String> properties = new HashMap<String, String>();
				properties.put("synclite-consolidator-scheduler-num-schedules", request.getSession().getAttribute("synclite-consolidator-scheduler-num-schedules").toString());
				
				for (int idx=1 ; idx <= numSchedules ; ++idx) {					
					String jobStartHourStr = request.getParameter("synclite-consolidator-scheduler-start-hour-" + idx);
					try {
						if (Integer.valueOf(jobStartHourStr) == null) {
							throw new ServletException("Please specify a valid numeric value for \"Job Start Hour\" for schedule number : " + idx);
						} else if ((Integer.valueOf(jobStartHourStr)) < 0 || (Integer.valueOf(jobStartHourStr) > 24)) {
							throw new ServletException("Please specify a value between 0 and 24 for \"Job Start Hour\" for schedule number : " + idx);
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"Job Start Hour\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-start-hour-" + idx, jobStartHourStr);

					String jobStartMinuteStr = request.getParameter("synclite-consolidator-scheduler-start-minute-" + idx);
					try {
						if (Integer.valueOf(jobStartMinuteStr) == null) {
							throw new ServletException("Please specify a valid numeric value for \"Job Start Minute\" for schedule number : " + idx);
						} else if ((Integer.valueOf(jobStartMinuteStr)) < 0 || (Integer.valueOf(jobStartMinuteStr) > 60)) {
							throw new ServletException("Please specify a value between 0 and 24 for \"Job Start Minute\" for schedule number : " + idx);
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"Job Start Minute\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-start-minute-" + idx, jobStartMinuteStr);


					String jobEndHourStr = request.getParameter("synclite-consolidator-scheduler-end-hour-" + idx);
					try {
						if (Integer.valueOf(jobEndHourStr) == null) {
							throw new ServletException("Please specify a valid numeric value for \"Job End Hour\" for schedule number : " + idx);
						} else if ((Integer.valueOf(jobEndHourStr)) < 0 || (Integer.valueOf(jobEndHourStr) > 24)) {
							throw new ServletException("Please specify a value between 0 and 24 for \"Job End Hour\" for schedule number : " + idx);
						}

						if (Integer.valueOf(jobEndHourStr) < Integer.valueOf(jobStartHourStr)) {
							throw new ServletException("Specified \"Job End Hour\" is earlier than the \"Job Start Hour\" for schedule number : " + idx);
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"Job End Hour\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-end-hour-" + idx, jobEndHourStr);


					String jobEndMinuteStr = request.getParameter("synclite-consolidator-scheduler-end-minute-" + idx);
					try {
						if (Integer.valueOf(jobEndMinuteStr) == null) {
							throw new ServletException("Please specify a valid numeric value for \"Job End Minute\" for schedule number : " + idx);
						} else if ((Integer.valueOf(jobEndMinuteStr)) < 0 || (Integer.valueOf(jobEndMinuteStr) > 60)) {
							throw new ServletException("Please specify a value between 0 and 24 for \"Job End Minute\" for schedule number : " + idx);
						}
						if (Integer.valueOf(jobEndHourStr) == Integer.valueOf(jobStartHourStr)) {
							if (Integer.valueOf(jobEndMinuteStr) < Integer.valueOf(jobStartMinuteStr)) {
								throw new ServletException("Specified \"Job End Hour:Minute \" is earlier than the \"Job Start Hour:Minute\" for schedule number : " + idx);
							}
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"Job End Minute\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-end-minute-" + idx, jobEndMinuteStr);


					String jobRunDurationStr = request.getParameter("synclite-consolidator-scheduler-job-run-duration-" + idx);
					try {
						if (Integer.valueOf(jobRunDurationStr) == null) {
							throw new ServletException("Please specify a non-negative numeric value for \"Job Run Duration\" for schedule number : " + idx);
						} else if (Integer.valueOf(jobRunDurationStr) < 0) {
							throw new ServletException("Please specify a non-negative numeric value for \"Job Run Duration\" for schedule number : " + idx);
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a non-negative numeric value for \"Job Run Duration\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-job-run-duration-" + idx, jobRunDurationStr);

					String jobRunDurationUnit = request.getParameter("synclite-consolidator-scheduler-job-run-duration-unit-" + idx);
					properties.put("synclite-consolidator-scheduler-job-run-duration-unit-" + idx, jobRunDurationUnit);

					String jobRunIntervalStr = request.getParameter("synclite-consolidator-scheduler-job-run-interval-" + idx);
					try {
						if (Integer.valueOf(jobRunIntervalStr) == null) {
							throw new ServletException("Please specify a non-negative numeric value for \"Job Run Interval\" for schedule number : " + idx);
						} else if (Integer.valueOf(jobRunIntervalStr) < 0) {
							throw new ServletException("Please specify a non-negative numeric value for \"Job Run Interval\" for schedule number : " + idx);
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a non-negative numeric value for \"Job Run Interval\" for schedule number : " + idx);
					}
					properties.put("synclite-consolidator-scheduler-job-run-interval-" + idx, jobRunIntervalStr);

					String jobRunIntervalUnit = request.getParameter("synclite-consolidator-scheduler-job-run-interval-unit-" + idx);
					properties.put("synclite-consolidator-scheduler-job-run-interval-unit-" + idx, jobRunIntervalUnit);

					String jobTypeStr = request.getParameter("synclite-consolidator-scheduler-job-type-" + idx);
					properties.put("synclite-consolidator-scheduler-job-type-" + idx, jobTypeStr);
					
				}

				//Write out configurations to scheduler conf file.
				
				writeConfigurations(confPath, properties);
				
				Scheduler scheduler = (Scheduler) request.getSession().getAttribute("syncite-consolidator-job-starter-scheduler-" + jobName);
				if (scheduler != null) {
					scheduler.clear();
					if (!scheduler.isShutdown()) {
						scheduler.shutdown();
					}
					request.getSession().setAttribute("syncite-consolidator-job-starter-scheduler-" + jobName, null);
				}

				SchedulerFactory schedulerFactory = new StdSchedulerFactory();
				scheduler = schedulerFactory.getScheduler();
				request.getSession().setAttribute("syncite-consolidator-job-starter-scheduler-" + jobName, scheduler);

				for (int idx=1 ; idx <= numSchedules ; ++idx) {		
					Integer jobRunDurationS = getDurationInSeconds(properties.get("synclite-consolidator-scheduler-job-run-duration-" + idx), properties.get("synclite-consolidator-scheduler-job-run-duration-unit-" + idx));
					long scheduleStartTime = getTimeInMillis(properties.get("synclite-consolidator-scheduler-start-hour-" + idx), properties.get("synclite-consolidator-scheduler-start-minute-" + idx));
					long scheduleEndTime = getTimeInMillis(properties.get("synclite-consolidator-scheduler-end-hour-" + idx), properties.get("synclite-consolidator-scheduler-end-minute-" + idx));
					Integer jobRunIntervalS= getDurationInSeconds(properties.get("synclite-consolidator-scheduler-job-run-interval-" + idx), properties.get("synclite-consolidator-scheduler-job-run-interval-unit-" + idx));
					
					JobDataMap jobDataMap = new JobDataMap();
					jobDataMap.put("session", request.getSession());
					jobDataMap.put("corePath", corePath);
					jobDataMap.put("globalTracer", this.globalTracer);
					jobDataMap.put("jobRunDurationS", jobRunDurationS);
					jobDataMap.put("jobType", properties.get("synclite-consolidator-scheduler-job-type-" + idx));
					jobDataMap.put("scheduleIndex", Integer.valueOf(idx));
					jobDataMap.put("scheduleStartTime", scheduleStartTime);
					jobDataMap.put("scheduleEndTime", scheduleEndTime);
					jobDataMap.put("jobRunIntervalS", jobRunIntervalS);

					JobDetail job = JobBuilder.newJob(JobStarter.class)
							.withIdentity("syncLiteConsolidatorJobStarter-" + idx, "syncLiteConsolidatorJobStarterGroup-" + idx)
							.usingJobData(jobDataMap)
							.build();
					/*
					Trigger trigger = TriggerBuilder.newTrigger()
							.withIdentity("yourTrigger", "group1")
							//.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * 1/1 * ? *")) // Example: Run every 1 minutes
							.withSchedule(CronScheduleBuilder.cronSchedule("0/30 * * * * ?"))
							//.withSchedule(CronScheduleBuilder.) // Example: Run every 5 minutes
							.build();
					 */

					Trigger trigger = null;
					if (jobRunIntervalS > 0) {
						globalTracer.info("Creating a periodic trigger for start time : " + properties.get("synclite-consolidator-scheduler-start-hour-" + idx) + ":" + properties.get("synclite-consolidator-scheduler-start-minute-" + idx) + " and end time : " + properties.get("synclite-consolidator-scheduler-end-hour-" + idx) + ":" + properties.get("synclite-consolidator-scheduler-end-minute-" + idx) + " with run interval : " + jobRunIntervalS + " seconds");
						trigger = TriggerBuilder.newTrigger()
								.withIdentity("syncLiteConsolidatorJobStarter-" + idx, "syncLiteConsolidatorJobStarterGroup-" + idx)
								.withSchedule(DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
										.startingDailyAt(
												TimeOfDay.hourAndMinuteOfDay(
														Integer.valueOf(properties.get("synclite-consolidator-scheduler-start-hour-" + idx)), 
														Integer.valueOf(properties.get("synclite-consolidator-scheduler-start-minute-" + idx))
														)
												)
										.endingDailyAt(
												TimeOfDay.hourAndMinuteOfDay(
														Integer.valueOf(properties.get("synclite-consolidator-scheduler-end-hour-" + idx)), 
														Integer.valueOf(properties.get("synclite-consolidator-scheduler-end-minute-" + idx))
														)
												)
										.withIntervalInSeconds(jobRunIntervalS)
										.withMisfireHandlingInstructionFireAndProceed()									
										)
								.build();
					} else {
						globalTracer.info("Creating one time trigger for start time : " + properties.get("synclite-consolidator-scheduler-start-hour-" + idx) + " and " + properties.get("synclite-consolidator-scheduler-start-minute-" + idx));

						trigger = TriggerBuilder.newTrigger()
								.withIdentity("syncLiteConsolidatorJobStarter-" + idx, "syncLiteConsolidatorJobStarterGroup-" + idx)
								.startAt(DateBuilder.todayAt(
										Integer.valueOf(properties.get("synclite-consolidator-scheduler-start-hour-" + idx)), 
										Integer.valueOf(properties.get("synclite-consolidator-scheduler-start-minute-" + idx)),
										0
										)										
										).build();

					}

					scheduler.scheduleJob(job, trigger);
					this.globalTracer.info("Added scheduled job");
				}
				scheduler.start();
				this.globalTracer.info("Started schedule");

				//request.getRequestDispatcher("dashboard.jsp").forward(request, response);
				response.sendRedirect("dashboard.jsp");
			}
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			this.globalTracer.error("Failed to configure and schedule job : " + e.getMessage(), e);
			request.getRequestDispatcher("configureScheduler.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private void setupSchedulerStatsFile(String schedulerStatsPath) throws ServletException {
		String url = "jdbc:sqlite:" + schedulerStatsPath;
		String createTableSql = "CREATE TABLE IF NOT EXISTS statistics(trigger_id long, schedule_index int, schedule_start_time long, schedule_end_time long, job_run_interval_s int, job_run_duration_s int, job_start_time long, job_start_status text, job_start_status_description text, job_stop_time long, job_stop_status text, job_stop_status_description text, PRIMARY KEY(trigger_id, schedule_index))";
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(createTableSql);
			}
		} catch (Exception e) {
			String errorMsg = "Failed to create scheduler statistics table in scheduler statistics db file : " + schedulerStatsPath  + e.getMessage();
			this.globalTracer.error(errorMsg, e);
			throw new ServletException(errorMsg, e);
		}		
	}

	private long getTimeInMillis(String hour, String minute) {
	      // Get today's date at 12:00 AM
        LocalDateTime todayMidnight = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);

        // Add given hour and minute
        LocalDateTime targetDateTime = todayMidnight.plusHours(Long.valueOf(hour)).plusMinutes(Long.valueOf(minute));

        // Convert to milliseconds since the epoch
        long millisSinceEpoch = targetDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return millisSinceEpoch;
	}

	private Integer getDurationInSeconds(String val, String unit) {
		switch (unit) {
		case "SECONDS":
			return Integer.valueOf(val);
		case "MINUTES":
			return Integer.valueOf(val) * 60;
		case "HOURS":
			return Integer.valueOf(val) * 60 * 60;			
		}
		return 0;
	}

	private void writeConfigurations(Path confPath, HashMap<String, String> properties) throws ServletException {
		try {
			StringBuilder confBuilder = new StringBuilder();
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				confBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
			}
			Files.writeString(confPath, confBuilder.toString());
		} catch (Exception e) {
			globalTracer.error("Failed to write configurations to scheduler config file : " + confPath + " : " + e.getMessage(), e);
			throw new ServletException("Failed to write configurations to scheduler config file : " + confPath + " : " + e.getMessage(), e);
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
		if (this.globalTracer.getAppender("ConsolidatorTracer") == null) {
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
