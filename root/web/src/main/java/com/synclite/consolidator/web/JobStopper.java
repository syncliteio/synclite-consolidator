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
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class JobStopper implements Job {
	private Logger globalTracer;
	
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {	
	
		Integer scheduleIndex = 0;			
		Long triggerId = 0L;
		Long jobStopTime = System.currentTimeMillis();
		String jobStopStatus = "";
		String jobStopStatusDescription = "";
		
		String deviceDataRoot = null;
		String schedulerStatsPath = null;

		try {
			
	        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

			HttpSession session = (HttpSession) jobDataMap.get("session");
			globalTracer = (Logger) jobDataMap.get("globalTracer");
			scheduleIndex = (Integer) jobDataMap.get("scheduleIndex");
			triggerId = (Long) jobDataMap.get("triggerId");

			deviceDataRoot = session.getAttribute("device-data-root").toString();
			schedulerStatsPath = Path.of(deviceDataRoot, "synclite_consolidator_scheduler_statistics.db").toString();

			globalTracer.info("Scheduler attempting to stop job started by schedule : " + scheduleIndex);

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
				jobStopTime = System.currentTimeMillis();
				jobStopStatus = "SUCCESS";
				
				session.setAttribute("job-status","STOPPED");
				globalTracer.info("Scheduler stopped job successfully with PID : " + currentJobPID);
			} else {
				jobStopStatus = "SKIPPED";
				jobStopStatusDescription = "No job running";
				globalTracer.info("Scheduler skipped attempting to stop job as no job found running");
			}
		} catch (Exception e) {
			String errorMsg = "Failed to stop scheduled consolidator job : " + e.getMessage();
			jobStopStatus = "ERROR";
			jobStopStatusDescription = errorMsg;
			this.globalTracer.error(errorMsg, e);
		} finally {
			if (schedulerStatsPath != null) {
				try {
					//Make an entry in the schedule report.
					updateSchedulerStats(schedulerStatsPath, triggerId, scheduleIndex, jobStopTime, jobStopStatus, jobStopStatusDescription);
				} catch (Exception e) {
					this.globalTracer.error("Failed to log an etry in schedule statistics file : " + e.getMessage(), e);
				}			
			}
		}
	}

	private void updateSchedulerStats(String schedulerStatsPath, long triggerId, int scheduleIndex, long jobStopTime, String jobStopStatus, String jobStopStatusDescription) {	
		String url = "jdbc:sqlite:" + schedulerStatsPath;
		StringBuilder updateSqlBuilder = new StringBuilder();
		updateSqlBuilder.append("UPDATE statistics SET ");
		updateSqlBuilder.append("job_stop_time = ").append(jobStopTime).append(", ");
		updateSqlBuilder.append("job_stop_status = '").append(jobStopStatus).append("', ");
		updateSqlBuilder.append("job_stop_status_description = '").append(jobStopStatusDescription).append("' ");
		updateSqlBuilder.append("WHERE ");
		updateSqlBuilder.append("trigger_id = ").append(triggerId).append(" AND ");
		updateSqlBuilder.append("schedule_index = ").append(scheduleIndex);
	
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(updateSqlBuilder.toString());
			}
		} catch (Exception e) {
			this.globalTracer.error("Failed to update a job trigger event entry in stats db file : " + schedulerStatsPath + " : sql : " + updateSqlBuilder.toString() + ", error : "+ e.getMessage(), e);
		}
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}
