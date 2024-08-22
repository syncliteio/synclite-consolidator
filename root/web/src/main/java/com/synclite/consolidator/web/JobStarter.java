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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.quartz.DateBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public class JobStarter implements Job {
	private Logger globalTracer;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		String jobType = "";
		Integer jobRunDurationS = 0;
		Integer scheduleIndex = 0;			
		Long scheduleStartTime = 0L;	
		Long scheduleEndTime = 0L;		
		Integer jobRunIntervalS = 0;		
		Long triggerId = 0L;
		Long jobStartTime = System.currentTimeMillis();
		String jobStartStatus = "";
		String jobStartStatusDescription = "";
		
		String deviceDataRoot = null;
		String propsPath = null;
		String schedulerStatsPath = null;

		try {
			JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

			HttpSession session = (HttpSession) jobDataMap.get("session");
			String corePath = (String) jobDataMap.get("corePath");
			globalTracer = (Logger) jobDataMap.get("globalTracer");
			jobType = (String) jobDataMap.get("jobType");
			jobRunDurationS = (Integer) jobDataMap.get("jobRunDurationS");
			scheduleIndex = (Integer) jobDataMap.get("scheduleIndex");			
			scheduleStartTime = (Long) jobDataMap.get("scheduleStartTime");	
			scheduleEndTime = (Long) jobDataMap.get("scheduleEndTime");		
			jobRunIntervalS = (Integer) jobDataMap.get("jobRunIntervalS");

			deviceDataRoot = session.getAttribute("device-data-root").toString();
			propsPath = Path.of(deviceDataRoot, "synclite_consolidator.conf").toString();
			schedulerStatsPath = Path.of(deviceDataRoot, "synclite_consolidator_scheduler_statistics.db").toString();

			globalTracer.info("Scheduler attempting to trigger job with job type :" + jobType + ", with job duration : " + jobRunDurationS + " seconds for schedule index : " + scheduleIndex);

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
			triggerId = System.currentTimeMillis();
			jobStartStatus = "";
			jobStartStatusDescription = "";
			if(currentJobPID == 0) {
				//Get env variable 
				String jvmArgs = "";
				if (session.getAttribute("jvm-arguments") != null) {
					jvmArgs = session.getAttribute("jvm-arguments").toString();
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
					jobStartTime = System.currentTimeMillis();
					jobStartStatus = "SUCCESS";
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
					jobStartTime = System.currentTimeMillis();
					jobStartStatus = "SUCCESS";
				}

				if (jobRunDurationS > 0) {
					globalTracer.info("Adding stopper job to trigger after seconds : " + jobRunDurationS);
					Scheduler scheduler = context.getScheduler();

					JobDataMap argJobDataMap = new JobDataMap();
					argJobDataMap.put("session", session);
					argJobDataMap.put("globalTracer", this.globalTracer);
					argJobDataMap.put("scheduleIndex", scheduleIndex);
					argJobDataMap.put("triggerId", triggerId);

					Trigger trigger = TriggerBuilder.newTrigger()
							.withIdentity("syncLiteConsolidatorJobStopper-" + scheduleIndex, "syncLiteConsolidatorJobStopperGroup-" + scheduleIndex)
							.startAt(DateBuilder.futureDate(jobRunDurationS, DateBuilder.IntervalUnit.SECOND)) 
							.build();	

					JobDetail job = JobBuilder.newJob(JobStopper.class)
							.withIdentity("syncLiteConsolidatorJobStopper-" + scheduleIndex, "syncLiteConsolidatorJobStopperGroup-" + scheduleIndex)
							.usingJobData(argJobDataMap)
							.build();

					JobKey jk= (JobKey) context.get("stopper-job-key");
					if (jk != null) {
						scheduler.deleteJob(jk);
					}
					context.put("stopper-job-key", job.getKey());

					scheduler.scheduleJob(job, trigger);
				}
				session.setAttribute("job-status","STARTED");
				session.setAttribute("job-type","SYNC");
				session.setAttribute("job-start-time",System.currentTimeMillis());
				globalTracer.info("Scheduler triggered job successfully");
			} else {
				jobStartStatus = "SKIPPED";
				jobStartStatusDescription = "Job found running with PID : " + currentJobPID;
				globalTracer.info("Scheduler skipped to trigger job as job found running with PID : " + currentJobPID);
			}

			session.setAttribute("job-status","STARTED");
			session.setAttribute("job-type",jobType);
			session.setAttribute("job-start-time",System.currentTimeMillis());
		} catch (Exception e) {
			jobStartStatus = "ERROR";
			jobStartStatusDescription =  "Failed to trigger job : " + e.getMessage();
			this.globalTracer.error("Failed to trigger scheduled consolidator job : " + e.getMessage(), e);
		} finally {
			if (schedulerStatsPath != null) {
				try {
					//Make an entry in the schedule report.
					addToSchedulerStats(schedulerStatsPath, triggerId, scheduleIndex, scheduleStartTime, scheduleEndTime, jobRunIntervalS, jobRunDurationS, jobStartTime, jobStartStatus, jobStartStatusDescription);
				} catch (Exception e) {
					this.globalTracer.error("Failed to log an etry in schedule statistics file : " + e.getMessage(), e);
				}			
			}
		}
	}

	private void addToSchedulerStats(String schedulerStatsPath, long triggerId, int scheduleIndex, long scheduleStartTime, long scheduleEndTime, int jobRunIntervalS, int jobRunDurationS, long jobStartTime, String jobStartStatus, String jobStartStatusDescription) {	
		String url = "jdbc:sqlite:" + schedulerStatsPath;
		StringBuilder insertSqlBuilder = new StringBuilder();
		insertSqlBuilder.append("INSERT INTO statistics(trigger_id, schedule_index, schedule_start_time, schedule_end_time, job_run_interval_s, job_run_duration_s, job_start_time, job_start_status, job_start_status_description, job_stop_time, job_stop_status, job_stop_status_description) VALUES (");
		insertSqlBuilder.append(triggerId);
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(scheduleIndex);
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(scheduleStartTime);
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(scheduleEndTime);		
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(jobRunIntervalS);		
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(jobRunDurationS);		
		insertSqlBuilder.append(",");
		insertSqlBuilder.append(jobStartTime);
		insertSqlBuilder.append(",'");
		insertSqlBuilder.append(jobStartStatus);
		insertSqlBuilder.append("','");
		insertSqlBuilder.append(jobStartStatusDescription);
		insertSqlBuilder.append("',");
		insertSqlBuilder.append(0);
		insertSqlBuilder.append(",");
		insertSqlBuilder.append("''");
		insertSqlBuilder.append(",");
		insertSqlBuilder.append("'')");

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(insertSqlBuilder.toString());
			}
		} catch (Exception e) {
			this.globalTracer.error("Failed to log an entry of job trigger event in the stats file : " + schedulerStatsPath + " : sql : "  + insertSqlBuilder.toString() + ", error : " + e.getMessage(), e);
		}
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}

}
