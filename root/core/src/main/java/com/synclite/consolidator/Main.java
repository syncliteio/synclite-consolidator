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

package com.synclite.consolidator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteAppLock;
import com.synclite.consolidator.global.UserCommand;

public class Main {

	private static Path confPath;
	private static Path manageDevicesConfPath;
	public static Path workDir;
	public static UserCommand COMMAND;
	public static SyncLiteAppLock appLock = new SyncLiteAppLock();
	public static long jobStartTime = System.currentTimeMillis();
	public static void main(String[] args) {
		try {
			if (args.length < 5) {
				usage();
			} else {
				String cmd = args[0].trim().toLowerCase();
				switch (cmd) {
				case "sync":
					COMMAND = UserCommand.SYNC;
					if (args.length != 5) {
						usage();
					}
					break;
				case "import":
					COMMAND = UserCommand.IMPORT;
					break;
				case "import-schema":
					COMMAND = UserCommand.IMPORT_SCHEMA;
					break;
				case "export-schema":
					COMMAND = UserCommand.EXPORT_SCHEMA;
					break;
				case "cleanup-schema":
					COMMAND = UserCommand.CLEANUP_SCHEMA;
					break;
				case "reinitialize-devices":
					COMMAND = UserCommand.REINITIALIZE_DEVICES;
					break;
				case "resync-devices":
					COMMAND = UserCommand.RESYNC_DEVICES;
					break;
				case "manage-devices":
					COMMAND = UserCommand.MANAGE_DEVICES;
					if (args.length != 7) {
						usage();
					}
					break;
				default:
					usage();
				}

				if (!args[1].trim().equals("--work-dir")) {
					usage();
				} else {
					workDir = Path.of(args[2]);
					if (!Files.exists(workDir)) {
						error("Invalid work directory specified : " + workDir);
					}
					
					if (!Files.isDirectory(workDir)) {
						error("Invalid work directory specified : " + workDir);
					} 
					
					if (!Files.isWritable(workDir)) {
						error("Specified work directory is not writable : " + workDir);
					}
				}

				
				if (!args[3].trim().equals("--config")) {
					usage();
				} else {
					confPath = Path.of(args[4]);
					if (!Files.exists(confPath)) {
						error("Invalid configuration file specified : " + confPath);
					}
				}
				
				if (COMMAND == UserCommand.MANAGE_DEVICES) {
					if (! args[5].trim().equals("--manage-devices-config")) {
						usage();
					} else {
						manageDevicesConfPath = Path.of(args[6]);
						if (!Files.exists(manageDevicesConfPath)) {
							error("Invalid manage-devices-config file : " + manageDevicesConfPath + " specified");
						}
					}
				}
			}

			ConfLoader.getInstance().loadSyncConfigProperties(confPath);

					/*
		        PropsLoader.deviceRoot = Path.of("/home/ubuntu/sqlite");
		        PropsLoader.dstBatchSize = 5000L;
		        PropsLoader.dstConnStr = "jdbc:postgresql://localhost:5435/io?user=dbwrap&password=dbwrap&ssl=false";
		        PropsLoader.dstDatabase = "io";
		        PropsLoader.dstSchema = "public";
		        PropsLoader.dstType = DstType.POSTGRESQL;
		        PropsLoader.syncerType = SyncerRole.ALL;
		        PropsLoader.syncMode = SyncMode.CONSOLIDATION;
					 */

			tryLockWorkDir();
			switch (COMMAND) {
			case SYNC:
				SyncDriver driver = SyncDriver.getInstance();
				//Run synchronously for now.
				driver.run();				
				break;
			case MANAGE_DEVICES:
				ConfLoader.getInstance().loadManageDevicesConfigProperties(manageDevicesConfPath);
				ManageDevicesDriver rDriver = ManageDevicesDriver.getInstance();
				rDriver.run();
				System.exit(0);
			}			
		} catch (Exception e) {
			try {				 
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				String stackTrace = sw.toString();
				Path exceptionFilePath; 
				if (workDir == null) { 
					exceptionFilePath = Path.of("synclite_consolidator_exception.trace");
				} else {
					exceptionFilePath = workDir.resolve("synclite_consolidator_exception.trace");
				}
				
				String finalStr = e.getMessage() + "\n" + stackTrace;
				Files.writeString(exceptionFilePath, finalStr);
				System.out.println("ERROR : " + finalStr);
				System.err.println("ERROR : " + finalStr);	
			} catch (Exception ex) {
				System.out.println("ERROR : " + ex);
				System.err.println("ERROR : " + ex);	
			}
			System.exit(1);
		}
	}

	private static final void tryLockWorkDir() throws SyncLiteException {
		appLock.tryLock(workDir);
	}

	private static final void error(String message) throws Exception {
		System.out.println("ERROR : " + message);
		throw new Exception(message);
	}

	private static final void usage() throws Exception {
		//System.out.println("ERROR : Usage: SyncLiteConsolidator <sync|import|import-schema|export-schema|cleanup-schema|reinitialize-devices|resync-devices|manage-devices> --config <path/to/props-file>");
		StringBuilder builder = new StringBuilder();
		builder.append("ERROR : Usages :").append("\n");
		builder.append("1. SyncLiteConsolidator sync --work-dir <path/to/work/directory> --config <path/to/config-file>").append("\n");
		builder.append("2. SyncLiteConsolidator manage-devices --work-dir <path/to/work/directory> --config <path/to/config-file> --manage-devices-config <path/to/manage-config-file>").append("\n");		
		System.out.println(builder.toString());
		throw new Exception(builder.toString());
	}
}
