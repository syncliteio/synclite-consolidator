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

import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/*import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
 */


import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.device.DeviceLocator;
import com.synclite.consolidator.device.DeviceStatus;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DeviceSchedulerType;
import com.synclite.consolidator.processor.DSTInitializer;
import com.synclite.consolidator.processor.DeviceLogCleaner;
import com.synclite.consolidator.processor.DeviceProcessor;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.watchdog.Monitor;
import com.synclite.consolidator.watchdog.WatchDogs;


public class SyncDriver implements Runnable{

	private static final class InstanceHolder {
		private static SyncDriver INSTANCE = new SyncDriver();
	}

	public static SyncDriver getInstance() {
		return InstanceHolder.INSTANCE;
	}

	private DeviceLocator locator;
	private Logger globalTracer;
	private Set<Device> devices = Collections.newSetFromMap(new ConcurrentHashMap<>());
	private Set<Device> failedDevices = Collections.newSetFromMap(new ConcurrentHashMap<>());
	// private HashMap<Device, List<>>
	private final BlockingQueue<Device> tasks = new LinkedBlockingQueue<Device>(Integer.MAX_VALUE);
	private ScheduledExecutorService deviceLocator;
	private ExecutorService deviceDetector;
	private ExecutorService deviceScheduler;
	private ExecutorService deviceLocatorAndScheduler;
	private ExecutorService syncer;
	private ScheduledExecutorService failedDeviceScheduler;
	private ScheduledExecutorService deviceRegisterScheduler;
	private WatchService deviceWatcher;
	private WatchService deviceParentWatcher;
	private static DeviceStageManager deviceDataStageManager;
	private static DeviceStageManager deviceCommandStageManager;
	//ScheduledExecutorService externalCommandLoader;

	private SyncDriver() {
	}

	/*
    public static void initLoggerConfig(Path filePath) 
    {
        ConfigurationBuilder< BuiltConfiguration > builder =
                ConfigurationBuilderFactory.newConfigurationBuilder();

        Level traceLevel = Level.ERROR;
        switch (PropsLoader.getInstance().getTraceLevel()) {
        case ERROR:           
        	traceLevel = Level.ERROR;
			break;			
        case INFO:
            traceLevel = Level.INFO;
        	break;
        case DEBUG:
            traceLevel = Level.DEBUG;
        }        

        builder.setStatusLevel(traceLevel);
        builder.setConfigurationName("RollingBuilder");
        // create the console appender
        AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "CONSOLE").addAttribute("target",
                ConsoleAppender.Target.SYSTEM_OUT);
        appenderBuilder.add(builder.newLayout("PatternLayout")
                //addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
				.addAttribute("pattern", "%d %-5p [%c{1}] %m%n"));
        builder.add( appenderBuilder );

        LayoutComponentBuilder layoutBuilder = builder.newLayout("PatternLayout")
                //.addAttribute("pattern", "%d [%t] %-5level: %msg%n");
        		.addAttribute("pattern", "%d %-5p [%c{1}] %m%n");
        ComponentBuilder triggeringPolicy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("CronTriggeringPolicy").addAttribute("schedule", "0 0 0 * * ?"))
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "100M"));
        appenderBuilder = builder.newAppender("rolling", "RollingFile")
                .addAttribute("fileName", filePath.toString())
                .addAttribute("filePattern", "synclite_consolidator_%d{MM-dd-yy}.trace.gz")
                .add(layoutBuilder)
                .addComponent(triggeringPolicy);
        builder.add(appenderBuilder);

        // create the new logger
        builder.add( builder.newLogger( "global", traceLevel )
                .add( builder.newAppenderRef( "rolling" ) )
                .addAttribute( "additivity", false ) );

        builder.add( builder.newRootLogger( traceLevel )
                .add( builder.newAppenderRef( "rolling" ) ) );
        Configurator.initialize(builder.build());    	
    }
	 */
	private final void initLogger() {

		this.globalTracer = Logger.getLogger(SyncDriver.class);    	
		switch (ConfLoader.getInstance().getTraceLevel()) {
		case ERROR:
			globalTracer.setLevel(Level.ERROR);
			break;
		case INFO:
			globalTracer.setLevel(Level.INFO);
			break;
		case DEBUG:
			globalTracer.setLevel(Level.DEBUG);
			break;
		}
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("FileLogger");
		fa.setFile(Path.of(ConfLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10MB"); // Set the maximum file size to 10 MB
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);    	

		//initLoggerConfig(Path.of(PropsLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator.trace"));
		//this.globalLogger = org.apache.logging.log4j.LogManager.getLogger(Driver.class);

		//RollingFileAppender fa = new RollingFileAppender();
		//fa.setName("FileLogger");


		/*     fa.setFile(Path.of(PropsLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator.trace").toString());
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setMaxBackupIndex(10);
        fa.setAppend(true);
        fa.activateOptions();
        globalLogger.addAppender(fa);*/


		/* 	LoggerContext context = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(true);
	    Configuration configuration = context.getConfiguration();
	    Layout<? extends Serializable> old_layout = configuration.getAppender("global").getLayout();

	    //delete old appender/logger
	    configuration.getAppender("global").stop();
	    //configuration.removeLogger(Driver.class);

	    //create new appender/logger
	    LoggerConfig loggerConfig = new LoggerConfig("com.synclite.consolidator.Driver", Level.INFO, false);
	    FileAppender appender = FileAppender.createAppender(Path.of(PropsLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator.trace").toString(), "false", "false", "global", "true", "true", "true",
	            "8192", old_layout, null, "false", "", configuration);
	    appender.start();
	    loggerConfig.addAppender(appender, Level.INFO, null);
	    configuration.addLogger("com.synclite.consolidator.Driver", loggerConfig);
	    context.updateLoggers();
		 */
	}

	public final Logger getTracer() {
		return this.globalTracer;
	}

	public final void stopSyncServices() {
		try {

			if (deviceWatcher != null) {
				deviceWatcher.close();
			}

			if (deviceLocator != null) {
				deviceLocator.shutdownNow();
			}

			if (deviceScheduler != null) {
				deviceScheduler.shutdownNow();
			}

			if (failedDeviceScheduler != null) {
				failedDeviceScheduler.shutdownNow();
			}

			if (deviceRegisterScheduler != null) {
				deviceRegisterScheduler.shutdownNow();
			}

			syncer.shutdownNow();

			if (deviceLocator != null) {
				deviceLocator.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}

			if (deviceScheduler != null) {
				deviceScheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}

			if (failedDeviceScheduler != null) {				
				failedDeviceScheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}

			if (deviceRegisterScheduler != null) {
				deviceRegisterScheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			}

			syncer.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		} catch (InterruptedException e) {
			Thread.interrupted();
		} catch (Exception e) {
			//Ignore
		}
	}

	private final void runSyncServices() throws SyncLiteException {
		try {
			deviceLocatorAndScheduler = Executors.newSingleThreadExecutor();
			deviceLocatorAndScheduler.submit(this::locateDevicesAndStartScheduler);

			DeviceSchedulerType schedulerType = ConfLoader.getInstance().getDeviceSchedulerType();
			if ((schedulerType == DeviceSchedulerType.POLLING) || (schedulerType ==  DeviceSchedulerType.EVENT_BASED)) { 
				if (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.POLLING) { 
					syncer = Executors.newScheduledThreadPool(ConfLoader.getInstance().getNumDeviceProcessors());
					for (int i = 0; i < ConfLoader.getInstance().getNumDeviceProcessors(); ++i) {					
						long devicePollingIntervalMs = ConfLoader.getInstance().getDevicePollingIntervalMs();
						if (devicePollingIntervalMs > 0) {
							((ScheduledExecutorService) syncer).scheduleWithFixedDelay(this::doSync, 0, devicePollingIntervalMs, TimeUnit.MILLISECONDS);
						} else {
							((ScheduledExecutorService) syncer).scheduleWithFixedDelay(this::doSync, 0, 1, TimeUnit.NANOSECONDS);
						}	
					}
				}
				else {
					syncer = Executors.newFixedThreadPool(ConfLoader.getInstance().getNumDeviceProcessors());
					for (int i = 0; i < ConfLoader.getInstance().getNumDeviceProcessors(); ++i) {
						syncer.submit(() -> doSyncContinuous());
					}
				}
			} else if (schedulerType == DeviceSchedulerType.STATIC) {
				syncer = Executors.newFixedThreadPool(ConfLoader.getInstance().getNumDestinations());
				for (Integer i = 1; i <= ConfLoader.getInstance().getNumDestinations(); ++i) {
					final Integer dstIndex = i;
					syncer.submit(() -> doSyncStatic(dstIndex));
				}
			}

			/*
			{
				ScheduledExecutorService syncer = Executors.newScheduledThreadPool(PropsLoader.getInstance().getNumDeviceProcessors());
				syncers.add(syncer);
				syncer.scheduleWithFixedDelay(this::doSync, 0, 1, TimeUnit.NANOSECONDS);
			}*/


			failedDeviceScheduler = Executors.newScheduledThreadPool(1);
			failedDeviceScheduler.scheduleWithFixedDelay(this::scheduleFailedDevices, 0, ConfLoader.getInstance().getFailedDeviceRetryIntervalS(), TimeUnit.SECONDS);

			deviceRegisterScheduler = Executors.newScheduledThreadPool(1);
			deviceRegisterScheduler.scheduleWithFixedDelay(this::registerDevices, 0, 10, TimeUnit.SECONDS);

			//externalCommandLoader = Executors.newScheduledThreadPool(1);
			//externalCommandLoader.scheduleWithFixedDelay(this::loadExternalCommands, 0, 10, TimeUnit.SECONDS);

			syncer.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	private final void detectNewDevices() {
		while (!Thread.interrupted()) {
			try {
				deviceParentWatcher = FileSystems.getDefault().newWatchService();
				Path deviceUploadRoot = ConfLoader.getInstance().getDeviceUploadRoot();
				Path deviceDataRoot = ConfLoader.getInstance().getDeviceDataRoot();
				//Watch all device parents and also device directories individually

				for (Path deviceParent : locator.getAllDeviceParents()) {
					deviceParent.register(deviceParentWatcher, StandardWatchEventKinds.ENTRY_CREATE);	
				}

				boolean poll = true;

				while (poll) {
					try {
						WatchKey key = deviceParentWatcher.take();
						Path detectedParent = (Path) key.watchable();
						for (WatchEvent<?> event : key.pollEvents()) {
							String detectedDevice = event.context().toString();
							Path detectedDevicePathInUpload = Path.of(detectedParent.toString(), detectedDevice);
							if (!detectedDevicePathInUpload.toFile().isDirectory()) {
								continue;
							}					
							if (Device.validateDeviceDataRoot(detectedDevicePathInUpload) == null) {
								//Watch this directory as this may be a nested parent for devices to be created in future
								detectedDevicePathInUpload.register(deviceParentWatcher, StandardWatchEventKinds.ENTRY_CREATE);
							}
							//Check if device exists in deviceDataRoot and if does not exists then create it
							//Replace uploadRoot by dataRoot in the detectedDevicePathInUpload and construct the deviceDataRootPath
							Path detectedDevicePathInDataRoot  = Path.of(deviceDataRoot.toString(), deviceUploadRoot.relativize(detectedDevicePathInUpload).toString());
							if (! Files.exists(detectedDevicePathInDataRoot)) {
								Files.createDirectories(detectedDevicePathInDataRoot);
							}
							Device device = this.locator.locateDeviceAtPath(detectedDevicePathInDataRoot, detectedDevicePathInUpload);
							if (device != null) {
								if ((devices.size() + failedDevices.size()) < ConfLoader.getInstance().getDeviceCountLimit()) {
									if (!devices.contains(device) && !failedDevices.contains(device)) {
										//Add the new device to devices, registerDevices will try to register it  
										//globalLogger.info("New : " + detectedDevice);
										this.devices.add(device);
										Monitor.getInstance().setDetectedDeviceCnt(this.devices.size() + this.failedDevices.size());
										Monitor.getInstance().setFailedDeviceCnt(this.failedDevices.size());
									}
								} else {
									globalTracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
								}

								//Register this device for watching and scheduling
								if (deviceWatcher != null) {
									device.getDeviceUploadRoot().register(deviceWatcher, StandardWatchEventKinds.ENTRY_CREATE);
								}
								//Register this device's parent as well for watching
								device.getDeviceUploadRoot().register(deviceParentWatcher, StandardWatchEventKinds.ENTRY_CREATE);

							}						
						}
						poll = key.reset();				
					} catch (Exception e) {
						globalTracer.error("Device detector failed while watching for new devices exception : ", e);
						//Ignore and keep retrying
					}
				}
			} catch (Exception e) {
				globalTracer.error("Device detector setup failed with exception, will be retried : ", e);
				//Go back and retry creating device watcher all over again.
			}
		}
	}

	private final void watchAndScheduleDevices() {
		while (!Thread.interrupted()) {
			try {
				deviceWatcher = FileSystems.getDefault().newWatchService();
				Path deviceUploadRoot = ConfLoader.getInstance().getDeviceUploadRoot();
				Path deviceDataRoot = ConfLoader.getInstance().getDeviceDataRoot();
				//Watch all device parents and also device directories individually

				for (Device device : devices) {
					//device.getDeviceUploadRoot().register(deviceWatcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);	
					device.getDeviceUploadRoot().register(deviceWatcher, StandardWatchEventKinds.ENTRY_CREATE);
				}
				boolean poll = true;
				while (poll) {
					try {
						WatchKey key = deviceWatcher.take();
						Path detectedDevicePath = (Path) key.watchable();
						Device device = Device.findInstance(detectedDevicePath);
						if (device != null) {
							if (devices.contains(device)) {
								if (!failedDevices.contains(device)) {		
									if((device.getStatus() == DeviceStatus.SYNCING) ||
											(device.getStatus() == DeviceStatus.SYNCING_FAILED) ||
											(device.getStatus() == DeviceStatus.REGISTERED)                        	
											) {
										//globalLogger.info("Modified : " + detectedDevice);
										addDeviceTask(device);    								
									}
								}								
							} 
						}
						key.pollEvents();
						poll = key.reset();
					} catch (ClosedWatchServiceException e) {
						//Ignore						 
					} catch (Exception e) {
						globalTracer.error("Device watcher failed while watching for new devices exception : " + e.getMessage(), e);
						//Ignore and keep retrying
					}
				}
			} catch (ClosedWatchServiceException e) {
				//Ignore
			} catch (Exception e) {
				globalTracer.error("Device watcher failed with exception, will be retried : " + e.getMessage(), e);
				//Go back and retry creating device watcher all over again.
			} 
		}
	}


	private final void scheduleAllDevices() {		
		//Add all devices for work
		try {
			for (Device device : devices) {
				if ((device.getStatus() == DeviceStatus.SYNCING) ||
						(device.getStatus() == DeviceStatus.SYNCING_FAILED) ||
						(device.getStatus() == DeviceStatus.REGISTERED))
				{
					try {
						addDeviceTask(device);
					} catch (InterruptedException e) {
						Thread.interrupted();
					}
				}
			}
		}catch (Exception e) {
			globalTracer.error("All device scheduler failed with exception : " + e.getMessage(), e);
		}
	}

	private final void locateNewDevices() {
		try {
			Set<Device> allDevices = locator.listDevices(ConfLoader.getInstance().getDeviceUploadRoot());
			Set<Device> newDevices = new HashSet<Device>();
			long latestDeviceCount = allDevices.size();
			if (latestDeviceCount > ConfLoader.getInstance().getDeviceCountLimit()) {
				globalTracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
				deviceLocator.shutdown();
			} else {
				for (Device device : allDevices) {
					if (!devices.contains(device) && !failedDevices.contains(device)) {
						++latestDeviceCount;
						newDevices.add(device);
						if (latestDeviceCount > ConfLoader.getInstance().getDeviceCountLimit()) {
							globalTracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
							break;
						}
					}
				}
				if (newDevices.size() > 0) {
					devices.addAll(newDevices);
				}
				Monitor.getInstance().setDetectedDeviceCnt(this.devices.size() + this.failedDevices.size());
				Monitor.getInstance().setFailedDeviceCnt(this.failedDevices.size());
				if (latestDeviceCount > ConfLoader.getInstance().getDeviceCountLimit()) {
					globalTracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
					deviceLocator.shutdown();
				}
			}
		} catch (Exception e){
			globalTracer.error("Device locator failed with exception : ", e);
		}
	}

	private final void registerDevices() {
		try {
			for (Device device : devices) {
				if ((device.getStatus() == DeviceStatus.UNREGISTERED) ||
						(device.getStatus() == DeviceStatus.REGISTRATION_FAILED)
						) {
					try {
						device.tryRegisterDevice();
						if(device.getStatus() == DeviceStatus.REGISTERED) {							
							if (ConfLoader.getInstance().getDeviceSchedulerType() != DeviceSchedulerType.STATIC) {
								addDeviceTask(device);
							}
						}
					} catch (InterruptedException e) {
						Thread.interrupted();
					} catch(Exception e) {
						//Catch all kinds of exceptions , dump and move on
						//We should not let this thread give up as it is critical for detecting new devices., 
						globalTracer.error("Device resgitrer failed with exception for device : " + device, e);
					} 
				}
			}
		} catch (Exception e) {
			globalTracer.error("Device registrer failed with exception : ", e);
		}
		checkLimits();
	}

	private void checkLimits() {
		if (ConfLoader.getInstance().checkGlobalLimits()) {
			//Check all global limits and shutdown consolidator if limits have exceeded
			if (Monitor.getInstance().getTotalProcessedLogSize() > ConfLoader.getInstance().getTotalProcessedLogSizeLimit()) {
				globalTracer.error("Total processed log size " + Monitor.getInstance().getTotalProcessedLogSize() + " bytes has exceeded the specified limit of " + ConfLoader.getInstance().getTotalProcessedLogSizeLimit() + " bytes. Please renew your license.");
				stopSyncServices();
				System.exit(1);
			}
			if (Monitor.getInstance().getTotalProcessedOperCount() > ConfLoader.getInstance().getTotalOperationCountLimit()) {
				globalTracer.error("Total processed operations " + Monitor.getInstance().getTotalProcessedOperCount() + " has exceeded the specified limit of " + ConfLoader.getInstance().getTotalOperationCountLimit() + ". Please renew your license.");
				stopSyncServices();
				System.exit(1);
			}
			if (Monitor.getInstance().getTotalDstTxnCount() > ConfLoader.getInstance().getTotalTransactionCountLimit()) {
				globalTracer.error("Total processed transactions " + Monitor.getInstance().getTotalDstTxnCount() + " has exceeded the specified limit of " + ConfLoader.getInstance().getTotalTransactionCountLimit() + ". Please renew your license.");
				stopSyncServices();
				System.exit(1);
			}
		}		
	}

	private final void scheduleFailedDevices() {
		try {
			for (Device device : failedDevices) {
				if (device.getStatus() !=  DeviceStatus.REGISTRATION_FAILED) {                
					try 
					{
						for (int dstIndex : device.getAllDstIndexes()) {
							DeviceProcessor processor = DeviceProcessor.getInstance(device, dstIndex);
							processor.removeInstance(device, dstIndex);
						}
						failedDevices.remove(device);
						devices.add(device);
						if (ConfLoader.getInstance().getDeviceSchedulerType() != DeviceSchedulerType.STATIC) {
							addDeviceTask(device);
						}
					} catch (InterruptedException e) {
						Thread.interrupted();
					} catch (Exception e) {
						globalTracer.error("Failed device scheduler failed with exception for device : " + device, e);
					}
				}
			}
			Monitor.getInstance().setDetectedDeviceCnt(this.devices.size() + this.failedDevices.size());
			Monitor.getInstance().setFailedDeviceCnt(this.failedDevices.size());
		} catch (Exception e) {
			globalTracer.error("Failed device scheduler failed with exception : ", e);
		}
	}

	private final void doSyncContinuous() {
		while(! Thread.interrupted()) {
			try {
				Device device = tasks.take();
				doSyncInternal(device);
			} catch (InterruptedException e) {
				Thread.interrupted();
				return;
			}
		}
	}


	private final void doSync() {
		Device device = null;
		//device = tasks.poll(Long.MAX_VALUE, TimeUnit.DAYS);        	
		device = tasks.poll();
		doSyncInternal(device);
	}

	private final void doSyncInternal(Device device) {
		try {
			if (device != null) {
				if (!device.aquireProcessingLock()) {
					//If the device is already being processed, then add it to queue and move on
					addDeviceTask(device);
					return;
				}
				//driverLogger.debug(Thread.currentThread().getName() + " : Processing device : " + device);
				if ((device.getStatus() == DeviceStatus.SYNCING) ||
						(device.getStatus() == DeviceStatus.SYNCING_FAILED) ||
						(device.getStatus() == DeviceStatus.REGISTERED)) {

					DeviceProcessor syncer;
					boolean hasMoreWork = false;
					for (int dstIndex : device.getAllDstIndexes()) {
						syncer = DeviceProcessor.getInstance(device, dstIndex);
						syncer.processDevice();
						hasMoreWork = (syncer.hasMoreWork()) ? true : hasMoreWork; 
					}
					DeviceLogCleaner.getInstance(device).markAppliedAndCleanUp();

					boolean scheduleDeviceAgain = true;
					if (device.getStatus() == DeviceStatus.REMOVED) {
						scheduleDeviceAgain = false;
					} 
					//Check device limits against licensed limits
					if (ConfLoader.getInstance().checkPerDeviceLimits()) {
						device.checkLimits();
						if ((device.getStatus() == DeviceStatus.SUSPENDED)) {
							scheduleDeviceAgain = false;
						}
					} 

					if (scheduleDeviceAgain == true) {
						if (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.EVENT_BASED) {
							if (!hasMoreWork) {
								scheduleDeviceAgain = false;
							}
						}
					}

					if (scheduleDeviceAgain) {
						addDeviceTask(device);
					}
				} 
			}
		} catch(InterruptedException e) {
			Thread.interrupted();
		} catch(Exception e) {
			try {
				device.tracer.error("Failed with exception : " + e.getMessage(), e);
				globalTracer.error("Device " + device.getDeviceUUID() + " : failed with exception : " + e.getMessage(), e);
				device.updateDeviceStatus(DeviceStatus.SYNCING_FAILED, "Sync failed with exception : " + e.getMessage());
				Monitor.getInstance().registerChangedDevice(device);
				removeDeviceTask(device);
				failedDevices.add(device);
				devices.remove(device);
				Monitor.getInstance().setFailedDeviceCnt(this.failedDevices.size());
			} catch (Exception e1) {
				device.tracer.error("Failed updating device status for device : " + device + " with exception : " + e1);
			}
		} finally {
			if (device != null) {
				device.releaseProcessingLock();
			}
		}
	}

	private void doSyncInternal() {
		// TODO Auto-generated method stub

	}

	private final void doSyncStatic(Integer dstIndex) {
		while(! Thread.interrupted()) {
			try {
				for (Device device : devices) {
					try {
						if ((device.getStatus() == DeviceStatus.SYNCING) ||
								(device.getStatus() == DeviceStatus.SYNCING_FAILED) ||
								(device.getStatus() == DeviceStatus.REGISTERED))
						{
							if (device.getAllDstIndexes().contains(dstIndex)) {
								for (int idx : device.getAllDstIndexes()) {
									DeviceProcessor syncer = DeviceProcessor.getInstance(device, idx);
									syncer.processDevice();
								}
								DeviceLogCleaner.getInstance(device).markAppliedAndCleanUp();
							}
						}
					} catch (Exception e) {
						try {
							device.tracer.error("Failed with exception : ", e);
							globalTracer.error("Device " + device.getDeviceUUID() + " : failed with exception ", e);
							device.updateDeviceStatus(DeviceStatus.SYNCING_FAILED, "Sync failed with exception : " + e);
							Monitor.getInstance().registerChangedDevice(device);
							failedDevices.add(device);
							devices.remove(device);
							Monitor.getInstance().setFailedDeviceCnt(this.failedDevices.size());
						} catch (Exception e1) {
							device.tracer.error("Failed updating device status for device : " + device + " with exception : " + e1);
						}
					}
				}
				Thread.sleep(ConfLoader.getInstance().getDevicePollingIntervalMs());
			}catch (Exception e) {
				globalTracer.error("Static device executor failed with exception : ", e);				
			}
		}
	}

	private final synchronized void addDeviceTask(Device device) throws InterruptedException {
		tasks.put(device);
		//Set global latency to the latency of the last device in the task queue( worst case ) 
		Monitor.getInstance().setlastQueuedDevice(device);
	}

	private final synchronized void removeDeviceTask(Device device) throws InterruptedException {
		while (tasks.remove(device)) {
			;
		}
		//Remove device processors
		for (int dstIndex : device.getAllDstIndexes()) {
			DeviceProcessor processor = DeviceProcessor.getInstance(device, dstIndex);
			processor.removeInstance(device, dstIndex);
		}
	}


	private final void initDriver() throws SyncLiteException {
		initLogger();
		try {			
			//Initialize all dsts
			for (int dstIndex = 1; dstIndex <= ConfLoader.getInstance().getNumDestinations(); ++dstIndex) {
				DSTInitializer.initialize(globalTracer, dstIndex);
			}
			deviceDataStageManager = DeviceStageManager.getDataStageManagerInstance();
			deviceDataStageManager.initializeDataStage(globalTracer);

			if (ConfLoader.getInstance().getEnableDeviceCommandHandler()) {
				deviceCommandStageManager = DeviceStageManager.getCommandStageManagerInstance();
				deviceCommandStageManager.initializeCommandStage(globalTracer);				
			}
			WatchDogs.getInstance().startWatchDogs(globalTracer);
			this.locator = DeviceLocator.getInstance(ConfLoader.getInstance().getDeviceDataRoot(), ConfLoader.getInstance().getDeviceUploadRoot(), globalTracer);
			//this.devices.addAll(locator.listDevices());
			Monitor.getInstance().setDetectedDeviceCnt(this.devices.size());
			//this.cmdProcessor = new ServerCommandProcessor();
		} catch (Exception e) {
			this.globalTracer.error("Failed to initialize SyncLite Consolidator driver : " + e.getMessage(), e);
			throw e;
		}
	}

	@Override
	public final void run() {
		try {
			initDriver();
			runSyncServices();
		} catch (Exception e) {
			globalTracer.error("ERROR : ", e);
			System.out.println("ERROR : " + e.getMessage());
			System.exit(1);
		}
	}


	private final Void locateDevicesAndStartScheduler() throws SyncLiteException {
		locator.tryReloadDevices(devices);

		//Add devices to task queue 
		if ((ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.POLLING) || (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.EVENT_BASED)) {
			for (Device device : devices) {
				globalTracer.debug("Device " + device.getDeviceUUID() + " status : " + device.getStatus());
				if ((device.getStatus() == DeviceStatus.SYNCING) ||
						(device.getStatus() == DeviceStatus.SYNCING_FAILED) ||
						(device.getStatus() == DeviceStatus.REGISTERED))
				{
					try {
						addDeviceTask(device);
					} catch (InterruptedException e) {
						Thread.interrupted();
					}
				} 
			}			
		}

		//registerDevices();
		if (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.POLLING) {
			//Polling device scheduler needs a continuous device locator thread to detect new devices
			deviceLocator = Executors.newScheduledThreadPool(1);
			deviceLocator.scheduleWithFixedDelay(this::locateNewDevices, ConfLoader.getInstance().getDeviceScannerIntervalS(), ConfLoader.getInstance().getDeviceScannerIntervalS(), TimeUnit.SECONDS);
		} else if (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.STATIC) { 
			//Static device scheduler needs a continuous device locator thread to detect new devices
			deviceLocator = Executors.newScheduledThreadPool(1);
			deviceLocator.scheduleWithFixedDelay(this::locateNewDevices, ConfLoader.getInstance().getDeviceScannerIntervalS(), ConfLoader.getInstance().getDeviceScannerIntervalS(), TimeUnit.SECONDS);		
		} else if (ConfLoader.getInstance().getDeviceSchedulerType() == DeviceSchedulerType.EVENT_BASED) {
			//EVENT_BASED scheduler needs a dedicated device scheduler thread that 
			//- locates new devices if any
			//- schedules devices for work on detecting any file activity in any device

			deviceDetector = Executors.newFixedThreadPool(1);
			deviceDetector.submit(this::detectNewDevices);

			deviceScheduler = Executors.newFixedThreadPool(1);
			deviceScheduler.submit(this::watchAndScheduleDevices);

			//
			//The File activity based watched also can miss events at a large scale 
			//Schedule all devices for at polling interval to check for any missed work
			//
			deviceLocator = Executors.newScheduledThreadPool(1);
			long devicePollingIntervalMs = ConfLoader.getInstance().getDevicePollingIntervalMs();
			if (devicePollingIntervalMs > 0) {
				deviceLocator.scheduleWithFixedDelay(this::scheduleAllDevices, ConfLoader.getInstance().getDevicePollingIntervalMs() , ConfLoader.getInstance().getDevicePollingIntervalMs(), TimeUnit.MILLISECONDS);
			}
		}
		return null;
	}

	public Set<Device> getAllDevices() {
		return this.devices; 
	}

	public void shutdownJob() {
		this.globalTracer.info("Shutdown job command received, shutting down the job.");
		stopSyncServices();
		this.globalTracer.info("Job shutdown successfully.");
		System.exit(0);
	}
}
