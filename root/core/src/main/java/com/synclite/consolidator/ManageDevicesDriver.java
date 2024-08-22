package com.synclite.consolidator;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.device.DeviceLocator;
import com.synclite.consolidator.device.DeviceManager;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.processor.DSTInitializer;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.watchdog.Monitor;

public class ManageDevicesDriver implements Runnable {

	private Logger globalTracer;
	private DeviceStageManager deviceDataStageManager;
	private DeviceStageManager deviceCommandStageManager;

	private DeviceManager deviceManager;
	private DeviceLocator locator;
	private Set<Device> devices = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private static final class InstanceHolder {
		private static ManageDevicesDriver INSTANCE = new ManageDevicesDriver();
	}

	public static ManageDevicesDriver getInstance() {
		return InstanceHolder.INSTANCE;
	}

	@Override
	public void run() {
		try {
			initDriver();
			runManageDevices();
		} catch (Exception e) {
			globalTracer.error("ERROR : ", e);
			System.out.println("ERROR : " + e);
			System.exit(1);
		}
	}

	private final void runManageDevices() throws SyncLiteException {
		this.locator.tryReloadDevices(devices);

		switch(ConfLoader.getInstance().getManageDevicesOperationType() ) {
		case REMOVE_DEVICES:
			deviceManager.removeDevices(devices, ConfLoader.getInstance().getManageDevicesNameList(), ConfLoader.getInstance().getManageDevicesNamePattern(), ConfLoader.getInstance().getManageDevicesIDList(), ConfLoader.getInstance().getManageDevicesIDPattern());
			break;
		case DISABLE_DEVICES:
			deviceManager.disableDevices(devices, ConfLoader.getInstance().getManageDevicesNameList(), ConfLoader.getInstance().getManageDevicesNamePattern(), ConfLoader.getInstance().getManageDevicesIDList(), ConfLoader.getInstance().getManageDevicesIDPattern());
			break;
		case ENABLE_DEVICES:
			deviceManager.enableDevices(devices, ConfLoader.getInstance().getManageDevicesNameList(), ConfLoader.getInstance().getManageDevicesNamePattern(), ConfLoader.getInstance().getManageDevicesIDList(), ConfLoader.getInstance().getManageDevicesIDPattern());
			break;
		case COMMAND_DEVICES:
			deviceManager.commandDevices(devices, ConfLoader.getInstance().getManageDevicesNameList(), ConfLoader.getInstance().getManageDevicesNamePattern(), ConfLoader.getInstance().getManageDevicesIDList(), ConfLoader.getInstance().getManageDevicesIDPattern(), ConfLoader.getInstance().getDeviceCommand(), ConfLoader.getInstance().getDeviceCommandDetails());
			break;
		case REINITIALIZE_DEVICES:
			deviceManager.reInitializeDevices(devices, ConfLoader.getInstance().getManageDevicesNameList(), ConfLoader.getInstance().getManageDevicesNamePattern(), ConfLoader.getInstance().getManageDevicesIDList(), ConfLoader.getInstance().getManageDevicesIDPattern());
			break;

		}
	}

	private final void initLogger() {
		this.globalTracer = Logger.getLogger(ManageDevicesDriver.class);    	
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
		fa.setName("ConsolidatorTracer");
		fa.setFile(Path.of(ConfLoader.getInstance().getDeviceDataRoot().toString(), "synclite_consolidator.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10MB"); // Set the maximum file size to 10 MB
		fa.setAppend(true);
		fa.activateOptions();
		globalTracer.addAppender(fa);    	
	}	

	private final void initDriver() throws SyncLiteException {
		initLogger();
		try {
			
			Monitor.createAndGetInstance(globalTracer).start();

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

			deviceManager = DeviceManager.getInstance(globalTracer);
			this.locator = DeviceLocator.getInstance(ConfLoader.getInstance().getDeviceDataRoot(), ConfLoader.getInstance().getDeviceUploadRoot(), globalTracer);
		} catch (Exception e) {
			this.globalTracer.error("Failed to initialize SyncLite Consolidator driver : ", e);
			throw e;
		}
	}


}
