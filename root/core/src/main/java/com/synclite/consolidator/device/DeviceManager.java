package com.synclite.consolidator.device;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.processor.DeviceDstInitializer;
import com.synclite.consolidator.schema.TableMapper;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceManager {

	private Logger globalTracer;
	private DeviceStageManager deviceDataStageManager;
	private DeviceStageManager deviceCommandStageManager;
	private static volatile DeviceManager INSTANCE;

	private DeviceManager(Logger tracer) {
		this.globalTracer = tracer;
		this.deviceDataStageManager = DeviceStageManager.getDataStageManagerInstance();		
		this.deviceCommandStageManager = DeviceStageManager.getCommandStageManagerInstance();
	}
	
	public static synchronized DeviceManager getInstance(Logger tracer) {
		if (INSTANCE == null) {
			INSTANCE = new DeviceManager(tracer);
		}
		return INSTANCE;
	}
	
	public final synchronized void commandDevices(Set<Device> devices, String commandDevicesNameList, Pattern commandDevicesNamePattern, String commandDevicesIDList, Pattern commandDevicesIDPattern, String command, String commandDetails) throws SyncLiteException {
		if (commandDevicesNameList != null) {
			String[] deviceNames = commandDevicesNameList.split(",");
			for (String deviceName : deviceNames) {
				Device d = Device.getDeviceByName(deviceName);
				if (d != null) {
					d.dispatchDeviceCommand(command, commandDetails);
					d.cleanupDeviceCommands();
				}
			}
		} else if (commandDevicesNamePattern != null) {
			for (Device d : devices) {
				if (d.getDeviceName() != null) {
					if (commandDevicesNamePattern.matcher(d.getDeviceName()).matches()) {
						d.dispatchDeviceCommand(command, commandDetails);
						d.cleanupDeviceCommands();
					}
				}
			}	
		} else if (commandDevicesIDList != null){
			String[] deviceIDs= commandDevicesIDList.split(",");
			for (String deviceID : deviceIDs) {
				Device d = Device.getDeviceByID(deviceID);
				if (d != null) {
					d.dispatchDeviceCommand(command, commandDetails);
					d.cleanupDeviceCommands();
				}
			}
		} else if (commandDevicesIDPattern != null) {
			for (Device d : devices) {
				if (commandDevicesIDPattern.matcher(d.getDeviceUUID()).matches()) {
					d.dispatchDeviceCommand(command, commandDetails);
					d.cleanupDeviceCommands();
				}
			}
		}
	}

	public final void disableDevices(Set<Device> devices, String disableDevicesNameList, Pattern disableDevicesNamePattern, String disableDevicesIDList, Pattern disableDevicesIDPattern) throws SyncLiteException {
		if (disableDevicesNameList != null) {
			String[] deviceNames = disableDevicesNameList.split(",");
			for (String deviceName : deviceNames) {
				Device d = Device.getDeviceByName(deviceName);
				if (d != null) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.updateDeviceStatus(DeviceStatus.DISABLED, "Disabled by user");
					}
				}
			}
		} else if (disableDevicesNamePattern != null) {
			for (Device d : devices) {
				if (d.getDeviceName() != null) {
					if (disableDevicesNamePattern.matcher(d.getDeviceName()).matches()) {
						if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
							d.updateDeviceStatus(DeviceStatus.DISABLED, "Disabled by user");
						}
					}
				}
			}			
		} else if (disableDevicesIDList != null) {
			String[] deviceIDs = disableDevicesIDList.split(",");
			for (String deviceID : deviceIDs) {
				Device d = Device.getDeviceByID(deviceID);
				if (d != null) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.updateDeviceStatus(DeviceStatus.DISABLED, "Disabled by user");
					}
				}
			}		
		} else if (disableDevicesIDPattern != null) {
			for (Device d : devices) {
				if (disableDevicesIDPattern.matcher(d.getDeviceUUID()).matches()) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.updateDeviceStatus(DeviceStatus.DISABLED, "Disabled by user");
					}
				}
			}
		} else {
			throw new SyncLiteException("None of the four : Device Name List/Device Name Pattern/Device ID List/Device ID Pattern are not provided. You must specify one of them.");		
		}		
	}
	
	public final void enableDevices(Set<Device> devices, String enableDevicesNameList, Pattern enableDevicesNamePattern, String enableDevicesIDList, Pattern enableDevicesIDPattern) throws SyncLiteException {
		if (enableDevicesNameList != null) {
			String[] deviceNames = enableDevicesNameList.split(",");
			for (String deviceName : deviceNames) {
				Device d = Device.getDeviceByName(deviceName);
				if (d != null) {
					d.updateDeviceStatus(DeviceStatus.SYNCING, "Enabled by user");
				}
			}
		} else if (enableDevicesNamePattern != null) {
			for (Device d : devices) {
				if (d.getDeviceName() != null) {
					if (enableDevicesNamePattern.matcher(d.getDeviceName()).matches()) {
						d.updateDeviceStatus(DeviceStatus.SYNCING, "Enabled by user");
					}
				}
			}
		}  else if (enableDevicesIDList != null) {
			String[] deviceIDs = enableDevicesIDList.split(",");
			for (String deviceID: deviceIDs) {
				Device d = Device.getDeviceByID(deviceID);
				if (d != null) {
					d.updateDeviceStatus(DeviceStatus.SYNCING, "Enabled by user");
				}
			}			
		} else if (enableDevicesIDPattern != null) {
			for (Device d : devices) {
				if (enableDevicesIDPattern.matcher(d.getDeviceUUID()).matches()) {
					d.updateDeviceStatus(DeviceStatus.SYNCING, "Enabled by user");					
				}
			}
		} else {
			throw new SyncLiteException("None of the fource : Device Name List/Device Name Pattern/Device ID List/Device ID Pattern are not provided. You must specify one of them.");
		}		
		
	}

	public final void removeDevices(Set<Device> devices, String removeDevicesNameList, Pattern removeDevicesNamePattern, String removeDevicesIDList, Pattern removeDevicesIDPattern) throws SyncLiteException {
		if (removeDevicesNameList != null) {
			String[] deviceNames = removeDevicesNameList.split(",");
			for (String deviceName : deviceNames) {
				Device d = Device.getDeviceByName(deviceName);
				if (d != null) {
					deleteDevice(d);
				}
			}
		} else  if (removeDevicesNamePattern != null) {
			for (Device d : devices) {
				if (d.getDeviceName() != null) {
					if (removeDevicesNamePattern.matcher(d.getDeviceName()).matches()) {
						deleteDevice(d);
					}
				}
			}			
		} else if (removeDevicesIDList != null) {
			String[] deviceIDs = removeDevicesIDList.split(",");
			for (String deviceID: deviceIDs) {
				Device d = Device.getDeviceByID(deviceID);
				if (d != null) {
					deleteDevice(d);
				}
			}
		} else if (removeDevicesIDPattern != null) {
			for (Device d : devices) {
				if (removeDevicesIDPattern.matcher(d.getDeviceUUID()).matches()) {
					deleteDevice(d);
				}
			}
		} else {
			throw new SyncLiteException("None of the fource : Device Name List/Device Name Pattern/Device ID List/Device ID Pattern are not provided. You must specify one of them.");
		}		
	}

	public final void reInitializeDevices(Set<Device> devices, String disableDevicesNameList, Pattern disableDevicesNamePattern, String disableDevicesIDList, Pattern disableDevicesIDPattern) throws SyncLiteException {
		if (disableDevicesNameList != null) {
			String[] deviceNames = disableDevicesNameList.split(",");
			for (String deviceName : deviceNames) {
				Device d = Device.getDeviceByName(deviceName);
				if (d != null) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.reInitialize();
					}
				}
			}
		} else if (disableDevicesNamePattern != null) {
			for (Device d : devices) {
				if (d.getDeviceName() != null) {
					if (disableDevicesNamePattern.matcher(d.getDeviceName()).matches()) {
						if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
							d.reInitialize();
						}
					}
				}
			}			
		} else if (disableDevicesIDList != null) {
			String[] deviceIDs = disableDevicesIDList.split(",");
			for (String deviceID : deviceIDs) {
				Device d = Device.getDeviceByID(deviceID);
				if (d != null) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.reInitialize();
					}
				}
			}		
		} else if (disableDevicesIDPattern != null) {
			for (Device d : devices) {
				if (disableDevicesIDPattern.matcher(d.getDeviceUUID()).matches()) {
					if ((d.getStatus() == DeviceStatus.SYNCING) || (d.getStatus() == DeviceStatus.SYNCING_FAILED) || (d.getStatus() == DeviceStatus.SUSPENDED)) {
						d.reInitialize();
					}
				}
			}
		} else {
			throw new SyncLiteException("None of the four : Device Name List/Device Name Pattern/Device ID List/Device ID Pattern are not provided. You must specify one of them.");		
		}		
	}

 	private final void deleteDevice(Device d) throws SyncLiteException {
		//Delete entries from stats file.
		globalTracer.info("Attempting to remove device : " + d);
		try {
			Monitor.getInstance().deleteDeviceFromStats(d);
		} catch (Exception e) {
			throw new SyncLiteException("Failed to delete statistics entry for device : " + d, e);
		}

		try {
			//Delete data from destination ( if the option to delete data from dst is enabled )
			if (ConfLoader.getInstance().getRemoveDevicesFromDst()) {
				//	Delete data from destination
				cleanupDstDataForDevice(d);
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to delete data from destination DB for device : " + d, e);			
		}

		try {
			//Delete uploadRoot
			deviceDataStageManager.deleteContainer(d.getDeviceUploadRoot());
		} catch (Exception e) {
			throw new SyncLiteException("Failed to delete device container from the stage for device : " + d, e);
		}
		//Delete the directory and all its contents


		try {
			//Delete dataDir contents
			deleteDeviceDataDir(d);
		} catch (Exception e) {
			throw new SyncLiteException("Failed to delete device container from the stage for device : " + d, e);
		}
		//Delete the directory and all its contents

		globalTracer.info("Successfully removed device : " + d);
	}

 	
	private final void deleteDeviceDataDir(Device d) throws SyncLiteException {
		Path directory = d.getDeviceDataRoot();
		try {
			Files.walk(directory)
			.sorted((a, b) -> b.compareTo(a))
			.forEach(path -> {
				try {
					Files.delete(path);
				} catch (IOException e) {
					//String errorMsg = "Failed to delete device data root : " + directory + " for device : " + d;
					//throw new RuntimeException(errorMsg, e);
					//Ignore
				}
			});
		} catch (IOException e) {
			String errorMsg = "Failed to delete device data root : " + directory + " for device : " + d;
			throw new SyncLiteException(errorMsg, e);
		}
	}

	private final void cleanupDstDataForDevice(Device d) throws SyncLiteException {
		for (int dstIndex : d.getAllDstIndexes()) {
			TableMapper userTableMapper = TableMapper.getUserTableMapperInstance(dstIndex);
			TableMapper systemTableMapper = TableMapper.getSystemTableMapperInstance(dstIndex);
			DeviceDstInitializer dstInitializer = new DeviceDstInitializer(d, userTableMapper, systemTableMapper, null, dstIndex);
			dstInitializer.cleanupTables();
		}
		
		//TODO: If SYNC_MODE is COPY then delete tables also
	}


}
