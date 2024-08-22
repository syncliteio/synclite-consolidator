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

package com.synclite.consolidator.device;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.stage.DeviceStageManager;
import com.synclite.consolidator.stage.SyncLiteObjectType;
import com.synclite.consolidator.watchdog.Monitor;

public class DeviceLocator {
	private Path root;
	private Path upload;
	private static ConcurrentHashMap<Path,DeviceLocator> deviceLocators = new ConcurrentHashMap<Path, DeviceLocator>();
	private final Logger tracer;
	private Set<Path> allDeviceParents = new HashSet<Path>();
	private DeviceStageManager deviceStageManager = DeviceStageManager.getDataStageManagerInstance();
	private DeviceLocator(Path root, Path upload, Logger tracer) {
		this.root = root;
		this.upload = upload;
		this.tracer = tracer;
		allDeviceParents.add(upload);
	}

	public static DeviceLocator getInstance(Path root, Path upload, Logger tracer) {
		if (root == null) {
			return null;
		}
		return deviceLocators.computeIfAbsent(root, s -> new DeviceLocator(s, upload, tracer));
	}

	public Set<Path> getAllDeviceParents() {
		return allDeviceParents;
	}
	
	private List<Path> getDeviceUploadRootsFromStage(Path startFrom) throws SyncLiteException {
		List<Path> deviceUploadRoots;
		try {
			deviceUploadRoots = deviceStageManager.listContainers(startFrom, SyncLiteObjectType.DATA);			
		} catch (SyncLiteStageException e) {
			throw new SyncLiteException("Exception while retrieving ist of containers from device stage : ", e);
		}
		return deviceUploadRoots;        
	}
	
	private void buildDevices(List<Path> deviceUploadRoots, Set<Device> devices) throws SyncLiteStageException {
		Path baseUploadRoot = ConfLoader.getInstance().getDeviceUploadRoot();
		for (Path deviceUploadRoot : deviceUploadRoots) {			
			Path deviceRoot = Path.of(root.toString(), baseUploadRoot.relativize(deviceUploadRoot).toString());
			Device device = locateDeviceAtPath(deviceRoot, deviceUploadRoot);
			if (device != null) {
				devices.add(device);
				Monitor.getInstance().setDetectedDeviceCnt(devices.size());

				if (devices.size() > ConfLoader.getInstance().getDeviceCountLimit()) {
					tracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
					break;
				}
			}
		}   	
	}

	public final Set<Device> listDevices(Path startFrom) throws SyncLiteException {
		Set<Device> devices = new HashSet<Device>();
		List<Path> devicUploadRoots = getDeviceUploadRootsFromStage(startFrom);
		buildDevices(devicUploadRoots, devices);
		return devices;
	}    

	public final void tryReloadDevices(Set<Device> devices) throws SyncLiteException {
		
		List<Path> deviceUploadRoots = Monitor.getInstance().getDeviceUploadRootsFromStats();
		buildDevices(deviceUploadRoots, devices);       	

		//Load devices from FS now as FS may have newly added devices.
		deviceUploadRoots = getDeviceUploadRootsFromStage(upload);
		buildDevices(deviceUploadRoots, devices);
	}    

	public Device locateDeviceAtPath(Path deviceRoot, Path deviceUploadRoot) throws SyncLiteStageException {
		if (! deviceStageManager.containerExists(deviceUploadRoot,SyncLiteObjectType.DATA_CONTAINER)) {
			//If upload root does not exist then ignore 
			return null;
		}
		DeviceIdentifier deviceIdentifier = Device.validateDeviceDataRoot(deviceRoot);
		Device device = null;
		if (deviceIdentifier != null) {
			//Don't need to check for existence again
			/*if (!deviceStageManager.containerExists(deviceUploadRoot)) {
				return null;
			}*/
			try {
				if (ConfLoader.getInstance().isAllowedDevice(deviceIdentifier)) {
					if (!Files.exists(deviceRoot)) {
						//create directory
						try {
							Files.createDirectories(deviceRoot);
						} catch (IOException e) {
							throw new SyncLiteException("Failed to create directory " + deviceRoot + " inside " + root, e);
						}
					} 

					device = Device.getInstance(deviceRoot, deviceUploadRoot);
					if (device.getStatus() == DeviceStatus.REMOVED) {
						Device.remove(device);
						device = Device.getInstance(deviceRoot, deviceUploadRoot);
					}
					allDeviceParents.add(device.getDeviceUploadRoot().getParent());
				}
			} catch(SyncLiteException e) {
				//Ignore the device with invalid root
				//Log somewhere
			}            
		}
		return device;
	}
}
