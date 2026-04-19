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
		tracer.debug("DEVICE-BUILD: Building devices from " + deviceUploadRoots.size() + " upload roots, current set size=" + devices.size() + ", baseUploadRoot=" + baseUploadRoot);
		int attempted = 0, succeeded = 0, skipped = 0;
		for (Path deviceUploadRoot : deviceUploadRoots) {
			attempted++;
			Path deviceRoot = Path.of(root.toString(), baseUploadRoot.relativize(deviceUploadRoot).toString());
			tracer.debug("DEVICE-BUILD: [" + attempted + "] deviceUploadRoot='" + deviceUploadRoot + "' -> deviceRoot='" + deviceRoot + "'");
			Device device = locateDeviceAtPath(deviceRoot, deviceUploadRoot);
			if (device != null) {
				boolean added = devices.add(device);
				if (added) {
					succeeded++;
					Monitor.getInstance().setDetectedDeviceCnt(devices.size());
					tracer.debug("DEVICE-BUILD: [" + attempted + "] device ADDED: " + device.getDeviceUUID() + " status=" + device.getStatus());
				} else {
					skipped++;
					tracer.debug("DEVICE-BUILD: [" + attempted + "] device ALREADY IN SET (skipped): " + device.getDeviceUUID());
				}

				if (devices.size() > ConfLoader.getInstance().getDeviceCountLimit()) {
					tracer.error("Device count exceeded the specified limit of " + ConfLoader.getInstance().getDeviceCountLimit() + ". Please renew your license.");
					break;
				}
			} else {
				skipped++;
				tracer.debug("DEVICE-BUILD: [" + attempted + "] locateDeviceAtPath returned null for uploadRoot='" + deviceUploadRoot + "'");
			}
		}
		tracer.debug("DEVICE-BUILD: Done. attempted=" + attempted + " added=" + succeeded + " skipped/null=" + skipped + " total set size=" + devices.size());
	}

	public final Set<Device> listDevices(Path startFrom) throws SyncLiteException {
		Set<Device> devices = new HashSet<Device>();
		List<Path> devicUploadRoots = getDeviceUploadRootsFromStage(startFrom);
		buildDevices(devicUploadRoots, devices);
		return devices;
	}    

	public final void tryReloadDevices(Set<Device> devices) throws SyncLiteException {
		tracer.debug("DEVICE-RELOAD: Starting tryReloadDevices. Current device set size=" + devices.size());

		//Load devices from stage first. This avoids startup stalls if stats DB is busy or slow.
		List<Path> deviceUploadRoots = getDeviceUploadRootsFromStage(upload);
		tracer.debug("DEVICE-RELOAD: Stage scan returned " + deviceUploadRoots.size() + " upload roots (upload=" + upload + ")");
		buildDevices(deviceUploadRoots, devices);
		tracer.debug("DEVICE-RELOAD: After stage-based reload, device set size=" + devices.size());

		//Use stats-based reload only as a best-effort fallback if stage scan did not yield devices.
		if (devices.isEmpty()) {
			try {
				deviceUploadRoots = Monitor.getInstance().getDeviceUploadRootsFromStats();
				tracer.debug("DEVICE-RELOAD: Stats returned " + deviceUploadRoots.size() + " upload roots for reload");
				buildDevices(deviceUploadRoots, devices);
				tracer.debug("DEVICE-RELOAD: After stats-based fallback reload, device set size=" + devices.size());
			} catch (RuntimeException e) {
				tracer.error("DEVICE-RELOAD: Stats-based fallback reload failed, continuing with stage-discovered devices only", e);
			}
		}
	}    

	public Device locateDeviceAtPath(Path deviceRoot, Path deviceUploadRoot) throws SyncLiteStageException {
		tracer.debug("DEVICE-LOCATE: Checking deviceRoot='" + deviceRoot + "' deviceUploadRoot='" + deviceUploadRoot + "'");
		if (! deviceStageManager.containerExists(deviceUploadRoot,SyncLiteObjectType.DATA_CONTAINER)) {
			//If upload root does not exist then ignore 
			tracer.debug("DEVICE-LOCATE: SKIP — upload container does not exist: " + deviceUploadRoot);
			return null;
		}
		DeviceIdentifier deviceIdentifier = Device.validateDeviceDataRoot(deviceRoot);
		Device device = null;
		if (deviceIdentifier != null) {
			tracer.debug("DEVICE-LOCATE: Valid device identifier found: " + deviceIdentifier + " at " + deviceRoot);
			try {
				if (ConfLoader.getInstance().isAllowedDevice(deviceIdentifier)) {
					if (!Files.exists(deviceRoot)) {
						//create directory
						try {
							Files.createDirectories(deviceRoot);
							tracer.debug("DEVICE-LOCATE: Created missing deviceRoot directory: " + deviceRoot);
						} catch (IOException e) {
							throw new SyncLiteException("Failed to create directory " + deviceRoot + " inside " + root, e);
						}
					} 

					device = Device.getInstance(deviceRoot, deviceUploadRoot);
					if (device.getStatus() == DeviceStatus.REMOVED) {
						tracer.debug("DEVICE-LOCATE: Device " + device.getDeviceUUID() + " is REMOVED, re-creating instance");
						Device.remove(device);
						device = Device.getInstance(deviceRoot, deviceUploadRoot);
					}
					allDeviceParents.add(device.getDeviceUploadRoot().getParent());
					tracer.debug("DEVICE-LOCATE: SUCCESS — device=" + device.getDeviceUUID() + " name=" + device.getDeviceName() + " status=" + device.getStatus());
				} else {
					tracer.debug("DEVICE-LOCATE: SKIP — device is NOT allowed by filter: " + deviceIdentifier);
				}
			} catch(SyncLiteException | RuntimeException e) {
				tracer.error("Failed to locate device at path : " + deviceRoot + ", skipping this device", e);
			}            
		} else {
			tracer.debug("DEVICE-LOCATE: SKIP — validateDeviceDataRoot returned null for: " + deviceRoot);
		}
		return device;
	}
}
