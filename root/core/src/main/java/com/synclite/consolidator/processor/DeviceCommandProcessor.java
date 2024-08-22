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

package com.synclite.consolidator.processor;

import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.synclite.consolidator.SyncDriver;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.device.DeviceManager;
import com.synclite.consolidator.exception.SyncLiteException;

public class DeviceCommandProcessor extends RequestProcessor{

	private String commandDevicesNameList;
	private Pattern commandDevicesNamePattern;
	private String commandDevicesIDList;
	private Pattern commandDevicesIDPattern;
	private String deviceCommand;
	private String deviceCommandDetails;

	public DeviceCommandProcessor(Logger tr, HashMap<String, Object> args) throws SyncLiteException{
		super(tr, args);
		
		//Validate arguments 

		boolean devicesSpecified = false;
		commandDevicesNameList = null;
		if (args.get("command-devices-name-list") != null) {
			commandDevicesNameList = args.get("command-devices-name-list").toString();
			devicesSpecified = true;
		}

		commandDevicesNamePattern = null;
		if (args.get("command-devices-name-pattern") != null) {
			if (devicesSpecified) {
				throw new SyncLiteException("Please specify only one of the four : command-devices-name-list/command-devices-id-list/command-devices-name-pattern/command-devices-id-list");
			}
			String manageDevicesNamePatternStr = args.get("command-devices-name-pattern").toString();
			try {
				if (manageDevicesNamePatternStr != null) {
					commandDevicesNamePattern = Pattern.compile(manageDevicesNamePatternStr);
				}
			} catch (PatternSyntaxException e){
				throw new SyncLiteException("Invalid command-devices-name-pattern specified", e);
			}
			devicesSpecified = true;
		}

		commandDevicesIDList = null;
		if (args.get("command-devices-id-list") != null) {
			if (devicesSpecified) {
				throw new SyncLiteException("Please specify only one of the four : command-devices-name-list/command-devices-id-list/command-devices-name-pattern/command-devices-id-list");
			}
			commandDevicesIDList = args.get("command-devices-id-list").toString();
			devicesSpecified = true;
		}

		commandDevicesIDPattern = null;		
		if (args.get("command-devices-id-pattern") != null) {
			if (devicesSpecified) {
				throw new SyncLiteException("Please specify only one of the four : command-devices-name-list/command-devices-id-list/command-devices-name-pattern/command-devices-id-list");
			}
			String manageDevicesIDPatternStr = args.get("command-devices-id-pattern").toString();	
			try {
				if (manageDevicesIDPatternStr != null) {
					commandDevicesIDPattern = Pattern.compile(manageDevicesIDPatternStr);
				}
			} catch (PatternSyntaxException e){
				throw new SyncLiteException("Invalid command-devices-id-pattern specified", e);
			}
			devicesSpecified = true;
		}

		if (!devicesSpecified) {
			throw new SyncLiteException("None of the the four : command-devices-name-list/command-devices-id-list/command-devices-name-pattern/command-devices-id-list specified. Please specify one.");
		}

		deviceCommand = args.get("device-command").toString();
		if (deviceCommand == null) {
			throw new SyncLiteException("device-command not specified in the request");
		}

		if (args.get("device-command-details") != null) {
			deviceCommandDetails = args.get("device-command-details").toString();
			if (deviceCommandDetails == null) {
				deviceCommandDetails = "";
			}
		} else {
			deviceCommandDetails = "";
		}
	}

	@Override
	protected void doProcessRequest(HashMap<String, Object> args) throws SyncLiteException {
		//Get all devices.
		//Create an instance of DeviceManager and execute commandDevices method
		//
		Set<Device> devices = SyncDriver.getInstance().getAllDevices();
		
		DeviceManager deviceMgr = DeviceManager.getInstance(this.tracer);		

		deviceMgr.commandDevices(devices, commandDevicesNameList, commandDevicesNamePattern, commandDevicesIDList, commandDevicesIDPattern, deviceCommand, deviceCommandDetails);
	}

}
