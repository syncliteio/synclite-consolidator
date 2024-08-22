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

package com.synclite.consolidator.stage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;

public class LocalMinioStageManager extends RemoteMinioStageManager {

	protected LocalMinioStageManager() {
	}

	@Override
	public void initializeDataStage(Logger tracer) throws SyncLiteStageException {
		super.initializeDataStage(tracer);
	}

	@Override
	public Path findObjectWithSuffix(Path container, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				List<Path> filesInContainer = Files.walk(container).filter(s->s.toString().endsWith(suffix)).collect(Collectors.toList());
				if (filesInContainer.size() > 0) {
					return filesInContainer.get(0);
				} else {
					return null;
				}
			} catch (IOException e) {
				tracer.error("Exception while finding object with suffix " + suffix + " from device stage, under container : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}				
			}
		}
		return null;
	}
	
	public List<Path> findObjectsWithSuffixPrefix(Path container, String prefix, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException  {
		List<Path> objects = null;
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				objects = Files.walk(container).filter(s->(s.getFileName().toString().startsWith(prefix) && s.getFileName().toString().endsWith(suffix))).collect(Collectors.toList());
				return objects;
			} catch (IOException e) {
				tracer.error("Exception while finding object with prefix : " + prefix + " and suffix : " + suffix + " from device stage in container : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return objects;
	}

	@Override
	public List<Path> listContainers(Path startFrom, SyncLiteObjectType objType) throws SyncLiteStageException {
		List<Path> deviceUploadRoots = new ArrayList<Path>(); 
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				deviceUploadRoots = Files.walk(startFrom).filter(Files::isDirectory).collect(Collectors.toList());
				return deviceUploadRoots;
			} catch (IOException e) {
				tracer.error("Exception while listing containers from device stage in container : ", e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return deviceUploadRoots;	
	}
}
