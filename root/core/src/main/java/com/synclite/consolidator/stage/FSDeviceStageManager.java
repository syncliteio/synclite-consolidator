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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;

public class FSDeviceStageManager extends DeviceStageManager {

	@Override
	public boolean objectExists(Path objectPath, SyncLiteObjectType objType) {
		if (Files.exists(objectPath)) {
			return true;
		}
		return false;
	}

	@Override
	public long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				boolean fileExists = false;
				if (Files.exists(objectPath)) {
					fileExists = true;
				} else {
					//FS may have cached Files exists information. So check other attributes to confirm again that will trigger a cache refresh.
					try {
						Files.getLastModifiedTime(objectPath);
						fileExists = true;
					} catch (IOException e) {
						//Ignore
					}
				}
				if (fileExists) {
					Files.copy(objectPath, outputFile, StandardCopyOption.REPLACE_EXISTING);
					BasicFileAttributes fileAttrs = Files.readAttributes(objectPath, BasicFileAttributes.class);
					long publishTime = fileAttrs.creationTime().toMillis();
					return publishTime;
				} else {					
					return 0;
				}
			} catch (IOException e) {
				tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
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
		return 0;
	}

	@Override
	public long doDownloadAndDecryptObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				boolean fileExists = false;
				if (Files.exists(objectPath)) {
					fileExists = true;
				} else {
					//FS may have cached Files exists information. So check other attributes to confirm again that will trigger a cache refresh.
					try {
						Files.getLastModifiedTime(objectPath);
						fileExists = true;
					} catch (IOException e) {
						//Ignore
					}
				}
				if (fileExists) {					
					try (FileInputStream fis = new FileInputStream(objectPath.toString())){
						this.fileDownloader.decryptAndWriteFile(fis, outputFile);
					}
					
					BasicFileAttributes fileAttrs = Files.readAttributes(objectPath, BasicFileAttributes.class);
					long publishTime = fileAttrs.creationTime().toMillis();
					return publishTime;
				} else {
					return 0;
				}
			} catch (IOException | SyncLiteStageException e) {
				tracer.error("Exception while downloading and decrypting object from device stage, object : " + objectPath, e);				
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
		return 0;
	}

	@Override
	public void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (Files.exists(objectPath)) {
					Files.delete(objectPath);
				}
				return;
			} catch (IOException e) {
				tracer.error("Exception while deleting object from device stage, object : " + objectPath, e);				
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
				tracer.error("Exception while finding object with suffix " + suffix + " from device stage in container : " + container, e);				
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
	public void initializeDataStage(Logger globalTracer) throws SyncLiteStageException {
		this.tracer = globalTracer;
	}

	@Override
	public void initializeCommandStage(Logger globalTracer) throws SyncLiteStageException {
		this.tracer = globalTracer;
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

	@Override
	public boolean containerExists(Path containerPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				boolean fileExists = false;
				if (Files.exists(containerPath)) {
					fileExists = true;
				} else {
					//FS may have cached Files exists information. So check other attributes to confirm again that will trigger a cache refresh.
					try {
						Files.getLastModifiedTime(containerPath);
						fileExists = true;
					} catch (IOException e) {
						//Ignore
					}
				}
				return fileExists;
			} catch (Exception e) {
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
		return false;	
	}

	@Override
	public List<Path> listObjects(Path container) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				List<Path> objects = Files.walk(container).filter(path -> !path.equals(container)).collect(Collectors.toList());			
				return objects;
			} catch (IOException e) {
				tracer.error("Exception while listing objects in container " + container, e);				
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
		return Collections.EMPTY_LIST;
	}
	
	@Override
	public void deleteContainer(Path container) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete all objects first
				List<Path> objects = listObjects(container);
				for (Path obj : objects) {
					deleteObject(obj, null);
				}
				if (Files.exists(container)) {
					Files.delete(container);
				}
				return;
			} catch (IOException e) {
				tracer.error("Exception while deleting container from device stage : " + container, e);				
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
	}

	@Override
	public void createContainer(Path container, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Files.createDirectories(container);
				return;
			} catch (IOException e) {
				tracer.error("Exception while creating container in device stage : " + container, e);				
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
	}

	@Override
	public void uploadObject(Path container, Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Path targetPath = container.resolve(objectPath.getFileName());
				Files.copy(objectPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
				return;
			} catch (IOException e) {
				tracer.error("Exception while uploading object from device stage, object : " + objectPath, e);				
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
	}

}
