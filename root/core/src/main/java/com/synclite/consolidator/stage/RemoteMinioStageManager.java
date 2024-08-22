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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;

public class RemoteMinioStageManager extends DeviceStageManager {

	protected MinioClient minioClient;
	protected String bucketName;

	protected RemoteMinioStageManager() {
	}

	protected void initialize(String bucketName) throws SyncLiteStageException {
		try {
			this.minioClient =
					MinioClient.builder()
					.endpoint(ConfLoader.getInstance().getStageMinioEndpoint())					
					.credentials(ConfLoader.getInstance().getStageMinioAccessKey(), ConfLoader.getInstance().getStageMinioSecretKey())
					.build();

			this.bucketName = bucketName;
			
			//Validate if we are connected
			minioClient.bucketExists(BucketExistsArgs.builder().bucket(this.bucketName).build());

		} catch (Exception e) {
			tracer.error("Failed to create a client/connect to MinIO server", e);
			throw new SyncLiteStageException("Device stage unreachable. Failed to create a client/connect to MinIO server. Failed to initialize Minio Device Stage ", e);
		}
	}
	
	@Override
	public void initializeDataStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initialize(ConfLoader.getInstance().getStageMinioDataBucketName());
	}

	@Override
	public void initializeCommandStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initialize(ConfLoader.getInstance().getStageMinioCommandBucketName());
	}
	
	
	@Deprecated
	public boolean objectExists(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				String objectName = objectPath.getFileName().toString();
				String bucketName = objectPath.getParent().getFileName().toString();
				minioClient.statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
				return true;
			} catch (InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
				tracer.error("Exception while checking if object exists in device stage for object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			} catch (ErrorResponseException e) {
				return false;
			}
		}
		return false;		
	}

	@Override
	public long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete the file at outputFile path		
				if (Files.exists(outputFile)) {
					Files.delete(outputFile);
				}
				
				minioClient.downloadObject(
						DownloadObjectArgs.builder()
						.bucket(this.bucketName)
						.object(objectName)
						.filename(outputFile.toString())
						.overwrite(true)
						.build());
				StatObjectResponse statObjectResponse =
						minioClient.statObject(
								StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
				ZonedDateTime dt = statObjectResponse.lastModified();
				long publishTime = dt.toEpochSecond() * 1000;
				return publishTime;				
			} catch (ErrorResponseException e) {
				if (e.errorResponse().code().equals("NoSuchKey")) {
					return 0;
				} else {
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
			} catch (InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
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
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {				
				try (InputStream fis = minioClient.getObject(
						GetObjectArgs.builder()
						.bucket(this.bucketName)
						.object(objectName)
						.build())) {
					fileDownloader.decryptAndWriteFile(fis, outputFile);
				}
				StatObjectResponse statObjectResponse =
						minioClient.statObject(
								StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
				ZonedDateTime dt = statObjectResponse.lastModified();
				long publishTime = dt.toEpochSecond() * 1000;
				return publishTime;				
			} catch (ErrorResponseException e) {
				if (e.errorResponse().code().equals("NoSuchKey")) {
					return 0;
				} else {
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
			} catch (InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
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
	public void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				minioClient.removeObject(
						RemoveObjectArgs.builder()
						.bucket(this.bucketName)
						.object(objectName)
						.build());

				return;
			} catch (ErrorResponseException | InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
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
	public List<Path> listObjects(Path container) throws SyncLiteStageException {
		String objectPrefix = container.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Iterable<Result<Item>> results =
						minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(objectPrefix).build());
				List<Path> objects = new ArrayList<Path>();
				for (Result<Item> result : results) {
					Item item = result.get();
					objects.add(Path.of(container.toString(), item.objectName()));
				}
				return objects;
			} catch (IOException | InvalidKeyException | ErrorResponseException | IllegalArgumentException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException e) {
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
	public Path findObjectWithSuffix(Path container, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException {
		String objectPrefix = container.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Iterable<Result<Item>> results =
						minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(objectPrefix).build());
				for (Result<Item> result : results) {
					Item item = result.get();
					if (item.objectName().endsWith(suffix)) {
						return Path.of(container.toString(), item.objectName());
					}
				}
				return null;
			} catch (IOException | InvalidKeyException | ErrorResponseException | IllegalArgumentException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException e) {
				tracer.error("Exception while finding an object from device stage with suffix : " + suffix + " in bucket : " + container, e);				
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

	
	@Override
	public List<Path> findObjectsWithSuffixPrefix(Path container, String prefix, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException  {
		List<Path> objects = new ArrayList<Path>();
		String objectPrefix = container.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Iterable<Result<Item>> results =
						minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(objectPrefix).build());
				for (Result<Item> result : results) {
					Item item = result.get();
					if (item.objectName().startsWith(prefix) && item.objectName().endsWith(suffix)) {
						objects.add(Path.of(container.toString(), item.objectName()));
					}
				}
				return objects;
			} catch (IOException | InvalidKeyException | ErrorResponseException | IllegalArgumentException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException e) {
				tracer.error("Exception while finding an object from device stage with prefix : " + prefix + " and suffix : " + suffix + " in bucket : " + container, e);				
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
				Iterable<Result<Item>> objects = minioClient.listObjects(ListObjectsArgs.builder().bucket(this.bucketName).prefix(SyncLiteLoggerInfo.getDeviceDataRootPrefix()).build());
				for (Result<Item> o : objects) {
					String obName = o.get().objectName();
					if (obName.endsWith("/")) {
						deviceUploadRoots.add(Path.of(obName));
					}
				}
				return deviceUploadRoots;						
			} catch (IOException | InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException e) {
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
		String objectName = containerPath.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {				
				minioClient.statObject(StatObjectArgs.builder().bucket(this.bucketName).object(objectName).build());						
				return true;				
			} catch(ErrorResponseException e) {
				if (e.errorResponse().code().equals("NoSuchKey")) {
					return false;
				} else {
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
			} catch (IOException | InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException e) {
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
	public void deleteContainer(Path container) throws SyncLiteStageException {
		String containerName = container.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete all objects first
				List<Path> objects = listObjects(container);
				for (Path obj : objects) {
					deleteObject(obj, null);
				}

				minioClient.removeObject(
						RemoveObjectArgs.builder()
						.bucket(this.bucketName)
						.object(containerName)
						.build());

				return;
			} catch (ErrorResponseException | InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
				tracer.error("Exception while deleting container from device stage : " + containerName, e);				
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
		String containerName = container.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				PutObjectArgs putObjArgs = PutObjectArgs.builder()
						.bucket(this.bucketName)
						.object(containerName + "/")
						.stream(new ByteArrayInputStream(new byte[0]), 0, -1)
						.build();     	    		

				minioClient.putObject(putObjArgs);
				return;
			} catch (ErrorResponseException | InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
				tracer.error("Exception while creating container in device stage : " + containerName, e);				
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
	public void uploadObject(Path container, Path objectPath, SyncLiteObjectType objType)
			throws SyncLiteStageException {
		String containerName = container.getFileName().toString();
		String objectName = objectPath.getFileName().toString();

		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				minioClient.uploadObject(
						UploadObjectArgs.builder()
						.bucket(this.bucketName)
						.object(containerName + "/" + objectName)
						.filename(objectPath.toString())
						.build());
				return;
			} catch (ErrorResponseException | InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException | IllegalArgumentException | IOException e) {
				tracer.error("Exception while uploading object " + objectPath + " in device stage in container : " + containerName, e);				
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
