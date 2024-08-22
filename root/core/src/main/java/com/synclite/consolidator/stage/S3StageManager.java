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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.AwsHostNameUtils;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;

public class S3StageManager extends DeviceStageManager {

	private AmazonS3 s3Client; 
	private String bucketName;
	private String writeBucketName;
	private long requestIntervalMs;
	private boolean throttleRequests;
	private volatile long lastRequestTime;
	
	@Override
	@Deprecated
	public boolean objectExists(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		//Throttling check
		if (!allowRequest(objType)) {
			return false;
		}
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();
				return s3Client.doesObjectExist(bucketName, objectName); 
			} catch (AmazonClientException e) {
				tracer.error("Exception while checking if object exists in device stage for object : " + objectPath, e);				
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
	public long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType)
			throws SyncLiteStageException {
		if (!allowRequest(objType)) {
			//Throttling request rate, return 0 as if object is not present.
			return 0;
		}
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete the file at outputFile path		
				if (Files.exists(outputFile)) {
					Files.delete(outputFile);
				}
				Files.createFile(outputFile);
	            S3Object s3Object = s3Client.getObject(bucketName, objectName);	            
	            try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
	            	try (FileOutputStream outputStream = new FileOutputStream(outputFile.toFile())) {
	            		int read = 0;
	            		byte[] bytes = new byte[2048];
	            		while ((read = inputStream.read(bytes)) != -1) {
	            			outputStream.write(bytes, 0, read);
	            		}
	            		outputStream.flush();
	            	}
	            }
	            
	            return s3Object.getObjectMetadata().getLastModified().getTime();			
			} catch (AmazonS3Exception e) {
				if (e.getErrorCode().equals("NoSuchKey") && (e.getStatusCode() == 404)) {
					//Object does not exist
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
		if (!allowRequest(objType)) {
			//Throttling request rate, return 0 as if object is not present.
			return 0;
		}
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				S3Object s3Object = s3Client.getObject(bucketName, objectName);	            
	            try (InputStream inputStream = s3Object.getObjectContent()) {
	            	fileDownloader.decryptAndWriteFile(inputStream, outputFile);
	            }	            
	            return s3Object.getObjectMetadata().getLastModified().getTime();			
			} catch (AmazonS3Exception e) {
				if (e.getErrorCode().equals("NoSuchKey") && (e.getStatusCode() == 404)) {
					//Object does not exist
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
	public void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		String objectName = objectPath.getParent().getFileName().toString() + "/" + objectPath.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				s3Client.deleteObject(bucketName, objectName);
				return;
			} catch (AmazonClientException e) {
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
		//Throttling check
		if (!allowRequest(objType)) {
			return null;
		}
		String objectPrefix = container.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
			     ListObjectsV2Result result = s3Client.listObjectsV2 (bucketName, objectPrefix);
			     for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					if (objectSummary.getKey().endsWith(suffix)) {
						return Path.of(objectSummary.getKey());
					}
			     }
			     return null;
			} catch (AmazonClientException e) {
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
		//Throttling check
		if (!allowRequest(objType)) {
			return null;
		}
		String objectPrefix = container.getFileName().toString() + "/";
		List<Path> objects = new ArrayList<Path>();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
			     ListObjectsV2Result result = s3Client.listObjectsV2 (bucketName, objectPrefix);
			     for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					if (objectSummary.getKey().startsWith(prefix) && objectSummary.getKey().endsWith(suffix)) {
						objects.add(Path.of(objectSummary.getKey()));						
					}
			     }
			     return objects;
			} catch (AmazonClientException e) {
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
	public boolean containerExists(Path containerPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		String objectName = containerPath.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				boolean found = s3Client.doesObjectExist(bucketName, objectName);
				return found;
			} catch (AmazonClientException e) {
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
	public List<Path> listContainers(Path startFrom, SyncLiteObjectType objType) throws SyncLiteStageException {
		List<Path> deviceUploadRoots = new ArrayList<Path>();
		//Throttling check
		if (!allowRequest(SyncLiteObjectType.DATA_CONTAINER)) {
			return deviceUploadRoots;
		}
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				ListObjectsV2Result result = s3Client.listObjectsV2(bucketName, SyncLiteLoggerInfo.getDeviceDataRootPrefix());				
				for (S3ObjectSummary ob : result.getObjectSummaries()) {
					if (ob.getKey().endsWith("/")) {
						String obName = ob.getKey().substring(0, ob.getKey().length() - 1);
						deviceUploadRoots.add(Path.of(obName));
					}
				}
				return deviceUploadRoots;
			} catch (AmazonClientException e) {
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

	private void initializeStage(String bucketName) throws SyncLiteStageException {
		try {
	        AWSCredentials credentials = new BasicAWSCredentials(ConfLoader.getInstance().getStageS3AccessKey(), ConfLoader.getInstance().getStageS3SecretKey());

			s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(new EndpointConfiguration(ConfLoader.getInstance().getStageS3Endpoint(), AwsHostNameUtils.parseRegion(ConfLoader.getInstance().getStageS3Endpoint(), null)))
                    .build();
			this.bucketName = bucketName;
			throttleRequests = ConfLoader.getInstance().getThrottleStageRequestRate();
			requestIntervalMs =  60000 / ConfLoader.getInstance().getMaxStageRequestsPerMinute();
			if (requestIntervalMs == 0) {
				throttleRequests = false;
			}
			//Validate if we are connected
			s3Client.doesBucketExistV2(this.bucketName);
			
			if (this.writeBucketName != null) {
				s3Client.doesBucketExistV2(this.writeBucketName);
			}
		} catch (Exception e) {
			tracer.error("Failed to create a client/connect to S3", e);
			throw new SyncLiteStageException("Device stage unreachable. Failed to create a client/connect to S3. Failed to initialize S3 Device Stage ", e);
		}
	}
	
	@Override
	public void initializeDataStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initializeStage(ConfLoader.getInstance().getStageS3DataBucketName());
	}

	@Override
	public void initializeCommandStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initializeStage(ConfLoader.getInstance().getStageS3CommandBucketName());
	}
	
	private final boolean allowRequest(SyncLiteObjectType objType) {
		if (this.throttleRequests == false) {
			return true;
		} else {
			if (objType == SyncLiteObjectType.DATA) {
				//No locking/throttling for data file download as it is a one time activity and guarded by 
				//metadata file download
				return true;
			}
			return getRequestLock();
		}		
	}
	
	private final synchronized boolean getRequestLock() {
		long currentTime = System.currentTimeMillis();
		if ((currentTime - this.lastRequestTime) > requestIntervalMs) {
			this.lastRequestTime = currentTime;
			return true;
		}
		return false;
	}
	
	@Override
	public List<Path> listObjects(Path container) throws SyncLiteStageException {
		String objectPrefix = container.getFileName().toString() + "/";
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
			     ListObjectsV2Result result = s3Client.listObjectsV2 (bucketName, objectPrefix);
				 List<Path> objects = new ArrayList<Path>();
			     for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					objects.add(Path.of(objectSummary.getKey()));
			     }
				return objects;
			} catch (AmazonClientException e) {
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
		String containerName = container.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete all objects first				
				DeleteObjectsRequest request = new DeleteObjectsRequest(containerName);
				s3Client.deleteObjects(request);
				s3Client.deleteObject(bucketName, container.getFileName().toString());
				return;
			} catch (AmazonClientException e) {
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
	       		s3Client.putObject(writeBucketName, containerName + "/", new ByteArrayInputStream(new byte[0]), new ObjectMetadata());
				return;
			} catch (AmazonClientException e) {
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
                s3Client.putObject(bucketName, containerName + "/" + objectName, objectPath.toFile());
				return;
			} catch (AmazonClientException e) {
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
