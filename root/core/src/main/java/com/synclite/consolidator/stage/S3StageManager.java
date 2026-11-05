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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;

public class S3StageManager extends DeviceStageManager {

	private S3Client s3Client;
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
				return objectExists(objectName);
			} catch (SdkException e) {
				tracer.error("Exception while checking if object exists in device stage for object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
	            try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(request -> request.bucket(bucketName).key(objectName))) {
	            	try (FileOutputStream outputStream = new FileOutputStream(outputFile.toFile())) {
	            		int read = 0;
	            		byte[] bytes = new byte[2048];
	            		while ((read = inputStream.read(bytes)) != -1) {
	            			outputStream.write(bytes, 0, read);
	            		}
	            		outputStream.flush();
	            	}
	                return inputStream.response().lastModified().toEpochMilli();
	            }
			} catch (S3Exception e) {
				if (e.statusCode() == 404 && "NoSuchKey".equals(e.awsErrorDetails().errorCode())) {
					//Object does not exist
					return 0;
				} else {
					tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
					if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
						throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
					} catch (InterruptedException e1) {
						Thread.currentThread().interrupt();
					}					
				}
			} catch (IOException | SdkException e) {
				tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
	            try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(request -> request.bucket(bucketName).key(objectName))) {
	            	fileDownloader.decryptAndWriteFile(inputStream, outputFile);
	                return inputStream.response().lastModified().toEpochMilli();
	            }
			} catch (S3Exception e) {
				if (e.statusCode() == 404 && "NoSuchKey".equals(e.awsErrorDetails().errorCode())) {
					//Object does not exist
					return 0;
				} else {
					tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
					if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
						throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
					} catch (InterruptedException e1) {
						Thread.currentThread().interrupt();
					}					
				}
			} catch (IOException | SdkException e) {
				tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
				s3Client.deleteObject(request -> request.bucket(bucketName).key(objectName));
				return;
			} catch (SdkException e) {
				tracer.error("Exception while deleting object from device stage, object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
			     for (S3Object objectSummary : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(objectPrefix)).contents()) {
					if (objectSummary.key().endsWith(suffix)) {
						return Path.of(objectSummary.key());
					}
			     }
			     return null;
			} catch (SdkException e) {
				tracer.error("Exception while finding an object from device stage with suffix : " + suffix + " in bucket : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
			     for (S3Object objectSummary : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(objectPrefix)).contents()) {
					if (objectSummary.key().startsWith(prefix) && objectSummary.key().endsWith(suffix)) {
						objects.add(Path.of(objectSummary.key()));
					}
			     }
			     return objects;
			} catch (SdkException e) {
				tracer.error("Exception while finding an object from device stage with prefix : " + prefix + " and suffix : " + suffix + " in bucket : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
				boolean found = objectExists(objectName);
				return found;
			} catch (SdkException e) {
				tracer.error("Exception while listing containers from device stage in container : ", e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
				for (S3Object ob : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(SyncLiteLoggerInfo.getDeviceDataRootPrefix())).contents()) {
					if (ob.key().endsWith("/")) {
						String obName = ob.key().substring(0, ob.key().length() - 1);
						deviceUploadRoots.add(Path.of(obName));
					}
				}
				return deviceUploadRoots;
			} catch (SdkException e) {
				tracer.error("Exception while listing containers from device stage in container : ", e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}
		}
		return deviceUploadRoots;
	}

	private void initializeStage(String bucketName) throws SyncLiteStageException {
		try {
			java.net.URI endpoint = java.net.URI.create(ConfLoader.getInstance().getStageS3Endpoint());
			Region defaultRegion = Region.of(System.getProperty("aws.region",
					System.getenv("AWS_REGION") == null ? "us-east-1" : System.getenv("AWS_REGION")));
			Region region;
			try {
				region = S3Utilities.builder().region(defaultRegion).build().parseUri(endpoint).region().orElse(defaultRegion);
			} catch (IllegalArgumentException exception) {
				region = defaultRegion;
			}
			String host = endpoint.getHost();
			boolean awsEndpoint = host != null && (host.endsWith(".amazonaws.com") || host.endsWith(".amazonaws.com.cn"));
			s3Client = S3Client.builder().endpointOverride(endpoint).region(region)
					.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
							ConfLoader.getInstance().getStageS3AccessKey(), ConfLoader.getInstance().getStageS3SecretKey())))
					.forcePathStyle(!awsEndpoint)
					.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
					.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED).build();
			this.bucketName = bucketName;
			throttleRequests = ConfLoader.getInstance().getThrottleStageRequestRate();
			requestIntervalMs =  60000 / ConfLoader.getInstance().getMaxStageRequestsPerMinute();
			if (requestIntervalMs == 0) {
				throttleRequests = false;
			}
			//Validate if we are connected
			s3Client.headBucket(request -> request.bucket(this.bucketName));
			
			if (this.writeBucketName != null) {
				s3Client.headBucket(request -> request.bucket(this.writeBucketName));
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
				 List<Path> objects = new ArrayList<Path>();
			     for (S3Object objectSummary : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(objectPrefix)).contents()) {
					objects.add(Path.of(objectSummary.key()));
			     }
				return objects;
			} catch (SdkException e) {
				tracer.error("Exception while listing objects in container " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
				for (ListObjectsV2Response page : s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName).prefix(containerName + "/"))) {
					List<ObjectIdentifier> keys = new ArrayList<>();
					for (S3Object object : page.contents()) {
						keys.add(ObjectIdentifier.builder().key(object.key()).build());
					}
					if (!keys.isEmpty()) {
						DeleteObjectsResponse response = s3Client.deleteObjects(request -> request.bucket(bucketName).delete(Delete.builder().objects(keys).build()));
						if (!response.errors().isEmpty()) {
							throw SdkClientException.create("S3 bulk deletion failed: " + response.errors());
						}
					}
				}
				s3Client.deleteObject(request -> request.bucket(bucketName).key(containerName + "/"));
				return;
			} catch (SdkException e) {
				tracer.error("Exception while deleting container from device stage : " + containerName, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	
	@Override
	public void createContainer(Path container, SyncLiteObjectType objType) throws SyncLiteStageException {
		String containerName = container.getFileName().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				s3Client.putObject(request -> request.bucket(bucketName).key(containerName + "/"), RequestBody.empty());
				return;
			} catch (SdkException e) {
				tracer.error("Exception while creating container in device stage : " + containerName, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
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
                s3Client.putObject(request -> request.bucket(bucketName).key(containerName + "/" + objectName), RequestBody.fromFile(objectPath));
				return;
			} catch (SdkException e) {
				tracer.error("Exception while uploading object " + objectPath + " in device stage in container : " + containerName, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs() );
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private boolean objectExists(String objectName) {
		try {
			s3Client.headObject(request -> request.bucket(bucketName).key(objectName));
			return true;
		} catch (S3Exception exception) {
			if (exception.statusCode() == 404) {
				return false;
			}
			throw exception;
		}
	}
}
