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

package com.synclite.consolidator.web;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateJobConfiguration
 */
@WebServlet("/validateDeviceStage")
public class ValidateDeviceStage extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateDeviceStage() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
			String stageOperRetryCountStr = request.getParameter("stage-oper-retry-count");
			String deviceEncryptionEnabledStr = request.getParameter("device-encryption-enabled");
			String enableDeviceCommandHandlerStr = request.getSession().getAttribute("enable-device-command-handler").toString();

			try {
				if (Long.valueOf(stageOperRetryCountStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Device Stage Operation Retry Count\"");
				} else if (Long.valueOf(stageOperRetryCountStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Device Stage Operation Retry Count\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Device Stage Operation Retry Count\"");
			}
			
			String stageOperRetryIntervalMsStr = request.getParameter("stage-oper-retry-interval-ms");
			try {
				if (Long.valueOf(stageOperRetryIntervalMsStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Device Stage Operation Retry Interval\"");
				} else if (Long.valueOf(stageOperRetryIntervalMsStr) <= 0) {
					throw new ServletException("Please specify a positive numeric value for \"Device Stage Operation Retry Interval\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Device Stage Operation Retry Interval\"");
			}

			String devicePollingIntervalMsStr = request.getParameter("device-polling-interval-ms");				
			try {
				if (Long.valueOf(devicePollingIntervalMsStr) == null) {
					throw new ServletException("Please specify a valid numeric value for \"Device Polling Interval\"");
				} else if (Long.valueOf(devicePollingIntervalMsStr) < 0) {
					throw new ServletException("Please specify a positive numeric value for \"Device Polling Interval\"");
				}
			} catch (NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Device Polling Interval\"");
			}

			String deviceStageType = request.getParameter("device-stage-type");
			if (deviceStageType.equals("FS")) {
				String deviceUploadRoot = request.getParameter("device-upload-root");
				Path deviceUploadRootPath;
				if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
					throw new ServletException("\"Device Data Stage Directory\" must be specified");
				} else {
					deviceUploadRootPath = Path.of(deviceUploadRoot);

					if (! Files.exists(deviceUploadRootPath)) {
						//If user is using default path then try creating these directories
						try {
							Files.createDirectories(deviceUploadRootPath);
						} catch (Exception e) {
							//Ignore
						}
					}

					if (! Files.isDirectory(deviceUploadRootPath)) {
						throw new ServletException("Specified \"Device Data Stage Directory \" : " + deviceUploadRoot + " does not exist, please specify a valid \"Upload Directory\"");
					}

					if (! deviceUploadRootPath.toFile().canRead()) {
						throw new ServletException("Specified \"Device Data Stage Directory\" does not have read permission");
					}				
				}

				if (! deviceUploadRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"Device Data Stage Directory\" does not have read permission");
				}
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);
				String deviceCommandRoot = request.getParameter("device-command-root");
				if (enableDeviceCommandHandlerStr.equals("true")) {
					Path deviceCommandRootPath;
					if ((deviceCommandRoot == null) || deviceCommandRoot.trim().isEmpty()) {
						throw new ServletException("\"Device Command Stage Directory\" must be specified when device command handler is enabled");
					} else {
						deviceCommandRootPath = Path.of(deviceCommandRoot);

						if (! Files.exists(deviceCommandRootPath)) {
							//If user is using default path then try creating these directories
							try {
								Files.createDirectories(deviceCommandRootPath);
							} catch (Exception e) {
								//Ignore
							}
						}

						if (! Files.isDirectory(deviceCommandRootPath)) {
							throw new ServletException("Specified \"Device Command Stage Directory \" : " + deviceCommandRoot + " does not exist, please specify a valid \"Command Directory\"");
						}

						if (! deviceCommandRootPath.toFile().canWrite()) {
							throw new ServletException("Specified \"Device Command Stage Directory\" does not have write permission");
						}
						
						if (! deviceCommandRootPath.toFile().canRead()) {
							throw new ServletException("Specified \"Device Command Stage Directory\" does not have read permission");
						}
					}
					request.getSession().setAttribute("device-command-root",deviceCommandRoot);
				} else {
					request.getSession().removeAttribute("device-command-root");
				}
			} else if (deviceStageType.equals("LOCAL_SFTP")) {
				String deviceUploadRoot = request.getParameter("device-upload-root");
				Path deviceUploadRootPath;
				if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
					throw new ServletException("\"SFTP Device Data Stage Directory\"" + "must be specified");
				} else {
					deviceUploadRootPath = Path.of(deviceUploadRoot);
				}	
				if (! Files.isDirectory(deviceUploadRootPath)) {
					throw new ServletException("Specified \"SFTP Device Data Stage Directory\" : " + deviceUploadRoot + " does not exist, please specify the valid \"SFTP Device Data Stage Directory\"");
				}

				if (! deviceUploadRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"SFTP Device Data Stage Directory\" does not have read permission");
				}				
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);
				
				String deviceCommandRoot = request.getParameter("device-command-root");
				if (enableDeviceCommandHandlerStr.equals("true")) {
					Path deviceCommandRootPath;
					if ((deviceCommandRoot == null) || deviceCommandRoot.trim().isEmpty()) {
						throw new ServletException("\"SFTP Device Command Stage Directory\" must be specified when device command handler is enabled");
					} else {
						deviceCommandRootPath = Path.of(deviceCommandRoot);

						if (! Files.isDirectory(deviceCommandRootPath)) {
							throw new ServletException("Specified \"SFTP Device Command Stage Directory \" : " + deviceCommandRoot + " does not exist, please specify a valid \"SFTP Device Command Directory\"");
						}

						if (! deviceCommandRootPath.toFile().canWrite()) {
							throw new ServletException("Specified \"SFTP Device Command Stage Directory\" does not have write permission");
						}
						
						if (! deviceCommandRootPath.toFile().canRead()) {
							throw new ServletException("Specified \"SFTP Device Command Stage Directory\" does not have read permission");
						}
					}
					request.getSession().setAttribute("device-command-root", deviceCommandRoot);
				} else {
					request.getSession().removeAttribute("device-command-root");
				}
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);								
			} else if (deviceStageType.equals("REMOTE_SFTP")) { 
				String stageSFTPHost = request.getParameter("stage-sftp-host");
				if ((stageSFTPHost == null) || stageSFTPHost.isBlank()) {
					throw new ServletException("\"Remote SFTP Server Host\"" + "must be specified");
				}

				String stageSFTPPortStr = request.getParameter("stage-sftp-port");
				try {
					if (Integer.valueOf(stageSFTPPortStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"SFTP Server Port\"");
					} else if (Integer.valueOf(stageSFTPPortStr) <= 0) {
						throw new ServletException("Please specify a positive numeric value for \"SFTP Server Port\"");
					}
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"SFTP Server Port\"");
				}

				String stageSFTPUser = request.getParameter("stage-sftp-user");
				if ((stageSFTPUser== null) || stageSFTPUser.isBlank()) {
					throw new ServletException("\"SFTP User\"" + "must be specified");
				}
				
				String stageSFTPPassword = request.getParameter("stage-sftp-password");
				if ((stageSFTPPassword == null) || stageSFTPPassword.isBlank()) {
					throw new ServletException("\"SFTP User Password\"" + "must be specified");
				}

				String stageSFTPDataDirectory = request.getParameter("stage-sftp-data-directory");
				if ((stageSFTPDataDirectory== null) || stageSFTPDataDirectory.isBlank()) {
					throw new ServletException("\"SFTP Data Stage Directory\"" + "must be specified");
				}
				
				String stageSFTPCommandDirectory = ""; 
				if (enableDeviceCommandHandlerStr.equals("true")) {
					stageSFTPCommandDirectory = request.getParameter("stage-sftp-command-directory");
					if ((stageSFTPCommandDirectory == null) || stageSFTPCommandDirectory.isBlank()) {
						throw new ServletException("\"SFTP Device Command Stage Directory\"" + "must be specified when device command handler is enabled");
					}
				}
				
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("stage-sftp-host",stageSFTPHost);
				request.getSession().setAttribute("stage-sftp-port",stageSFTPPortStr);
				request.getSession().setAttribute("stage-sftp-user",stageSFTPUser);
				request.getSession().setAttribute("stage-sftp-password",stageSFTPPassword);
				request.getSession().setAttribute("stage-sftp-data-directory",stageSFTPDataDirectory);
				if (enableDeviceCommandHandlerStr.equals("true")) {
					request.getSession().setAttribute("stage-sftp-command-directory",stageSFTPCommandDirectory);
				} else {
					request.getSession().removeAttribute("stage-sftp-command-directory");
				}
			} else if (deviceStageType.equals("MS_ONEDRIVE")) {
				String deviceUploadRoot = request.getParameter("device-upload-root");
				Path deviceUploadRootPath;
				if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
					throw new ServletException("\"Microsoft OneDrive Device Data Stage Folder\"" + "must be specified");
				} else {
					deviceUploadRootPath = Path.of(deviceUploadRoot);
				}	
				if (! Files.isDirectory(deviceUploadRootPath)) {
					throw new ServletException("Specified \"Microsoft OneDrive Device Data Stage Directory\" : " + deviceUploadRoot + " does not exist, please specify the valid \"Microsoft OneDrive Device Data Stage Directory\"");
				}

				if (! deviceUploadRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"Microsoft OneDrive Device Data Stage Directory\" does not have read permission");
				}				
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);
				
				if (enableDeviceCommandHandlerStr.equals("true")) {
					String deviceCommandRoot = request.getParameter("device-command-root");
					Path deviceCommandRootPath;
					if ((deviceCommandRoot == null) || deviceCommandRoot.trim().isEmpty()) {
						throw new ServletException("\"Microsoft OneDrive Device Command Stage Directory\"" + "must be specified");
					} else {
						deviceCommandRootPath = Path.of(deviceCommandRoot);
					}	
					if (! Files.isDirectory(deviceCommandRootPath)) {
						throw new ServletException("Specified \"Microsoft OneDrive Device Command Stage Directory\" : " + deviceCommandRoot + " does not exist, please specify the valid \"Microsoft OneDrive Device Command Stage Directory\"");
					}

					if (! deviceCommandRootPath.toFile().canRead()) {
						throw new ServletException("Specified \"Microsoft OneDrive Device Command Stage Directory\" does not have read permission");
					}				
					request.getSession().setAttribute("device-command-root",deviceCommandRoot);
				} else {
					request.getSession().removeAttribute("device-command-root");
				}
			} else if (deviceStageType.equals("GOOGLE_DRIVE")) {
				String deviceUploadRoot = request.getParameter("device-upload-root");
				Path deviceUploadRootPath;
				if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
					throw new ServletException("\"Google Drive Device Data Stage Directory \"" + "must be specified");
				} else {
					deviceUploadRootPath = Path.of(deviceUploadRoot);
				}	
				if (! Files.isDirectory(deviceUploadRootPath)) {
					throw new ServletException("Specified \"Google Drive Device Data Stage Diretory\" : " + deviceUploadRoot + " does not exist, please specify the valid \"Google Drive Data Stage Folder\"");
				}

				if (! deviceUploadRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"Google Drive Device Data Stage Directory\" does not have read permission");
				}				
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);

				if (enableDeviceCommandHandlerStr.equals("true")) {
					String deviceCommandRoot = request.getParameter("device-command-root");
					Path deviceCommandRootPath;
					if ((deviceCommandRoot == null) || deviceCommandRoot.trim().isEmpty()) {
						throw new ServletException("\"Google Drive Device Command Stage Diretory\"" + "must be specified when device command handler is enabled");
					} else {
						deviceCommandRootPath = Path.of(deviceCommandRoot);
					}	
					if (! Files.isDirectory(deviceCommandRootPath)) {
						throw new ServletException("Specified \"Google Drive Device Command Stage Directory\" : " + deviceCommandRoot + " does not exist, please specify the valid \"Google Drive Device Command Stage Directory\"");
					}

					if (! deviceCommandRootPath.toFile().canRead()) {
						throw new ServletException("Specified \"Google Drive Device Command Stage Directory\" does not have read permission");
					}				
					request.getSession().setAttribute("device-command-root",deviceCommandRoot);
				} else {
					request.getSession().removeAttribute("device-command-root");
				}
			} else if (deviceStageType.equals("LOCAL_MINIO")) {
				String deviceUploadRoot = request.getParameter("device-upload-root");
				Path deviceUploadRootPath;
				if ((deviceUploadRoot == null) || deviceUploadRoot.trim().isEmpty()) {
					throw new ServletException("\"MinIO Device Data Stage Bucket Storage Path\"" + "must be specified");
				} else {
					deviceUploadRootPath = Path.of(deviceUploadRoot);
				}	
				if (! Files.isDirectory(deviceUploadRootPath)) {
					throw new ServletException("Specified \"MinIO Device Data Stage Bucket Storage Path\" : " + deviceUploadRoot + " does not exist, please specify valid \"MinIO Object Storage Storage Directory\"");
				}

				if (! deviceUploadRootPath.toFile().canRead()) {
					throw new ServletException("Specified \"MinIO Device Data Stage Bucket Storage Path\" does not have read permission");
				}

				String minioEndPoint = request.getParameter("stage-minio-endpoint");
				if ((minioEndPoint == null) || (minioEndPoint.isBlank())) {
					throw new ServletException("\"MinIO Endpoint\"" + "must be specified");
				}

				String minioDataBucketName = request.getParameter("stage-minio-data-bucket-name");
				if ((minioDataBucketName == null) || minioDataBucketName.isBlank()) {
					throw new ServletException("\"MinIO Device Data Stage Bucket Name\"" + "must be specified");
				}
				String minioCommandBucketName = ""; 
				if (enableDeviceCommandHandlerStr.equals("true")) {
					minioCommandBucketName = request.getParameter("stage-minio-command-bucket-name");
					if ((minioCommandBucketName == null) || minioCommandBucketName.isBlank()) {
						throw new ServletException("\"MinIO Device Command Stage Bucket Name\"" + "must be specified when device command handler is enabled");
					}
				}

				String minioAccessKey = request.getParameter("stage-minio-access-key");
				if ((minioAccessKey == null) || (minioAccessKey.isBlank())) {
					throw new ServletException("\"MinIO Access Key\"" + "must be specified");
				}

				String minioSecretKey = request.getParameter("stage-minio-secret-key");
				if ((minioSecretKey == null) || (minioSecretKey.isBlank())) {
					throw new ServletException("\"MinIO Secret Key\"" + "must be specified");
				}
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("device-upload-root",deviceUploadRoot);
				
				if (enableDeviceCommandHandlerStr.equals("true")) {
					request.getSession().setAttribute("stage-minio-command-bucket-name",minioCommandBucketName);
				} else {
					request.getSession().removeAttribute("stage-minio-command-bucket-name");
				}
				request.getSession().setAttribute("stage-minio-endpoint",minioEndPoint);
				request.getSession().setAttribute("stage-minio-data-bucket-name",minioDataBucketName);
				request.getSession().setAttribute("stage-minio-access-key",minioAccessKey);
				request.getSession().setAttribute("stage-minio-secret-key",minioSecretKey);
			} else if (deviceStageType.equals("REMOTE_MINIO")) {
				String minioEndPoint = request.getParameter("stage-minio-endpoint");
				if ((minioEndPoint == null) || minioEndPoint.isBlank()) {
					throw new ServletException("\"MinIO Endpoint\"" + "must be specified");
				}

				String minioDataBucketName = request.getParameter("stage-minio-data-bucket-name");
				if ((minioDataBucketName == null) || minioDataBucketName.isBlank()) {
					throw new ServletException("\"MinIO Device Data Stage Bucket Name\"" + "must be specified");
				}

				String minioCommandBucketName = ""; 
				if (enableDeviceCommandHandlerStr.equals("true")) {
					minioCommandBucketName = request.getParameter("stage-minio-command-bucket-name");
					if ((minioCommandBucketName == null) || minioCommandBucketName.isBlank()) {
						throw new ServletException("\"MinIO Device Command Stage Bucket Name\"" + "must be specified");
					}					
				}

				String minioAccessKey = request.getParameter("stage-minio-access-key");
				if ((minioAccessKey == null) || minioAccessKey.isBlank()) {
					throw new ServletException("\"MinIO Access Key\"" + "must be specified");
				}

				String minioSecretKey = request.getParameter("stage-minio-secret-key");
				if ((minioSecretKey == null) || minioSecretKey.isBlank()) {
					throw new ServletException("\"MinIO Secret Key\"" + "must be specified");
				}
				
				String deviceScannerIntervalSStr = request.getParameter("device-scanner-interval-s");
				
				try {
					if (Long.valueOf(deviceScannerIntervalSStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"New Device Scan Interval\"");
					} else if (Long.valueOf(deviceScannerIntervalSStr) <= 0) {
						throw new ServletException("Please specify a positive numeric value for \"New Device Scan Interval\"");
					}
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"New Device Scan Interval\"");
				}

				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("stage-minio-endpoint",minioEndPoint);
				request.getSession().setAttribute("stage-minio-data-bucket-name",minioDataBucketName);
				if (enableDeviceCommandHandlerStr.equals("true")) {
					request.getSession().setAttribute("stage-minio-command-bucket-name",minioCommandBucketName);
				} else {
					request.getSession().removeAttribute("stage-minio-command-bucket-name");
				}
				request.getSession().setAttribute("stage-minio-access-key",minioAccessKey);
				request.getSession().setAttribute("stage-minio-secret-key",minioSecretKey);
				request.getSession().setAttribute("device-scanner-interval-s",deviceScannerIntervalSStr);
			} else if (deviceStageType.equals("S3")) {
				String s3EndPoint = request.getParameter("stage-s3-endpoint");
				if ((s3EndPoint == null) || s3EndPoint.isBlank()) {
					throw new ServletException("\"S3 Endpoint\"" + "must be specified");
				}

				String s3DataBucketName = request.getParameter("stage-s3-data-bucket-name");
				if ((s3DataBucketName == null) || s3DataBucketName.isBlank()) {
					throw new ServletException("\"S3 Device Data Stage Bucket Name\"" + "must be specified");
				}

				String s3CommandBucketName = ""; 
				if (enableDeviceCommandHandlerStr.equals("true")) {
					s3CommandBucketName = request.getParameter("stage-s3-command-bucket-name");
					if ((s3CommandBucketName == null) || s3CommandBucketName.isBlank()) {
						throw new ServletException("\"S3 Device Command Stage Bucket Name\"" + "must be specified when device command handler is enabled");
					}					
				}

				String s3AccessKey = request.getParameter("stage-s3-access-key");
				if ((s3AccessKey == null) || s3AccessKey.isBlank()) {
					throw new ServletException("\"S3 Access Key\"" + "must be specified");
				}

				String s3SecretKey = request.getParameter("stage-s3-secret-key");
				if ((s3SecretKey == null) || s3SecretKey.isBlank()) {
					throw new ServletException("\"S3 Secret Key\"" + "must be specified");
				}

				String throttleStageRequestRateStr = request.getParameter("throttle-stage-request-rate");
				try {
					if (Boolean.valueOf(throttleStageRequestRateStr) == null) {
						throw new ServletException("Please specify a valid boolean value for \"Throttle Request Rate\"");
					} 
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"Throttle Request Rate\"");					
				}

				String maxStageRequestsPerMinute = "2";
				if (request.getParameter("max-stage-requests-per-minute") != null) {
					maxStageRequestsPerMinute = request.getParameter("max-stage-requests-per-minute");
					try {
						if (Long.valueOf(maxStageRequestsPerMinute) == null) {
							throw new ServletException("Please specify a valid numeric value for \"Maximum Requests Per Minute\"");
						} else if (Long.valueOf(maxStageRequestsPerMinute) <= 0) {
							throw new ServletException("Please specify a positive numeric value for \"Maximum Requests Per Minute\"");
						}
					} catch (NumberFormatException e) {
						throw new ServletException("Please specify a valid numeric value for \"Maximum Requests Per Minute\"");					
					}
				}

				String deviceScannerIntervalSStr = request.getParameter("device-scanner-interval-s");
				try {
					if (Long.valueOf(deviceScannerIntervalSStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"New Device Scan Interval\"");
					} else if (Long.valueOf(deviceScannerIntervalSStr) <= 0) {
						throw new ServletException("Please specify a positive numeric value for \"New Device Scan Interval\"");
					}
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"New Device Scan Interval\"");					
				}
				
				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("stage-s3-endpoint", s3EndPoint);
				request.getSession().setAttribute("stage-s3-data-bucket-name", s3DataBucketName);
				if (enableDeviceCommandHandlerStr.equals("true")) {
					request.getSession().setAttribute("stage-s3-command-bucket-name",s3CommandBucketName);
				} else {
					request.getSession().removeAttribute("stage-s3-command-bucket-name");
				}
				request.getSession().setAttribute("stage-s3-access-key", s3AccessKey);
				request.getSession().setAttribute("stage-s3-secret-key", s3SecretKey);
				request.getSession().setAttribute("throttle-stage-request-rate", throttleStageRequestRateStr);
				request.getSession().setAttribute("max-stage-requests-per-minute", maxStageRequestsPerMinute);
				request.getSession().setAttribute("device-scanner-interval-s",deviceScannerIntervalSStr);
			} else if (deviceStageType.equals("KAFKA")) {
				
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();

				String kafkaConsumerProperties = request.getParameter("stage-kafka-consumer-properties");
				if ((kafkaConsumerProperties == null) || (kafkaConsumerProperties.isBlank())) {
					throw new ServletException("\"Device Stage Kafka Consumer Propeties\"" + "must be specified");
				}			
				String stageKafkaConsumerPropsPath = Path.of(deviceDataRoot, "stage_kafka_consumer.properties").toString();				
				try {
					Files.writeString(Path.of(stageKafkaConsumerPropsPath), kafkaConsumerProperties);
				} catch (IOException e) {
					throw new ServletException("Failed to write stage kafka consumer properties into file : " + stageKafkaConsumerPropsPath + " : " + e.getMessage(), e);
				}
				
				String stageKafkaProducerPropsPath = "";
				if (enableDeviceCommandHandlerStr.equals("true")) {
					String stageKafkaProducerProperties = request.getParameter("stage-kafka-producer-properties");
					if ((stageKafkaProducerProperties == null) || (stageKafkaProducerProperties.isBlank())) {
						throw new ServletException("\"Stage Kafka Producer Properties\"" + "must be specified when device command handler is enabled");
					}
					stageKafkaProducerPropsPath = Path.of(deviceDataRoot, "stage_kafka_producer.properties").toString();
					try {
						Files.writeString(Path.of(stageKafkaProducerPropsPath), stageKafkaProducerProperties);
					} catch (IOException e) {
						throw new ServletException("Failed to write stage kafka producer properties into file : " + stageKafkaProducerPropsPath + " : " + e.getMessage(), e);
					}
				}			

				String deviceScannerIntervalSStr = request.getParameter("device-scanner-interval-s");
				try {
					if (Long.valueOf(deviceScannerIntervalSStr) == null) {
						throw new ServletException("Please specify a valid numeric value for \"Device Scan Interval\"");
					} else if (Long.valueOf(stageOperRetryIntervalMsStr) <= 0) {
						throw new ServletException("Please specify a positive numeric value for \"Device Scan Interval\"");
					}
				} catch (NumberFormatException e) {
					throw new ServletException("Please specify a valid numeric value for \"Device Scan Interval\"");
				}

				request.getSession().setAttribute("device-stage-type",deviceStageType);
				request.getSession().setAttribute("stage-kafka-consumer-properties-file", stageKafkaConsumerPropsPath.toString());
				if (enableDeviceCommandHandlerStr.equals("true")) {
					request.getSession().setAttribute("stage-kafka-producer-properties-file", stageKafkaProducerPropsPath.toString());
				} else {
					request.getSession().removeAttribute("stage-kafka-producer-properties-file");
				}
				request.getSession().setAttribute("device-scanner-interval-s",deviceScannerIntervalSStr);
			}
			
			request.getSession().setAttribute("device-encryption-enabled", deviceEncryptionEnabledStr);
			String deviceDecryptionKeyFileStr = ""; 
			if (deviceEncryptionEnabledStr.equals("true")) {
				deviceDecryptionKeyFileStr = request.getParameter("device-decryption-key-file");
				Path deviceDecryptionKeyFilePath;
				if ((deviceDecryptionKeyFileStr == null) || deviceDecryptionKeyFileStr.trim().isEmpty()) {
					throw new ServletException("\"Device Decryption Key File\" must be specified");
				} else {
					deviceDecryptionKeyFilePath = Path.of(deviceDecryptionKeyFileStr);
					if (! Files.exists(deviceDecryptionKeyFilePath)) {
						throw new ServletException("Specified \"Device Decryption Key File\" : " + deviceDecryptionKeyFilePath + " does not exist, please specify a valid \"Device Decryption Key File\"");
					}
				}
				request.getSession().setAttribute("device-decryption-key-file", deviceDecryptionKeyFileStr);
			} else {
				request.getSession().removeAttribute("device-decryption-key-file");
			}
			
			
			String deviceSchedulerTypeStr = "POLLING";
			if (request.getParameter("device-scheduler-type") != null) {
				deviceSchedulerTypeStr = request.getParameter("device-scheduler-type");
			}
			
			request.getSession().setAttribute("device-scheduler-type", deviceSchedulerTypeStr);
			request.getSession().setAttribute("device-polling-interval-ms", devicePollingIntervalMsStr);
			request.getSession().setAttribute("stage-oper-retry-count", stageOperRetryCountStr);
			request.getSession().setAttribute("stage-oper-retry-interval-ms", stageOperRetryIntervalMsStr);	
			
			//request.getRequestDispatcher("configureNumDestinations.jsp").forward(request, response);			
			response.sendRedirect("configureNumDestinations.jsp");
		} catch (Exception e) {
			//throw e;
			//		request.setAttribute("saveStatus", "FAIL");
			//System.out.println("exception : " + e);
			//String errorMsg = e.toString() + e.getLocalizedMessage() + e.getCause().getMessage() +  e.getMessage();
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureDeviceStage.jsp?errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
