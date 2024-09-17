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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Servlet implementation class SaveJobConfiguration
 */
@WebServlet("/saveJobConfiguration")
public class SaveJobConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger globalTracer;

	/**
	 * Default constructor. 
	 */
	public SaveJobConfiguration() {
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

			String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
			String corePath = Path.of(getServletContext().getRealPath("/"), "WEB-INF", "lib").toString();
			String propsPath = Path.of(deviceDataRoot, "synclite_consolidator.conf").toString();

			initTracer(Path.of(deviceDataRoot));

			StringBuilder builder = new StringBuilder();

			/*
			 * 
			 * 
				device-data-root = E:\database\jobDir
				device-upload-root = E:\database\dataDir
				#device-command-root = E:\database
				#device-tracing = true
				#syncer-role = REPLICATOR
				num-device-processors = 8
				failed-device-retry-interval = 5
				dst-type = MSSQL
				#dst-connection-string = jdbc:sqlserver://localhost;databaseName=synclitedb;user=syncliteuser;password=pass#123
				dst-connection-string = jdbc:sqlserver://localhost;databaseName=synclitedb;user=syncliteuser;password=pass
				dst-database = synclitedb
				dst-schema = dbo
				dst-sync-mode = CONSOLIDATION
				dst-batch-size = 5000
				dst-strong-typing = true
				syncer-role = ALL
				#dst-txn-retry-count=10
				#dst-oper-predicate-optimization=true
				#map-src-integer-to-dst=integer
				gui-dashbord=true

			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 */

			HttpSession session = request.getSession();

			//Job configuration
			builder.append("job-name = ").append(session.getAttribute("job-name").toString()).append("\n");
			builder.append("device-data-root = ").append(session.getAttribute("device-data-root").toString()).append("\n");
			builder.append("src-app-type = ").append(session.getAttribute("src-app-type").toString()).append("\n");
			//builder.append("license-file = ").append(session.getAttribute("license-file").toString()).append("\n");
			builder.append("dst-sync-mode = ").append(session.getAttribute("dst-sync-mode").toString()).append("\n");
			builder.append("num-device-processors = ").append(session.getAttribute("num-device-processors").toString()).append("\n");
			builder.append("failed-device-retry-interval-s = ").append(session.getAttribute("failed-device-retry-interval-s").toString()).append("\n");
			builder.append("device-trace-level = ").append(session.getAttribute("device-trace-level").toString()).append("\n");
			builder.append("syncer-role = ").append("ALL").append("\n");
			if (!session.getAttribute("device-name-pattern").toString().trim().equals(".*")) {
				builder.append("device-name-pattern = ").append(session.getAttribute("device-name-pattern").toString()).append("\n");
			}
			if (!session.getAttribute("device-id-pattern").toString().trim().equals(".*")) {
				builder.append("device-id-pattern = ").append(session.getAttribute("device-id-pattern").toString()).append("\n");
			}
			if (session.getAttribute("enable-replicas-for-telemetry-devices") != null) {
				builder.append("enable-replicas-for-telemetry-devices = ").append(session.getAttribute("enable-replicas-for-telemetry-devices").toString()).append("\n");
			}

			if (session.getAttribute("disable-replicas-for-appender-devices") != null) {
				builder.append("disable-replicas-for-appender-devices = ").append(session.getAttribute("disable-replicas-for-appender-devices").toString()).append("\n");
			}

			if (session.getAttribute("skip-bad-txn-files") != null) {
				builder.append("skip-bad-txn-files = ").append(session.getAttribute("skip-bad-txn-files").toString()).append("\n");
			}

			if (session.getAttribute("enable-request-processor") != null) {
				builder.append("enable-request-processor = ").append(session.getAttribute("enable-request-processor").toString()).append("\n");
			}

			if (session.getAttribute("request-processor-port") != null) {
				builder.append("request-processor-port = ").append(session.getAttribute("request-processor-port").toString()).append("\n");
			}

			if (session.getAttribute("enable-device-command-handler") != null) {
				builder.append("enable-device-command-handler = ").append(session.getAttribute("enable-device-command-handler").toString()).append("\n");				
			}

			if (session.getAttribute("device-command-timeout-s") != null) {
				builder.append("device-command-timeout-s = ").append(session.getAttribute("device-command-timeout-s").toString()).append("\n");				
			}

			if (session.getAttribute("jvm-arguments") != null) {
				builder.append("jvm-arguments = ").append(session.getAttribute("jvm-arguments").toString()).append("\n");
			}

			//Device Stage configuration
			builder.append("device-stage-type = ").append(session.getAttribute("device-stage-type").toString()).append("\n");

			if (session.getAttribute("device-upload-root") != null) {
				builder.append("device-upload-root = ").append(session.getAttribute("device-upload-root").toString()).append("\n");
			}
			if (session.getAttribute("device-command-root") != null) {
				builder.append("device-command-root = ").append(session.getAttribute("device-command-root").toString()).append("\n");
			}
			
			if (request.getSession().getAttribute("stage-sftp-host") != null) {
				builder.append("stage-sftp-host = ").append(session.getAttribute("stage-sftp-host").toString()).append("\n");
			}
			
			if (request.getSession().getAttribute("stage-sftp-port") != null) {
				builder.append("stage-sftp-port = ").append(session.getAttribute("stage-sftp-port").toString()).append("\n");
			}
			
			if (request.getSession().getAttribute("stage-sftp-user") != null) {
				builder.append("stage-sftp-user = ").append(session.getAttribute("stage-sftp-user").toString()).append("\n");
			}
			
			if (request.getSession().getAttribute("stage-sftp-password") != null) {
				builder.append("stage-sftp-password = ").append(session.getAttribute("stage-sftp-password").toString()).append("\n");
			}

			if (request.getSession().getAttribute("stage-sftp-data-directory") != null) {
				builder.append("stage-sftp-data-directory = ").append(session.getAttribute("stage-sftp-data-directory").toString()).append("\n");
			}

			if (request.getSession().getAttribute("stage-sftp-command-directory") != null) {
				builder.append("stage-sftp-command-directory = ").append(session.getAttribute("stage-sftp-command-directory").toString()).append("\n");
			}

			if (session.getAttribute("stage-minio-endpoint") != null) {
				builder.append("stage-minio-endpoint = ").append(session.getAttribute("stage-minio-endpoint").toString()).append("\n");
			}
			if (session.getAttribute("stage-minio-data-bucket-name") != null) {
				builder.append("stage-minio-data-bucket-name = ").append(session.getAttribute("stage-minio-data-bucket-name").toString()).append("\n");
			}		
			if (session.getAttribute("stage-minio-command-bucket-name") != null) {
				builder.append("stage-minio-command-bucket-name = ").append(session.getAttribute("stage-minio-command-bucket-name").toString()).append("\n");
			}		
			if (session.getAttribute("stage-minio-access-key") != null) {
				builder.append("stage-minio-access-key = ").append(session.getAttribute("stage-minio-access-key").toString()).append("\n");
			}
			if (session.getAttribute("stage-minio-secret-key") != null) {
				builder.append("stage-minio-secret-key = ").append(session.getAttribute("stage-minio-secret-key").toString()).append("\n");
			}			
			if (session.getAttribute("stage-s3-endpoint") != null) {
				builder.append("stage-s3-endpoint = ").append(session.getAttribute("stage-s3-endpoint").toString()).append("\n");
			}
			if (session.getAttribute("stage-s3-data-bucket-name") != null) {
				builder.append("stage-s3-data-bucket-name = ").append(session.getAttribute("stage-s3-data-bucket-name").toString()).append("\n");
			}		
			if (session.getAttribute("stage-s3-command-bucket-name") != null) {
				builder.append("stage-s3-command-bucket-name = ").append(session.getAttribute("stage-s3-command-bucket-name").toString()).append("\n");
			}
			if (session.getAttribute("stage-s3-access-key") != null) {
				builder.append("stage-s3-access-key = ").append(session.getAttribute("stage-s3-access-key").toString()).append("\n");
			}
			if (session.getAttribute("stage-s3-secret-key") != null) {
				builder.append("stage-s3-secret-key = ").append(session.getAttribute("stage-s3-secret-key").toString()).append("\n");
			}
			if (session.getAttribute("stage-kafka-producer-properties-file") != null) {
				builder.append("stage-kafka-producer-properties-file = ").append(session.getAttribute("stage-kafka-producer-properties-file").toString()).append("\n");
			}
			if (session.getAttribute("stage-kafka-consumer-properties-file") != null) {
				builder.append("stage-kafka-consumer-properties-file = ").append(session.getAttribute("stage-kafka-consumer-properties-file").toString()).append("\n");
			}
			if (session.getAttribute("device-scanner-interval-s") != null) {
				builder.append("device-scanner-interval-s = ").append(session.getAttribute("device-scanner-interval-s").toString()).append("\n");
			}

			if (session.getAttribute("throttle-stage-request-rate") != null) {
				builder.append("throttle-stage-request-rate = ").append(session.getAttribute("throttle-stage-request-rate").toString()).append("\n");
			}

			if (session.getAttribute("max-stage-requests-per-minute") != null) {
				builder.append("max-stage-requests-per-minute = ").append(session.getAttribute("max-stage-requests-per-minute").toString()).append("\n");
			}

			if (session.getAttribute("device-encryption-enabled") != null) {
				builder.append("device-encryption-enabled = ").append(session.getAttribute("device-encryption-enabled").toString()).append("\n");				
			}		

			if (session.getAttribute("device-decryption-key-file") != null) {
				builder.append("device-decryption-key-file = ").append(session.getAttribute("device-decryption-key-file").toString()).append("\n");
			}

			if (session.getAttribute("device-scheduler-type") != null) {
				builder.append("device-scheduler-type = ").append(session.getAttribute("device-scheduler-type").toString()).append("\n");
			}

			if (session.getAttribute("device-polling-interval-ms") != null) {
				builder.append("device-polling-interval-ms = ").append(session.getAttribute("device-polling-interval-ms").toString()).append("\n");
			}

			if (session.getAttribute("stage-oper-retry-count") != null) {
				builder.append("stage-oper-retry-count = ").append(session.getAttribute("stage-oper-retry-count").toString()).append("\n");
			}
			if (session.getAttribute("stage-oper-retry-interval-ms") != null) {
				builder.append("stage-oper-retry-interval-ms = ").append(session.getAttribute("stage-oper-retry-interval-ms").toString()).append("\n");
			}		

			//Monitoring Configuration
			builder.append("gui-dashboard = ").append("true").append("\n");			
			if (session.getAttribute("update-statistics-interval-s") != null) {
				builder.append("update-statistics-interval-s = ").append(session.getAttribute("update-statistics-interval-s").toString()).append("\n");
			}

			if (session.getAttribute("enable-prometheus-statistics-publisher") != null) {
				builder.append("enable-prometheus-statistics-publisher = ").append(session.getAttribute("enable-prometheus-statistics-publisher").toString()).append("\n");
			}

			if (session.getAttribute("prometheus-push-gateway-url") != null) {
				builder.append("prometheus-push-gateway-url = ").append(session.getAttribute("prometheus-push-gateway-url").toString()).append("\n");
			}

			if (session.getAttribute("prometheus-statistics-publisher-interval-s") != null) {
				builder.append("prometheus-statistics-publisher-interval-s = ").append(session.getAttribute("prometheus-statistics-publisher-interval-s").toString()).append("\n");
			}


			//DB Configuration

			builder.append("num-destinations = ").append(session.getAttribute("num-destinations").toString()).append("\n");
			Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());


			for (Integer dstIndex = 1 ; dstIndex <= numDestinations ; ++dstIndex) {
				builder.append("dst-type-" + dstIndex + " = ").append(session.getAttribute("dst-type-" + dstIndex).toString()).append("\n");

				if (session.getAttribute("dst-connection-string-" + dstIndex) != null) {
					builder.append("dst-connection-string-" + dstIndex + " = ").append(session.getAttribute("dst-connection-string-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex) != null) {
					builder.append("dst-postgresql-vector-extension-enabled-" + dstIndex + " = ").append(session.getAttribute("dst-postgresql-vector-extension-enabled-" + dstIndex).toString()).append("\n");
				}
				
				if (session.getAttribute("dst-user-" + dstIndex) != null) {
					builder.append("dst-user-" + dstIndex + " = ").append(session.getAttribute("dst-user-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-password-" + dstIndex) != null) {			
					builder.append("dst-password-" + dstIndex + " = ").append(session.getAttribute("dst-password-" + dstIndex).toString()).append("\n");
				}			

				if (session.getAttribute("dst-database-" + dstIndex) != null) {
					builder.append("dst-database-" + dstIndex + " = ").append(session.getAttribute("dst-database-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-schema-" + dstIndex) != null) {			
					builder.append("dst-schema-" + dstIndex + " = ").append(session.getAttribute("dst-schema-" + dstIndex).toString()).append("\n");
				}			

				if (session.getAttribute("dst-connection-timeout-s-" + dstIndex) != null) {			
					builder.append("dst-connection-timeout-s-" + dstIndex + " = ").append(session.getAttribute("dst-connection-timeout-s-" + dstIndex).toString()).append("\n");
				}			
				
				if (session.getAttribute("dst-duckdb-reader-port-" + dstIndex) != null) {
					builder.append("dst-duckdb-reader-port-" + dstIndex + " = ").append(session.getAttribute("dst-duckdb-reader-port-" + dstIndex).toString()).append("\n");
				}
				
				if (session.getAttribute("dst-device-schema-name-policy-" + dstIndex) != null) {
                    builder.append("dst-device-schema-name-policy-" + dstIndex + " = ").append(session.getAttribute("dst-device-schema-name-policy-" + dstIndex).toString()).append("\n");
                }
				
				if (session.getAttribute("dst-type-" + dstIndex).toString().equals("DATALAKE")) {
					if (session.getAttribute("dst-data-lake-data-format-" + dstIndex) != null) {
						builder.append("dst-data-lake-data-format-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-data-format-" + dstIndex).toString()).append("\n");
					}

					if (session.getAttribute("dst-data-lake-object-switch-interval-" + dstIndex) != null) {
						builder.append("dst-data-lake-object-switch-interval-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-object-switch-interval-" + dstIndex).toString()).append("\n");
					}

					if (session.getAttribute("dst-data-lake-object-switch-interval-unit-" + dstIndex) != null) {			
						builder.append("dst-data-lake-object-switch-interval-unit-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-object-switch-interval-unit-" + dstIndex).toString().toUpperCase()).append("\n");
					}

					if (session.getAttribute("dst-data-lake-local-storage-dir-" + dstIndex) != null) {
						builder.append("dst-data-lake-local-storage-dir-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-local-storage-dir-" + dstIndex).toString()).append("\n");
					}

					if (session.getAttribute("dst-data-lake-publishing-" + dstIndex) != null) {
						builder.append("dst-data-lake-publishing-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-publishing-" + dstIndex).toString()).append("\n");					

						if (session.getAttribute("dst-data-lake-publishing-" + dstIndex).toString().equals("true")) {
							if (session.getAttribute("dst-data-lake-type-" + dstIndex) != null) {
								builder.append("dst-data-lake-type-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-type-" + dstIndex).toString()).append("\n");				

								if (session.getAttribute("dst-data-lake-type-" + dstIndex).toString().equals("S3")) {
									if (session.getAttribute("dst-data-lake-s3-endpoint-" + dstIndex) != null) {
										builder.append("dst-data-lake-s3-endpoint-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-s3-endpoint-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-s3-bucket-name-" + dstIndex) != null) {
										builder.append("dst-data-lake-s3-bucket-name-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-s3-bucket-name-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-s3-access-key-" + dstIndex) != null) {
										builder.append("dst-data-lake-s3-access-key-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-s3-access-key-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-s3-secret-key-" + dstIndex) != null) {
										builder.append("dst-data-lake-s3-secret-key-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-s3-secret-key-" + dstIndex).toString()).append("\n");					
									}
								} else if (session.getAttribute("dst-data-lake-type-" + dstIndex).toString().equals("MINIO")) {
									if (session.getAttribute("dst-data-lake-minio-endpoint-" + dstIndex) != null) {
										builder.append("dst-data-lake-minio-endpoint-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-minio-endpoint-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-minio-bucket-name-" + dstIndex) != null) {
										builder.append("dst-data-lake-minio-bucket-name-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-minio-bucket-name-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-minio-access-key-" + dstIndex) != null) {
										builder.append("dst-data-lake-minio-access-key-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-minio-access-key-" + dstIndex).toString()).append("\n");					
									}

									if (session.getAttribute("dst-data-lake-minio-secret-key-" + dstIndex) != null) {
										builder.append("dst-data-lake-minio-secret-key-" + dstIndex + " = ").append(session.getAttribute("dst-data-lake-minio-secret-key-" + dstIndex).toString()).append("\n");					
									}
								}
							}
						}
					}
				} else if (session.getAttribute("dst-type-" + dstIndex).toString().equals("CSV")) {

					if (session.getAttribute("dst-file-storage-type-" + dstIndex) != null) {
						builder.append("dst-file-storage-type-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-type-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-local-fs-directory-" + dstIndex) != null) {
						builder.append("dst-file-storage-local-fs-directory-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-local-fs-directory-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-sftp-host-" + dstIndex) != null) {
						builder.append("dst-file-storage-sftp-host-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-sftp-host-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-sftp-port-" + dstIndex) != null) {
						builder.append("dst-file-storage-sftp-port-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-sftp-port-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-sftp-directory-" + dstIndex) != null) {
						builder.append("dst-file-storage-sftp-directory-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-sftp-directory-" + dstIndex).toString()).append("\n");					
					}
					
					if (session.getAttribute("dst-file-storage-sftp-user-" + dstIndex) != null) {
						builder.append("dst-file-storage-sftp-user-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-sftp-user-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-sftp-password-" + dstIndex) != null) {
						builder.append("dst-file-storage-sftp-password-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-sftp-password-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-s3-url-" + dstIndex) != null) {
						builder.append("dst-file-storage-s3-url-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-s3-url-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-s3-bucket-name-" + dstIndex) != null) {
						builder.append("dst-file-storage-s3-bucket-name-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-s3-bucket-name-" + dstIndex).toString()).append("\n");					
					}
					
					if (session.getAttribute("dst-file-storage-s3-access-key-" + dstIndex) != null) {
						builder.append("dst-file-storage-s3-access-key-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-s3-access-key-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-s3-secret-key-" + dstIndex) != null) {
						builder.append("dst-file-storage-s3-secret-key-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-s3-secret-key-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-minio-url-" + dstIndex) != null) {
						builder.append("dst-file-storage-minio-url-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-minio-url-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-minio-bucket-name-" + dstIndex) != null) {
						builder.append("dst-file-storage-minio-bucket-name-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-minio-bucket-name-" + dstIndex).toString()).append("\n");					
					}
					
					if (session.getAttribute("dst-file-storage-minio-access-key-" + dstIndex) != null) {
						builder.append("dst-file-storage-minio-access-key-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-minio-access-key-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-file-storage-minio-secret-key-" + dstIndex) != null) {
						builder.append("dst-file-storage-minio-secret-key-" + dstIndex + " = ").append(session.getAttribute("dst-file-storage-minio-secret-key-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-with-headers-" + dstIndex) != null) {
						builder.append("dst-csv-files-with-headers-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-with-headers-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-field-delimiter-" + dstIndex) != null) {
						builder.append("dst-csv-files-field-delimiter-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-field-delimiter-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-record-delimiter-" + dstIndex) != null) {
						builder.append("dst-csv-files-record-delimiter-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-record-delimiter-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-escape-character-" + dstIndex) != null) {
						builder.append("dst-csv-files-escape-character-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-escape-character-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-quote-character-" + dstIndex) != null) {
						builder.append("dst-csv-files-quote-character-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-quote-character-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-csv-files-null-string-" + dstIndex) != null) {
						builder.append("dst-csv-files-null-string-" + dstIndex + " = ").append(session.getAttribute("dst-csv-files-null-string-" + dstIndex).toString()).append("\n");					
					}
				} else if (session.getAttribute("dst-type-" + dstIndex).toString().equals("DATABRICKS_SQL")) {
					if (session.getAttribute("dst-databricks-dbfs-endpoint-" + dstIndex) != null) {
						builder.append("dst-databricks-dbfs-endpoint-" + dstIndex + " = ").append(session.getAttribute("dst-databricks-dbfs-endpoint-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-databricks-dbfs-access-token-" + dstIndex) != null) {
						builder.append("dst-databricks-dbfs-access-token-" + dstIndex + " = ").append(session.getAttribute("dst-databricks-dbfs-access-token-" + dstIndex).toString()).append("\n");					
					}

					if (session.getAttribute("dst-databricks-dbfs-basepath-" + dstIndex) != null) {
						builder.append("dst-databricks-dbfs-basepath-" + dstIndex + " = ").append(session.getAttribute("dst-databricks-dbfs-basepath-" + dstIndex).toString()).append("\n");					
					}
				}
			
				if (session.getAttribute("dst-alias-" + dstIndex) != null) {			
					builder.append("dst-alias-" + dstIndex + " = ").append(session.getAttribute("dst-alias-" + dstIndex).toString()).append("\n");
				}			

				builder.append("dst-data-type-mapping-" + dstIndex  + " = ").append(session.getAttribute("dst-data-type-mapping-" + dstIndex).toString()).append("\n");

				if (session.getAttribute("all-mapping-props-" + dstIndex) != null) {
					HashMap<String, String> allMappingProps = (HashMap<String, String>) session.getAttribute("all-mapping-props-" + dstIndex);				
					for (Map.Entry<String, String> entry :  allMappingProps.entrySet()) {
						builder.append(entry.getKey() + " = ").append(entry.getValue()).append("\n");
					}
				}

				if (session.getAttribute("dst-enable-filter-mapper-rules-" + dstIndex) != null) {
					builder.append("dst-enable-filter-mapper-rules-" + dstIndex  + " = ").append(session.getAttribute("dst-enable-filter-mapper-rules-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-allow-unspecified-tables-" + dstIndex) != null) {
					builder.append("dst-allow-unspecified-tables-" + dstIndex  + " = ").append(session.getAttribute("dst-allow-unspecified-tables-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-allow-unspecified-columns-" + dstIndex) != null) {
					builder.append("dst-allow-unspecified-columns-" + dstIndex  + " = ").append(session.getAttribute("dst-allow-unspecified-columns-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-filter-mapper-rules-file-" + dstIndex) != null) {
					builder.append("dst-filter-mapper-rules-file-" + dstIndex  + " = ").append(session.getAttribute("dst-filter-mapper-rules-file-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-enable-value-mapper-" + dstIndex) != null) {
					builder.append("dst-enable-value-mapper-" + dstIndex  + " = ").append(session.getAttribute("dst-enable-value-mapper-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-value-mappings-file-" + dstIndex) != null) {
					builder.append("dst-value-mappings-file-" + dstIndex  + " = ").append(session.getAttribute("dst-value-mappings-file-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-enable-triggers-" + dstIndex) != null) {
					builder.append("dst-enable-triggers-" + dstIndex  + " = ").append(session.getAttribute("dst-enable-triggers-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-triggers-file-" + dstIndex) != null) {
					builder.append("dst-triggers-file-" + dstIndex  + " = ").append(session.getAttribute("dst-triggers-file-" + dstIndex).toString()).append("\n");
				}

				builder.append("dst-insert-batch-size-" + dstIndex + " = ").append(session.getAttribute("dst-insert-batch-size-" + dstIndex).toString()).append("\n");
				builder.append("dst-update-batch-size-" + dstIndex + " = ").append(session.getAttribute("dst-update-batch-size-" + dstIndex).toString()).append("\n");
				builder.append("dst-delete-batch-size-" + dstIndex + " = ").append(session.getAttribute("dst-delete-batch-size-" + dstIndex).toString()).append("\n");
				builder.append("dst-object-init-mode-" + dstIndex + " = ").append(session.getAttribute("dst-object-init-mode-" + dstIndex).toString()).append("\n");

				builder.append("dst-txn-retry-count-" + dstIndex + " = ").append(session.getAttribute("dst-txn-retry-count-" + dstIndex).toString()).append("\n");
				builder.append("dst-txn-retry-interval-ms-" + dstIndex + " = ").append(session.getAttribute("dst-txn-retry-interval-ms-" + dstIndex).toString()).append("\n");
				builder.append("dst-idempotent-data-ingestion-" + dstIndex + " = ").append(session.getAttribute("dst-idempotent-data-ingestion-" + dstIndex).toString()).append("\n");
				builder.append("dst-idempotent-data-ingestion-method-" + dstIndex + " = ").append(session.getAttribute("dst-idempotent-data-ingestion-method-" + dstIndex).toString()).append("\n");
				builder.append("dst-disable-metadata-table-" + dstIndex + " = ").append(session.getAttribute("dst-disable-metadata-table-" + dstIndex).toString()).append("\n");
				builder.append("dst-skip-failed-log-files-" + dstIndex + " = ").append(session.getAttribute("dst-skip-failed-log-files-" + dstIndex).toString()).append("\n");
				builder.append("dst-set-unparsable-values-to-null-" + dstIndex + " = ").append(session.getAttribute("dst-set-unparsable-values-to-null-" + dstIndex).toString()).append("\n");
				builder.append("dst-quote-object-names-" + dstIndex + " = ").append(session.getAttribute("dst-quote-object-names-" + dstIndex).toString()).append("\n");
				builder.append("dst-quote-column-names-" + dstIndex + " = ").append(session.getAttribute("dst-quote-column-names-" + dstIndex).toString()).append("\n");
				builder.append("dst-use-catalog-scope-resolution-" + dstIndex + " = ").append(session.getAttribute("dst-use-catalog-scope-resolution-" + dstIndex).toString()).append("\n");
				builder.append("dst-use-schema-scope-resolution-" + dstIndex + " = ").append(session.getAttribute("dst-use-schema-scope-resolution-" + dstIndex).toString()).append("\n");
				
				if (session.getAttribute("dst-clickhouse-engine-" + dstIndex) != null) {
					builder.append("dst-clickhouse-engine-" + dstIndex + " = ").append(session.getAttribute("dst-clickhouse-engine-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-mongodb-use-transactions-" + dstIndex) != null) {
					builder.append("dst-mongodb-use-transactions-" + dstIndex + " = ").append(session.getAttribute("dst-mongodb-use-transactions-" + dstIndex).toString()).append("\n");
				}
				if (session.getAttribute("dst-create-table-suffix-" + dstIndex) != null) {
					builder.append("dst-create-table-suffix-" + dstIndex + " = ").append(session.getAttribute("dst-create-table-suffix-" + dstIndex).toString()).append("\n");
				}

				if (session.getAttribute("dst-spark-configuration-file-" + dstIndex) != null) {
					builder.append("dst-spark-configuration-file-" + dstIndex + " = ").append(session.getAttribute("dst-spark-configuration-file-" + dstIndex).toString()).append("\n");
				}
			}

			//Map Devices Configs			
			if (numDestinations > 1) {
				builder.append("map-devices-to-dst-pattern-type = " + session.getAttribute("map-devices-to-dst-pattern-type")).append("\n");			
				for (int dstIndex = 1 ; dstIndex <= numDestinations; ++dstIndex) {
					builder.append("map-devices-to-dst-pattern-" + dstIndex + " = " + session.getAttribute("map-devices-to-dst-pattern-" + dstIndex)).append("\n");
				}
				builder.append("default-dst-index-for-unmapped-devices = " + session.getAttribute("default-dst-index-for-unmapped-devices")).append("\n");
			}


			try {
				Files.writeString(Path.of(propsPath), builder.toString());
			} catch (IOException e) {
				throw new ServletException("Failed to write SyncLite consolidator configurations into file : " + propsPath, e);
			}

			/*
			FileWriter propsWriter = null;
			try {
				propsWriter = new FileWriter(propsPath);
				propsWriter.write(builder.toString());
				propsWriter.close();
			} catch (Exception e) {
				if (propsWriter != null) {
					propsWriter.close();
				}
				throw e;
			}
			 */

			//Get current job PID if running
			long currentJobPID = 0;
			Process jpsProc;
			if (isWindows()) {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "\\bin\\jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			} else {
				String javaHome = System.getenv("JAVA_HOME");			
				String scriptPath = "jps";
				if (javaHome != null) {
					scriptPath = javaHome + "/bin/jps";
				} else {
					scriptPath = "jps";
				}
				String[] cmdArray = {scriptPath, "-l", "-m"};
				jpsProc = Runtime.getRuntime().exec(cmdArray);
			}
			BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
			String line = stdout.readLine();
			while (line != null) {
				if (line.contains("com.synclite.consolidator.Main") && line.contains(deviceDataRoot)) {
					currentJobPID = Long.valueOf(line.split(" ")[0]);
				}
				line = stdout.readLine();
			}
			//stdout.close();

			//Kill job if found

			if(currentJobPID > 0) {
				if (isWindows()) {
					Runtime.getRuntime().exec("taskkill /F /PID " + currentJobPID);
				} else {
					Runtime.getRuntime().exec("kill -9 " + currentJobPID);
				}
			}

			//Get env variable 
			String jvmArgs = "";
			if (session.getAttribute("jvm-arguments") != null) {
				jvmArgs = session.getAttribute("jvm-arguments").toString();
			}
			//Start job again
			Process p;
			if (isWindows()) {
				String scriptName = "synclite-consolidator.bat";
				String scriptPath = Path.of(corePath, scriptName).toString();
				if (!jvmArgs.isBlank()) {
					try {
						//Delete and re-create a file variables.bat under scriptPath and set the variable JVM_ARGS
						Path varFilePath = Path.of(corePath, "synclite-variables.bat");
						if (Files.exists(varFilePath)) {
							Files.delete(varFilePath);
						}
						String varString = "set \"JVM_ARGS=" + jvmArgs + "\""; 
						Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
					} catch (Exception e) {
						throw new ServletException("Failed to write jvm-arguments to variables.bat file", e);
					}
				}
				String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
				p = Runtime.getRuntime().exec(cmdArray);

			} else {				
				String scriptName = "synclite-consolidator.sh";
				Path scriptPath = Path.of(corePath, scriptName);

				if (!jvmArgs.isBlank()) {
					try {
						//Delete and re-create a file variables.sh under scriptPath and set the variable JVM_ARGS
						Path varFilePath = Path.of(corePath, "synclite-variables.sh");
						String varString = "JVM_ARGS=\"" + jvmArgs + "\"";
						if (Files.exists(varFilePath)) {
							Files.delete(varFilePath);
						}
						Files.writeString(varFilePath, varString, StandardOpenOption.CREATE);
						Set<PosixFilePermission> perms = Files.getPosixFilePermissions(varFilePath);
						perms.add(PosixFilePermission.OWNER_EXECUTE);
						Files.setPosixFilePermissions(varFilePath, perms);
					} catch (Exception e) {
						throw new ServletException("Failed to write jvm-arguments to variables.sh file", e);
					}
				}

				// Get the current set of script permissions
				Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptPath);
				// Add the execute permission if it is not already set
				if (!perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
					perms.add(PosixFilePermission.OWNER_EXECUTE);
					Files.setPosixFilePermissions(scriptPath, perms);
				}

				String[] cmdArray = {scriptPath.toString(), "sync", "--work-dir", deviceDataRoot, "--config", propsPath};
				p = Runtime.getRuntime().exec(cmdArray);		        	
			}
			//int exitCode = p.exitValue();
			//Thread.sleep(3000);
			Thread.sleep(5000);
			boolean processStatus = p.isAlive();
			if (!processStatus) {
				BufferedReader procErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				if ((line = procErr.readLine()) != null) {
					StringBuilder errorMsg = new StringBuilder();
					int i = 0;
					do {
						errorMsg.append(line);
						errorMsg.append("\n");
						line = procErr.readLine();
						if (line == null) {
							break;
						}
						++i;
					} while (i < 5);
					throw new ServletException("Failed to start consolidator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
				}

				BufferedReader procOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
				if ((line = procOut.readLine()) != null) {
					StringBuilder errorMsg = new StringBuilder();
					int i = 0;
					do {
						errorMsg.append(line);
						errorMsg.append("\n");
						line = procOut.readLine();
						if (line == null) {
							break;
						}
						++i;
					} while (i < 5);
					throw new ServletException("Failed to start consolidator job with exit code : " + p.exitValue() + " and errors : " + errorMsg.toString());
				}

				throw new ServletException("Failed to start consolidator job with exit value : " + p.exitValue());
			}
			//System.out.println("process status " + processStatus);
			//System.out.println("process output line " + line);
			request.getSession().setAttribute("job-status","STARTED");
			request.getSession().setAttribute("job-type","SYNC");
			request.getSession().setAttribute("job-start-time",System.currentTimeMillis());

			//request.getRequestDispatcher("dashboard.jsp").forward(request, response);
			response.sendRedirect("dashboard.jsp");
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = "Error : " + e.getMessage();
			this.globalTracer.error("Failed to save job configuration with exception : " + e.getCause(), e);
			request.getRequestDispatcher("jobSummary.jsp?saveStatus=FAIL&saveStatusDetails=" + errorMsg).forward(request, response);
		}
	}

	private boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
	
	private final void initTracer(Path workDir) {
		this.globalTracer = Logger.getLogger(ValidateJobConfiguration.class);
		if (this.globalTracer.getAppender("ConsolidatorTracer") == null) {
			globalTracer.setLevel(Level.INFO);
			RollingFileAppender fa = new RollingFileAppender();
			fa.setName("consolidatorTracer");
			fa.setFile(workDir.resolve("synclite_consolidator.trace").toString());
			fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
			fa.setMaxBackupIndex(10);
			fa.setAppend(true);
			fa.activateOptions();
			globalTracer.addAppender(fa);
		}
	}

}
