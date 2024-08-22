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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLitePropsException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.SyncLiteLoggerInfo;

public class KafkaStageManager extends DeviceStageManager {

	private ConcurrentHashMap<String, KafkaConsumer<String, Map>> kafkaConsumers = new ConcurrentHashMap<String, KafkaConsumer<String,Map>>(ConfLoader.getInstance().getNumDeviceProcessors()); 
	private ConcurrentHashMap<String, KafkaProducer<String, String>> kafkaProducers = new ConcurrentHashMap<String, KafkaProducer<String,String>>(ConfLoader.getInstance().getNumDeviceProcessors()); 
	private HashMap<String, String> kafkaConsumerProperties;
	private HashMap<String, String> kafkaProducerProperties;
	private AdminClient adminClient;
	
	@Override
	@Deprecated
	public boolean objectExists(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {		
		Path containerPath = objectPath.getParent();
		fetchNextObject(containerPath, objType, "");

		if (Files.exists(objectPath)) {
			return true;
		}
		return false;
	}

	@Override
	public long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				Path containerPath = objectPath.getParent();
				fetchNextObject(containerPath, objType, "");
				if (Files.exists(objectPath)) {
					if (!Files.isSameFile(objectPath, outputFile)) {
						Files.copy(objectPath, outputFile, StandardCopyOption.REPLACE_EXISTING);
					}
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
				Path containerPath = objectPath.getParent();
				fetchNextObject(containerPath, objType, ".enc");
				Path encryptedObjPath = Path.of(objectPath + ".enc");
				if (Files.exists(encryptedObjPath)) {
					try (InputStream fis = new FileInputStream(encryptedObjPath.toString())) {
						fileDownloader.decryptAndWriteFile(null, outputFile);
					}					
					try {
						Files.delete(encryptedObjPath);
					} catch (IOException e) {
						throw new IOException("Failed to delete encrypted object after decryption : " + encryptedObjPath, e);
					}
					BasicFileAttributes fileAttrs = Files.readAttributes(encryptedObjPath, BasicFileAttributes.class);
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
	public void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//Delete all downloaded parts of this object if any. 
				String partFilePrefix = objectPath.getFileName() + ".part.";
				List<Path> partFiles = Files.walk(objectPath.getParent()).filter(s->s.toString().startsWith(partFilePrefix)).collect(Collectors.toList());
				
				for (Path f : partFiles) {
					if (Files.exists(f)) {
						Files.delete(f);
					}
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
		fetchNextObject(container, objType, "");
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
		fetchNextObject(container, objType, "");
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
	public boolean containerExists(Path containerPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		List<Path> containers = listContainers(containerPath, objType);
		if (containers.contains(containerPath)) {
			return true;
		}
		return false;
	}

	@Override
	public List<Path> listContainers(Path startFrom, SyncLiteObjectType objType) throws SyncLiteStageException {	
		List<Path> deviceUploadRoots = new ArrayList<Path>(); 
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				//TODO create an adminClient beforehand ?
				ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
			    listTopicsOptions.listInternal(true);
			    
				ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
				Set<String> topics = result.names().get();
				for (String topicName : topics) {
					switch(objType) {
					case DATA:
					case METADATA:
						if (topicName.startsWith(SyncLiteLoggerInfo.getDeviceDataRootPrefix()) && topicName.endsWith("-data")) {
							deviceUploadRoots.add(Path.of(startFrom.getFileName().toString(), topicName.substring(0,topicName.lastIndexOf("-"))));
						}
						break;
					case LOG:
						if (topicName.startsWith(SyncLiteLoggerInfo.getDeviceDataRootPrefix()) && topicName.endsWith("-log")) {
							deviceUploadRoots.add(Path.of(startFrom.getFileName().toString(), topicName.substring(0,topicName.lastIndexOf("-"))));
						}
						break;
					case COMMAND:
						if (topicName.startsWith(SyncLiteLoggerInfo.getDeviceDataRootPrefix()) && topicName.endsWith("-command")) {
							deviceUploadRoots.add(Path.of(startFrom.getFileName().toString(), topicName.substring(0,topicName.lastIndexOf("-"))));
						}
						break;
					}						
				}		
				return deviceUploadRoots;
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
		return deviceUploadRoots;
	}

	private HashMap<String, Object> getKafkaConsumerPropertiesForAdminClient() {
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.putAll(kafkaConsumerProperties);
		return props;
	}


	@Override
	public void initializeDataStage(Logger globalTracer) throws SyncLiteStageException {
		this.tracer = globalTracer;
		try {
			loadKafkaProperties();
		} catch (SyncLitePropsException e) {
			throw new SyncLiteStageException("Failed to load kafka consumer properties : ", e);
		}		
		//create admin client and try connecting
		try {
			adminClient = KafkaAdminClient.create(getKafkaConsumerPropertiesForAdminClient());
			ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
			ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
			result.names().get();
		} catch (Exception e) {
			throw new SyncLiteStageException("Device stage unreachable. Failed to create kafka admin client/connect to kafka broker : ", e);
		}
	}
	
	@Override
	public void initializeCommandStage(Logger globalTracer) throws SyncLiteStageException {
		this.tracer = globalTracer;
		try {
			loadKafkaProperties();
		} catch (SyncLitePropsException e) {
			throw new SyncLiteStageException("Failed to load kafka consumer properties : ", e);
		}		
		//create admin client and try connecting
		try {
			adminClient = KafkaAdminClient.create(getKafkaConsumerPropertiesForAdminClient());
			ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
			ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
			result.names().get();
		} catch (Exception e) {
			throw new SyncLiteStageException("Device stage unreachable. Failed to create kafka admin client/connect to kafka broker : ", e);
		}
	}


	private void fetchNextObject(Path container, SyncLiteObjectType objType, String objNameSuffix) throws SyncLiteStageException {
		//Stream all files from respective kafka topics into specified destination

		//Kafka consumers are not thread safe, hence we use one consumer per device processing thread.
		//Check and create consumer for current thread

		try {
			String topicName = getSyncLiteTopicName(container, objType);
			KafkaConsumer<String, Map> consumer  = kafkaConsumers.computeIfAbsent(topicName, s -> { 
				KafkaConsumer<String, Map> c = new KafkaConsumer<String, Map>(getKafkaConsumerProperties(topicName));
				c.subscribe(Collections.singletonList(topicName));
				return c;
			});			

			//boolean processedMsg = false;
//			while (true) {
			ConsumerRecords<String, Map> messages = consumer.poll(100);
			for (ConsumerRecord<String, Map> message : messages) {								
				for (Map.Entry<String, byte[]> entry : ((Map<String,byte[]>) message.value()).entrySet()) {
					String fileName = entry.getKey();
					Path filePath = Path.of(container.toString(), fileName);
					byte[] messageContent = entry.getValue();
					if (messageContent.length == 5) {
						String msg = new String(messageContent, Charset.defaultCharset());
						if(msg.startsWith("MERGE")) {
							String[] msgTokens = msg.split(":");
							long numParts = 1;
							try {
								numParts = Long.valueOf(msgTokens[1]);
							} catch (IllegalArgumentException e) {
								//Ignore
							}
							mergeParts(numParts, filePath, objNameSuffix);
						}
					} else {
						
						if (Files.exists(filePath)) {
							Files.delete(filePath);
						}						
						Files.createFile(filePath);
						Files.write(filePath, messageContent, StandardOpenOption.WRITE);
					}
				}
				//processedMsg = true;
			}
			consumer.commitSync();
			//if (processedMsg) {
			//	break;
			//}
	//		}
		} catch (Exception e) {
			throw new SyncLiteStageException("Exception while prefetching objects under specified container : " + container + " into destination :" + container , e);
		}
		
	}

	private final void mergeParts(long numParts, Path filePath, String objNameSuffix) throws SyncLiteStageException {
		try {
			Path outFilePath = Path.of(filePath.toString(), objNameSuffix);
			if (Files.exists(filePath)) {
				Files.delete(filePath);
			}	
			Files.createFile(filePath);
			for (long i=1; i <= numParts; ++i) {
				Path partPath = Path.of(filePath.toString() + ".part." + i);
				Files.write(outFilePath, Files.readAllBytes(partPath), StandardOpenOption.APPEND);
			}
		} catch (IOException e) {
			throw new SyncLiteStageException("Failed to merge " + numParts + " of file " + filePath );
		}
	}

	private final String getSyncLiteTopicName(Path container, SyncLiteObjectType objType) {
		switch (objType) {
		case DATA:
		case DATA_CONTAINER:
		case METADATA:	
			return container.getFileName().toString() + "-data";
		case LOG:
			return container.getFileName().toString() + "-log";
		case COMMAND:
		case COMMAND_CONTAINER:	
			return container.getFileName().toString() + "-command";
		}
		return null;
	}

	private final Map<String, Object> getKafkaConsumerProperties(String topic) {
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.putAll(kafkaConsumerProperties);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SyncLiteFileDeserializer.class.getName());
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "synclite-consolidator");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		//props.put(ConsumerConfig.GROUP_ID_CONFIG, "synclite-consolidator");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return props;
	}

	private final Map<String, Object> getKafkaProducerProperties(String topic) {
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.putAll(kafkaProducerProperties);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "synclite-consolidator");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		//props.put(ConsumerConfig.GROUP_ID_CONFIG, "synclite-consolidator");
		return props;
	}


	private final void loadKafkaProperties() throws SyncLitePropsException {
		Path kafkaConsumerPropsFile = ConfLoader.getInstance().getStageKafkaConsumerPropertiesFile();		
		kafkaConsumerProperties = ConfLoader.loadPropertiesFromFile(kafkaConsumerPropsFile);
		
		if (ConfLoader.getInstance().getEnableDeviceCommandHandler()) {
			Path kafkaProducerPropsFile = ConfLoader.getInstance().getCommandKafkaProducerPropertiesFile();
			kafkaProducerProperties = ConfLoader.loadPropertiesFromFile(kafkaProducerPropsFile);
		} else {
			kafkaProducerProperties = null;
		}
		//kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	}

	@Override
	public void deleteContainer(Path container) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				String topicName = null;
				try {
					topicName = getSyncLiteTopicName(container, SyncLiteObjectType.DATA);
					adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
				} catch (ExecutionException e) {
		            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
		                tracer.error("Kafka topic " + topicName + " does not exist");
		            } else {
		                throw e;
		            }
				}

				try {
					topicName = getSyncLiteTopicName(container, SyncLiteObjectType.LOG);
					adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
				} catch (ExecutionException e) {
		            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
		                tracer.error("Kafka topic " + topicName + " does not exist");
		            } else {
		                throw e;
		            }
				}

				try {
					topicName = getSyncLiteTopicName(container, SyncLiteObjectType.COMMAND);
					adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
				} catch (ExecutionException e) {
		            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
		                tracer.error("Kafka topic " + topicName + " does not exist");
		            } else {
		                throw e;
		            }
				}

				return;
			} catch (Exception e) {
				tracer.error("Exception while deleting container from device stage : " + container.getFileName(), e);				
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
				String topicName = null;
				try {
					topicName = getSyncLiteTopicName(container, objType);
		    		List<NewTopic> newTopics = new ArrayList<NewTopic>(1);
					newTopics.add(new NewTopic(topicName, 1, (short) 1));
		    		CreateTopicsResult res = adminClient.createTopics(newTopics);
		    		res.all().get();
				} catch (ExecutionException e) {
		        	if (e.getMessage().contains("Topic") && e.getMessage().contains("already exists")) {
		        		return;
		        	} else {
		        		throw new Exception("Failed to create remote write archive with exception : ", e);
		        	}
				}
		        return;
			} catch (Exception e) {
				tracer.error("Exception while creating container in device stage : " + container.getFileName(), e);				
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
				String topicName = getSyncLiteTopicName(container, objType);

				KafkaProducer<String, String> producer = kafkaProducers.computeIfAbsent(topicName, s -> { 
					KafkaProducer<String, String> p = new KafkaProducer<String, String>(getKafkaProducerProperties(topicName));
					return p;
				});
				
				//upload object as <OBJECT_NAME> <OBJECT_CONTENT>
				
				String objectContent = Files.readString(objectPath);
				
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, objectPath.getFileName().toString() + " " + objectContent);
				producer.send(record).get();
				return;
			} catch (Exception e) {
				tracer.error("Exception while uploading object " + objectPath + " in device stage in container : " + container, e);				
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
