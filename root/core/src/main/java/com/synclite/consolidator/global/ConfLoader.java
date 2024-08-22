package com.synclite.consolidator.global;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.synclite.consolidator.device.DeviceIdentifier;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.exception.SyncLitePropsException;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.stage.DeviceStageType;

public class ConfLoader {
	//Job Configuration
	private Path licenseFilePath;
	private Integer numDeviceProcessors;
	private Integer failedDeviceRetryIntervalS;
	private Long deviceScannerIntervalS;
	private DeviceSchedulerType deviceSchedulerType;
	private Long devicePollingIntervalMs;
	private TraceLevel traceLevel;
	private Long deviceSnapshotIntervalS;
	private HashMap<String, String> properties;
	private Set<String> includeDeviceIDs;
	private Set<String> includeDeviceNames;
	private Set<String> excludeDeviceIDs;
	private Set<String> excludeDeviceNames;
	private Pattern includeDeviceIDPattern;
	private Pattern includeDeviceNamePattern;
	private Boolean enableReplicasForTelemetryDevices;
	private Boolean skipBadTxnFiles;
	private Boolean disableReplicasForAppenderDevices;
	private Long deviceCountLimit;
	private Long perDeviceOperationCountLimit;
	private Long perDeviceProcessedLogSizeLimit;
	private Boolean checkPerDeviceLimits;
	private Long totalProcessedLogSizeLimit;
	private Long totalOperationCountLimit;
	private Long totalTransactionCountLimit;
	private Boolean checkGlobalLimits;

	private ConsolidatorEdition edition;

	private Set<DstType> allowedDestinations;
	private Long syncLiteOperRetryCount;
	private Long syncLiteOperRetryIntervalMs;
	private DstSyncMode dstSyncMode;
	private SyncerRole syncerRole;
	private Path deviceCommandRoot;
	private Path deviceDataRoot;
	private Boolean enableRequestProcessor;
	private Integer requestProcessorPort;

	//Monitoring configuration
	private Boolean guiDashboard;
	private Long updateStatisticsIntervalS;
	private Boolean enablePrometheusStatisticsPublisher;
	private URL prometheusPushGatewayURL;
	private Long prometheusStatisticsPublisherIntervalS;

	//Stage configuration
	private DeviceStageType deviceStageType;
	private Path deviceUploadRoot;

	private String stageSFTPHost;
	private Integer stageSFTPPort;
	private String stageSFTPUser;
	private String stageSFTPPassword;
	private String stageSFTPDataDirectory;
	private String stageSFTPCommandDirectory;
	private String stageMinioEndPoint;
	private String stageMinioDataBucketName;
	private String stageMinioCommandBucketName;
	private String stageMinioAccessKey;
	private String stageMinioSecretKey;
	private String stageS3DataBucketName;
	private String stageS3CommandBucketName;
	private String stageS3EndPoint;
	private String stageS3AccessKey;
	private String stageS3SecretKey;
	private Boolean throttleStageRequestRate;
	private Long maxStageRequestsPerMinute; 
	private Path stageKafkaConsumerPropertiesFile;
	private Path stageKafkaProducerPropertiesFile;
	private Boolean deviceEncryptionEnabled;
	private Path deviceDecryptionKeyFile;    
	private Long failedStageOperRetryCount;
	private Long failedStageOperRetryIntervalMs;

	private Boolean enableDeviceCommandHandler;
	private Long deviceCommandTimeoutS;

	//Dst Configuration
	private Integer numDestinations;
	private DstType[] dstType;
	private String[] dstTypeName;
	private String[] dstDatabase;
	private String[] dstSchema;
	private DstDeviceSchemaNamePolicy[] dstDeviceSchemaNamePolicy;
	private String[] dstAlias;
	private String[] dstConnStr;
	private Long[] dstConnectionTimeoutS;
	private String[] dstUser;
	private String[] dstPassword;
	private Boolean[] dstPGVectorExtensionEnabled;
	private String[] dstSparkConfigurations;
	private Long[] dstInsertBatchSize;
	private Long[] dstUpdateBatchSize;
	private Long[] dstDeleteBatchSize;
	private DstObjectInitMode[] dstObjectInitMode;
	private DstDataTypeMapping[] dstDataTypeMapping;


	private Boolean[] dstEnableFilterMapperRules;
	private Boolean[] dstAllowUnspecifiedTables;
	private Boolean[] dstAllowUnspecifiedColumns;
	private HashMap<String, String>[] dstTableFilterMapperRules;
	private HashMap<String, String>[] dstTableToSrcTableMap;
	private HashMap<String, HashMap<String, String>>[] dstColumnFilterMapperRules;

	private Boolean[] dstEnableValueMapper;
	private HashMap<String, HashMap<String, HashMap<String, String>>>[] dstValueMappings;

	private Boolean[] dstEnableTriggers;
	private HashMap<String, List<String>>[] dstTriggers;

	private Boolean[] dstOperPredicateOpt;
	private Boolean[] dstIdempotentDataIngestion;
	private DstIdempotentDataIngestionMethod[] dstIdempotentDataIngestionMethod;
	private Boolean[] dstSkipFailedLogFiles;
	private Boolean[] dstSetUnparsableValuesToNull;
	private Boolean[] dstQuoteObjectNames;
	private Boolean[] dstQuoteColumnNames;
	private Boolean[] dstUseCatalogScopeResolution;
	private Boolean[] dstUseSchemaScopeResolution;
	private Boolean[] dstDisableMetadataTable;
	private String [] dstCreateTableSuffix;
	private String [] dstClickHouseEngine;
	private Boolean[] dstMongoDBUseTransactions;
	private DstDataLakeDataFormat[] dstDataLakeDataFormat;
	private Integer[] dstDataLakeObjectSwitchInterval;
	private ChronoUnit[] dstDataLakeObjectSwitchIntervalUnit;
	private Path[] dstDataLakeLocalStorageDir;
	private Boolean[] dstDataLakePublishing;
	private DataLakeType[] dstDataLakeType;
	private String[] dstDataLakeMinioEndPoint;
	private String[] dstDataLakeMinioBucketName;
	private String[] dstDataLakeMinioAccessKey;
	private String[] dstDataLakeMinioSecretKey;
	private String[] dstDataLakeS3BucketName;
	private String[] dstDataLakeS3EndPoint;
	private String[] dstDataLakeS3AccessKey;
	private String[] dstDataLakeS3SecretKey;
		
	private DstFileStorageType[] dstFileStorageType;
	private Path[] dstFileStorageLocalFSDirectory;
	private String[] dstFileStorageSFTPHost;
	private Integer[] dstFileStorageSFTPPort;
	private String[] dstFileStorageSFTPDirectory;
	private String[] dstFileStorageSFTPUser;
	private String[] dstFileStorageSFTPPassword;
	private String[] dstFileStorageS3Url;
	private String[] dstFileStorageS3BucketName;
	private String[] dstFileStorageS3AccessKey;
	private String[] dstFileStorageS3SecretKey;
	private String[] dstFileStorageMinIOUrl;
	private String[] dstFileStorageMinIOBucketName;
	private String[] dstFileStorageMinIOAccessKey;
	private String[] dstFileStorageMinIOSecretKey;
	private Boolean[] dstCSVFilesWithHeader;
	private Character[] dstCSVFilesFieldDelimiter;
	private String[] dstCSVFilesRecordDelimiter;
	private Character[] dstCSVFilesEscapeCharacter;
	private Character[] dstCSVFilesQuoteCharacter;
	private String[] dstCSVFilesNullString;
	
	private Integer[] dstDuckDBReaderPortNumber;
	
	private String[] dstDatabricksSQLDBFSEndpoint;
	private String[] dstDatabricksSQLDBFSAccessToken;
	private String[] dstDatabricksSQLDBFSBasePath;
	
	private Long[] failedOperRetryCount;
	private Long[] failedOperRetryIntervalMs;


	//Device mapping configuration
	private DevicePatternType mapDevicesToDstPatternType;
	private Pattern[] mapDevicesToDstPattern;
	private Integer defaultDstIndexForUnmappedDevices;

	//Remove job configuration
	private String manageDevicesNameList;
	private Pattern manageDevicesNamePattern;
	private String manageDevicesIDList;
	private Pattern manageDevicesIDPattern;
	private Boolean removeDevicesFromDst;
	private ManageDevicesOperationType manageDevicesOperationType;
	private String deviceCommand;
	private String deviceCommandDetails;

	public Integer getNumDestinations() {
		return numDestinations;
	}

	public DstType getDstType(int dstIndex) {
		return this.dstType[dstIndex];
	}

	public String getDstTypeName(int dstIndex) {
		return this.dstTypeName[dstIndex];
	}

	public Long getDstInsertBatchSize(int dstIndex) {
		return this.dstInsertBatchSize[dstIndex];
	}

	public Long getDstUpdateBatchSize(int dstIndex) {
		return this.dstUpdateBatchSize[dstIndex];
	}

	public Long getDstDeleteBatchSize(int dstIndex) {
		return this.dstDeleteBatchSize[dstIndex];
	}

	public DstObjectInitMode getDstObjectInitMode(int dstIndex) {
		return this.dstObjectInitMode[dstIndex];
	}

	public DstSyncMode getDstSyncMode() {
		return this.dstSyncMode;
	}

	public DstDataTypeMapping getDstDataTypeMapping(int dstIndex) {
		return this.dstDataTypeMapping[dstIndex];
	}

	public boolean getDstEnableFilterMapperRules(int dstIndex) {
		return this.dstEnableFilterMapperRules[dstIndex];
	}

	public boolean getDstEnableValueMapper(int dstIndex) {
		return this.dstEnableValueMapper[dstIndex];
	}

	public boolean getDstEnableTriggers(int dstIndex) {
		return this.dstEnableTriggers[dstIndex];
	}

	public String getSrcTableFromDstTable(int dstIndex, String dstTable) {
		String srcTable = dstTable;
		if (this.dstTableToSrcTableMap != null) {
			if (this.dstTableToSrcTableMap[dstIndex] != null) {
				if (this.dstTableToSrcTableMap[dstIndex].containsKey(dstTable.toUpperCase())) {
					srcTable = this.dstTableToSrcTableMap[dstIndex].get(dstTable.toUpperCase());
				}
			}
		}
		return srcTable;
	}
	
	public boolean isAllowedTable(int dstIndex, String tableName) {
		if (dstTableFilterMapperRules == null) {
			return true;
		}
		if (!getDstEnableFilterMapperRules(dstIndex)) {
			return true;
		}
		String rule = dstTableFilterMapperRules[dstIndex].get(tableName.toUpperCase());
		if (rule == null) {
			//unspecified table in the rules
			if (dstAllowUnspecifiedTables[dstIndex] == true) {
				return true;
			} else {
				return false;
			}
		} else {
			if (rule.equals("true")) {
				//allowed
				return true;
			} else if (rule.equals("false")) {
				//blocked
				return false;
			} else {
				//it is mapped to other table name and hence allowed
				return true;
			}
		}
	}

	public String getMappedTableName(int dstIndex, String tableName) {
		if (dstTableFilterMapperRules == null) {
			return tableName;
		}
		if (!getDstEnableFilterMapperRules(dstIndex)) {
			return tableName;
		}
		String rule = dstTableFilterMapperRules[dstIndex].get(tableName.toUpperCase());
		if (rule == null) {
			//unspecified table in the rules
			if (dstAllowUnspecifiedTables[dstIndex] == true) {
				return tableName;
			} else {
				return null;
			}
		} else {
			if (rule.equals("true")) {
				//allowed
				return tableName;
			} else if (rule.equals("false")) {
				//blocked
				return null;
			} else {
				//it is mapped to other table name and hence allowed
				return rule;
			}
		}
	}

	public boolean isAllowedColumn(int dstIndex, String tableName, String columnName) {
		if (dstColumnFilterMapperRules == null) {
			return true;
		}
		if (!getDstEnableFilterMapperRules(dstIndex)) {
			return true;
		}
		String rule = null;
		
		if (dstColumnFilterMapperRules[dstIndex] == null) {
			return true;
		}
		
		if (dstColumnFilterMapperRules[dstIndex].get(tableName.toUpperCase()) != null) {
			rule = dstColumnFilterMapperRules[dstIndex].get(tableName.toUpperCase()).get(columnName.toUpperCase());
		} else {
			return true;
		}
		if (rule == null) {
			//unspecified column in the rules
			if (dstAllowUnspecifiedColumns[dstIndex] == true) {
				return true;
			} else {
				return false;
			}
		} else {
			if (rule.equals("true")) {
				//allowed
				return true;
			} else if (rule.equals("false")) {
				//blocked
				return false;
			} else {
				//it is mapped to other table name and hence allowed
				return true;
			}
		}
	}

	public String getMappedColumnName(int dstIndex, String tableName, String columnName) {		
		if (dstColumnFilterMapperRules == null) {
			return columnName;
		}
		if (!getDstEnableFilterMapperRules(dstIndex)) {
			return columnName;
		}
		String rule = null;
		if (dstColumnFilterMapperRules[dstIndex].get(tableName.toUpperCase()) != null) {
			rule = dstColumnFilterMapperRules[dstIndex].get(tableName.toUpperCase()).get(columnName.toUpperCase());
		} 
		if (rule == null) {
			//unspecified column in the rules
			if (dstAllowUnspecifiedColumns[dstIndex] == true) {
				return columnName;
			} else {
				return null;
			}
		} else {
			if (rule.equals("true")) {
				//allowed
				return columnName;
			} else if (rule.equals("false")) {
				//blocked
				return null;
			} else {
				//it is mapped to other table name and hence allowed
				return rule;
			}
		}
	}

	public String getMappedValue(int dstIndex, String srcTableName, String srcColumnName, String srcValue) {
		if (! getDstEnableValueMapper(dstIndex)) {
			return null;
		} else {
			if (this.dstValueMappings[dstIndex].get(srcTableName.toUpperCase()) == null) {
				return null;
			} else {
				if (this.dstValueMappings[dstIndex].get(srcTableName.toUpperCase()).get(srcColumnName.toUpperCase()) == null) {
					return null;
				} else {
					return this.dstValueMappings[dstIndex].get(srcTableName.toUpperCase()).get(srcColumnName.toUpperCase()).get(srcValue);
				}
			}
		}
	}
	
	public boolean getDstOperPredicateOpt(int dstIndex) {
		return this.dstOperPredicateOpt[dstIndex];
	}

	public boolean getDstIdempotentDataIngestion(int dstIndex) {
		return this.dstIdempotentDataIngestion[dstIndex];
	}

	public DstIdempotentDataIngestionMethod getDstIdempotentDataIngestionMethod(int dstIndex) {
		return this.dstIdempotentDataIngestionMethod[dstIndex];
	}

	public boolean getDstSkipFailedLogFiles(int dstIndex) {
		return this.dstSkipFailedLogFiles[dstIndex];
	}

	public boolean getDstSetUnparsableValuesToNull(int dstIndex) {
		return this.dstSetUnparsableValuesToNull[dstIndex];
	}

	public boolean getDstQuoteObjectNames(int dstIndex) {
		return this.dstQuoteObjectNames[dstIndex];
	}

	public boolean getDstQuoteColumnNames(int dstIndex) {
		return this.dstQuoteColumnNames[dstIndex];
	}

	public boolean getDstUseCatalogScopeResolution(int dstIndex) {
		return this.dstUseCatalogScopeResolution[dstIndex];
	}

	public boolean getDstUseSchemaScopeResolution(int dstIndex) {
		return this.dstUseSchemaScopeResolution[dstIndex];
	}

	public boolean getDstDisableMetadataTable(int dstIndex) {
		return this.dstDisableMetadataTable[dstIndex];
	}

	public String getDstCreateTableSuffix(int dstIndex) {
		return this.dstCreateTableSuffix[dstIndex];
	}

	public String getDstClickHouseEngine(int dstIndex) {
		return this.dstClickHouseEngine[dstIndex];
	}

	public Boolean getDstMongoDBUseTransactions(int dstIndex) {
		return this.dstMongoDBUseTransactions[dstIndex];
	}

	public Long getDstOperRetryCount(int dstIndex) {
		return this.failedOperRetryCount[dstIndex];
	}

	public Long getDstOperRetryIntervalMs(int dstIndex) {
		return this.failedOperRetryIntervalMs[dstIndex];
	}

	public Long getSyncLiteOperRetryCount() {
		return this.syncLiteOperRetryCount;
	}

	public Long getSyncLiteOperRetryIntervalMs() {
		return this.syncLiteOperRetryIntervalMs;
	}

	public String getDstDatabase(int dstIndex) {
		return this.dstDatabase[dstIndex];
	}

	public String getDstSchema(int dstIndex) {
		return this.dstSchema[dstIndex];
	}

	public DstDeviceSchemaNamePolicy getDstDeviceSchemaNamePolicy(int dstIndex) {
		return this.dstDeviceSchemaNamePolicy[dstIndex];
	}

	public String getDstAlias(int dstIndex) {
		return this.dstAlias[dstIndex];
	}

	public String getDstConnStr(int dstIndex) {
		return this.dstConnStr[dstIndex];
	}

	public Long getDstConnectionTimeoutS(int dstIndex) {
		return this.dstConnectionTimeoutS[dstIndex];
	}

	public String getDstUser(int dstIndex) {
		return this.dstUser[dstIndex];
	}

	public String getDstPassword(int dstIndex) {
		return this.dstPassword[dstIndex];
	}

	public Boolean getDstPGVectorExtensionEnabled(int dstIndex) {
		return this.dstPGVectorExtensionEnabled[dstIndex];
	}

	public String getDstSparkConfigurations(int dstIndex) {
		return this.dstSparkConfigurations[dstIndex];
	}

	public Integer getDstDuckDBReaderPortNumber(int dstIndex) {
		return this.dstDuckDBReaderPortNumber[dstIndex];
	}

	public DstDataLakeDataFormat getDstDataLakeDataFormat(int dstIndex) {
		return this.dstDataLakeDataFormat[dstIndex];
	}

	public Integer getDstDataLakeObjectSwitchInterval(int dstIndex) {
		return this.dstDataLakeObjectSwitchInterval[dstIndex];
	}

	public ChronoUnit getDstDataLakeObjectSwitchIntervalUnit(int dstIndex) {
		return this.dstDataLakeObjectSwitchIntervalUnit[dstIndex];
	}

	public Path getDataLakeLocalStorageDir(int dstIndex) {
		return this.dstDataLakeLocalStorageDir[dstIndex];
	}

	public Boolean getDstDataLakePublishing(int dstIndex) {
		return this.dstDataLakePublishing[dstIndex];
	}

	public DataLakeType getDstRemoteDataLakeType(int dstIndex) {
		return this.dstDataLakeType[dstIndex];
	}

	public String getDstDataLakeMinioEndpoint(int dstIndex) {
		return this.dstDataLakeMinioEndPoint[dstIndex];
	}

	public String getDstDataLakeMinioBucketName(int dstIndex) {
		return this.dstDataLakeMinioBucketName[dstIndex];
	}

	public String getDstDataLakeMinioAccessKey(int dstIndex) {
		return this.dstDataLakeMinioAccessKey[dstIndex];
	}

	public String getDstDataLakeMinioSecretKey(int dstIndex) {
		return this.dstDataLakeMinioSecretKey[dstIndex];
	}

	public String getDstDataLakeS3Endpoint(int dstIndex) {
		return this.dstDataLakeS3EndPoint[dstIndex];
	}

	public String getDstDataLakeS3BucketName(int dstIndex) {
		return this.dstDataLakeS3BucketName[dstIndex];
	}

	public String getDstDataLakeS3AccessKey(int dstIndex) {
		return this.dstDataLakeS3AccessKey[dstIndex];
	}

	public String getDstDataLakeS3SecretKey(int dstIndex) {
		return this.dstDataLakeS3SecretKey[dstIndex];
	}

	public DstFileStorageType getDstFileStorageType(int dstIndex) {
		return dstFileStorageType[dstIndex];
	}

	public Path getDstFileStorageLocalFSDirectory(int dstIndex) {
		return dstFileStorageLocalFSDirectory[dstIndex];
	}

	public String getDstFileStorageSFTPHost(int dstIndex) {
		return dstFileStorageSFTPHost[dstIndex];
	}

	public Integer getDstFileStorageSFTPPort(int dstIndex) {
		return dstFileStorageSFTPPort[dstIndex];
	}

	public String getDstFileStorageSFTPDirectory(int dstIndex) {
		return dstFileStorageSFTPDirectory[dstIndex];
	}

	public String getDstFileStorageSFTPUser(int dstIndex) {
		return dstFileStorageSFTPUser[dstIndex];
	}

	public String getDstFileStorageSFTPPassword(int dstIndex) {
		return dstFileStorageSFTPPassword[dstIndex];
	}

	public String getDstFileStorageS3Url(int dstIndex) {
		return dstFileStorageS3Url[dstIndex];
	}

	public String getDstFileStorageS3BucketName(int dstIndex) {
		return dstFileStorageS3BucketName[dstIndex];
	}

	public String getDstFileStorageS3AccessKey(int dstIndex) {
		return dstFileStorageS3AccessKey[dstIndex];
	}

	public String getDstFileStorageS3SecretKey(int dstIndex) {
		return dstFileStorageS3SecretKey[dstIndex];
	}


	public String getDstFileStorageMinIOUrl(int dstIndex) {
		return dstFileStorageMinIOUrl[dstIndex];
	}

	public String getDstFileStorageMinIOBucketName(int dstIndex) {
		return dstFileStorageMinIOBucketName[dstIndex];
	}

	public String getDstFileStorageMinIOAccessKey(int dstIndex) {
		return dstFileStorageMinIOAccessKey[dstIndex];
	}

	public String getDstFileStorageMinIOSecretKey(int dstIndex) {
		return dstFileStorageMinIOSecretKey[dstIndex];
	}

	public Boolean getDstCSVFilesWithHeader(int dstIndex) {
		return dstCSVFilesWithHeader[dstIndex];
	}
	
	public Character getDstCSVFilesFieldDelimiter(int dstIndex) {
		return dstCSVFilesFieldDelimiter[dstIndex];
	}
	
	public String  getDstCSVFilesRecordDelimiter(int dstIndex) {
		return dstCSVFilesRecordDelimiter[dstIndex];
	}
	
	public Character getDstCSVFilesEscapeCharacter(int dstIndex) {
		return dstCSVFilesEscapeCharacter[dstIndex];
	}
	
	public Character getDstCSVFilesQuoteCharacter(int dstIndex) {
		return dstCSVFilesQuoteCharacter[dstIndex];
	}
	
	public String getDstCSVFilesNullString(int dstIndex) {
		return dstCSVFilesNullString[dstIndex];
	}

	public String getDstDatabricksSQLDBFSEndpoint(int dstIndex) {
		return dstDatabricksSQLDBFSEndpoint[dstIndex];
	}

	public String getDstDatabricksSQLDBFSAccessToken(int dstIndex) {
		return dstDatabricksSQLDBFSAccessToken[dstIndex];
	}

	public String getDstDatabricksSQLDBFSBasePath(int dstIndex) {
		return dstDatabricksSQLDBFSBasePath[dstIndex];
	}

	public DevicePatternType getMapDevicesToDstPatternType() {
		return this.mapDevicesToDstPatternType;
	}

	public Pattern getMapDevicesToDstPattern(int dstIndex) {
		return this.mapDevicesToDstPattern[dstIndex];
	}

	public Integer getDefaultDstIndexForUnmappedDevices() {
		return this.defaultDstIndexForUnmappedDevices;
	}

	public boolean getGuiDashboard() { 
		return this.guiDashboard;
	}

	public long getUpdateStatisticsIntervalS() { 
		return this.updateStatisticsIntervalS;
	}

	public boolean getEnablePrometheusStatisticsPublisher() { 
		return this.enablePrometheusStatisticsPublisher;
	}

	public URL getPrometheusPushGatewayURL() { 
		return this.prometheusPushGatewayURL;
	}

	public long getPrometheusStatisticsPublisherIntervalS( ) {
		return prometheusStatisticsPublisherIntervalS;
	}

	public SyncerRole getSyncerRole() {
		return this.syncerRole;
	}

	public Path getDeviceCommandRoot() {
		return this.deviceCommandRoot;
	}

	public Boolean getEnableRequestProcessor() {
		return this.enableRequestProcessor;
	}

	public Integer getRequestProcessorPort() {
		return this.requestProcessorPort;
	}

	public Path getDeviceDataRoot() {
		return this.deviceDataRoot;
	}

	public Path getDeviceUploadRoot() {
		return this.deviceUploadRoot;
	}

	public Path getLicenseFile() {
		return this.licenseFilePath;
	}

	public String getPropertyValue(String propName) {
		return properties.get(propName);
	}

	public Integer getNumDeviceProcessors() {
		return this.numDeviceProcessors;
	}

	public Integer getFailedDeviceRetryIntervalS() {
		return this.failedDeviceRetryIntervalS;
	}

	public Long getDeviceScannerIntervalS() {
		return this.deviceScannerIntervalS;
	}

	public Long getDevicePollingIntervalMs() {
		return this.devicePollingIntervalMs;
	}

	public DeviceSchedulerType getDeviceSchedulerType() {
		return this.deviceSchedulerType;
	}

	public TraceLevel getTraceLevel() {
		return this.traceLevel;
	}

	public Long getDeviceSnapshotIntervalS() {
		return this.deviceSnapshotIntervalS;
	}

	public Set<String> getIncludeDeviceIDs() {
		return includeDeviceIDs;
	}

	public Pattern getIncludeDeviceIDPattern() {
		return includeDeviceIDPattern;
	}

	public Set<String> getIncludeDeviceNames() {
		return includeDeviceNames;
	}

	public Pattern getIncludeDeviceNamePattern() {
		return includeDeviceNamePattern;
	}

	public Long getDeviceCountLimit() {
		return this.deviceCountLimit;
	}

	public Long getPerDeviceOperationCountLimit() {
		return this.perDeviceOperationCountLimit;
	}

	public Long getPerDeviceProcessedLogSizeLimit() {
		return this.perDeviceProcessedLogSizeLimit;
	}

	public Boolean checkPerDeviceLimits() {
		return checkPerDeviceLimits;
	}

	public Long getTotalOperationCountLimit() {
		return this.totalOperationCountLimit;
	}

	public Long getTotalTransactionCountLimit() {
		return this.totalTransactionCountLimit;
	}

	public Long getTotalProcessedLogSizeLimit() {
		return this.totalProcessedLogSizeLimit;
	}

	public Boolean checkGlobalLimits() {
		return checkGlobalLimits;
	}

	public ConsolidatorEdition getConsolidatorEdition() {
		return edition;
	}

	public boolean isAllowedDestination(DstType dst) {
		if (this.allowedDestinations.contains(dst)) {
			return true;
		} else {
			if (this.allowedDestinations.contains(DstType.ALL)) {
				return true;
			}
		}
		return false;
	}

	public DeviceStageType getDeviceStageType() {
		return this.deviceStageType;
	}

	public String getStageSFTPHost() {
		return stageSFTPHost;
	}

	public Integer getStageSFTPPort() {
		return stageSFTPPort;
	}

	public String getStageMinioCommandBucketName() {
		return this.stageMinioCommandBucketName;
	}

	public String getStageSFTPUser() {
		return stageSFTPUser;
	}

	public String getStageSFTPPassword() {
		return stageSFTPPassword;
	}

	public String getStageSFTPDataDirectory() {
		return stageSFTPDataDirectory;
	}

	public String getStageSFTPCommandDirectory() {
		return stageSFTPCommandDirectory;
	}

	public String getStageMinioEndpoint() {
		return this.stageMinioEndPoint;
	}

	public String getStageMinioDataBucketName() {
		return this.stageMinioDataBucketName;
	}

	public String getStageMinioAccessKey() {
		return this.stageMinioAccessKey;
	}

	public String getStageMinioSecretKey() {
		return this.stageMinioSecretKey;
	}    

	public String getStageS3Endpoint() {
		return this.stageS3EndPoint;
	}

	public String getStageS3DataBucketName() {
		return this.stageS3DataBucketName;
	}

	public String getStageS3CommandBucketName() {
		return this.stageS3CommandBucketName;
	}

	public String getStageS3AccessKey() {
		return this.stageS3AccessKey;
	}

	public String getStageS3SecretKey() {
		return this.stageS3SecretKey;
	}    

	public Boolean getThrottleStageRequestRate() {
		return this.throttleStageRequestRate;
	}

	public Long getMaxStageRequestsPerMinute() {
		return this.maxStageRequestsPerMinute;
	}

	public Path getStageKafkaConsumerPropertiesFile() {
		return this.stageKafkaConsumerPropertiesFile;
	}

	public Path getCommandKafkaProducerPropertiesFile() {
		return this.stageKafkaProducerPropertiesFile;
	}

	public Boolean getEnableDeviceCommandHandler() {
		return this.enableDeviceCommandHandler;
	}

	public Long getDeviceCommandTimeoutS() {
		return this.deviceCommandTimeoutS;
	}

	public Boolean getDeviceEncryptionEnabled() {
		return this.deviceEncryptionEnabled;
	}

	public Path getDeviceDecryptionKeyFile() {
		return this.deviceDecryptionKeyFile;
	}

	public Long getStageOperRetryCount() {
		return this.failedStageOperRetryCount;
	}

	public Long getStageOperRetryIntervalMs() {
		return this.failedStageOperRetryIntervalMs;
	}

	public Boolean getEnableReplicasForTelemetryDevices() {
		return this.enableReplicasForTelemetryDevices;
	}

	public Boolean getSkipBadTxnFiles() {
		return this.skipBadTxnFiles;
	}

	public Boolean getDisableReplicasForAppenderDevices() {
		return this.disableReplicasForAppenderDevices;
	}

	public String getManageDevicesNameList() {
		return manageDevicesNameList;
	}

	public Pattern getManageDevicesNamePattern() {
		return manageDevicesNamePattern;
	}

	public String getManageDevicesIDList() {
		return manageDevicesIDList;
	}

	public Pattern getManageDevicesIDPattern() {
		return manageDevicesIDPattern;
	}

	public Boolean getRemoveDevicesFromDst() {
		return removeDevicesFromDst;
	}

	public String getDeviceCommand() {
		return deviceCommand;
	}

	public String getDeviceCommandDetails() {
		return deviceCommandDetails;
	}

	public ManageDevicesOperationType getManageDevicesOperationType() {
		return manageDevicesOperationType;
	}

	private ConfLoader() {

	}

	public void loadSyncConfigProperties(Path propsPath) throws SyncLiteException {
		this.properties = loadPropertiesFromFile(propsPath);
		validateAndLoadLicense();
		validateAndProcessSyncProperties();    	
	}

	public void loadManageDevicesConfigProperties(Path manageConfPath) throws SyncLitePropsException {
		this.properties.putAll(loadPropertiesFromFile(manageConfPath));
		validateAndProcessManageDevicesProperties();		
	}

	private void validateAndLoadLicense() throws SyncLiteException {
		String propValue = properties.get("license-file");
		if (propValue != null) {
			this.licenseFilePath= Path.of(propValue);
			if (this.licenseFilePath == null) {
				throw new SyncLitePropsException("Invalid value specified for license-file in configuration file");
			}
			if (!Files.exists(this.licenseFilePath)) {
				throw new SyncLitePropsException("Specified license-file does not exist : " + licenseFilePath);
			}
			if (!this.licenseFilePath.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified license-file path");
			}
		} else {
			//throw new SyncLitePropsException("license-file not specified in configuration file");
		}
		LicenseVerifier.validateLicense(licenseFilePath);
		properties.putAll(LicenseVerifier.getLicenseProperties());
	}

	public static HashMap<String, String> loadPropertiesFromFile(Path propsPath) throws SyncLitePropsException {
		BufferedReader reader = null;
		try {
			HashMap<String, String> properties = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (tokens[0].startsWith("=")) {
							throw new SyncLitePropsException("Invalid line in configuration file " + propsPath + " : " + line);
						}
					} else { 
						throw new SyncLitePropsException("Invalid line in configuration file " + propsPath + " : " + line);
					}
				}
				properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				line = reader.readLine();
			}
			return properties;
		} catch (IOException e) {
			throw new SyncLitePropsException("Failed to load configuration file : " + propsPath + " : ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new SyncLitePropsException("Failed to close configuration file : " + propsPath + ": " , e);
				}
			}
		}
	}

	private void validateAndProcessSyncProperties() throws SyncLitePropsException {

		String propValue = properties.get("device-data-root");
		if (propValue != null) {
			this.deviceDataRoot = Path.of(propValue);
			if (this.deviceDataRoot == null) {
				throw new SyncLitePropsException("Invalid value specified for device-data-root in configuration file");
			}
			if (!Files.exists(this.deviceDataRoot)) {
				throw new SyncLitePropsException("Specified device-data-root path does not exist : " + this.deviceDataRoot);
			}
			if (!this.deviceDataRoot.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified device-data-root path");
			}
			if (!this.deviceDataRoot.toFile().canWrite()) {
				throw new SyncLitePropsException("No write permission on specified device-data-root path");
			}
		} else {
			throw new SyncLitePropsException("device-data-root not specified in configuration file");
		}

		propValue = properties.get("edition");
		if (propValue != null) {
			try {
				this.edition = ConsolidatorEdition.valueOf(propValue);
				if (this.edition == null) {
					throw new SyncLitePropsException("Invalid edition " + propValue + " specified : in license file");
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid edition " + propValue + " specified in license file");
			}
		} else {
			this.edition = ConsolidatorEdition.DEVELOPER;
		}

		propValue = properties.get("num-destinations");
		if (propValue != null) {
			this.numDestinations= Integer.valueOf(propValue);
			if (this.numDestinations== null) {
				throw new SyncLitePropsException("Invalid value specified for num-destinations in configuration file : " + propValue);
			} else if (this.numDestinations <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for num-destinations in configuration file : " + propValue);
			}

			/*
			if (this.numDestinations > 1) {
				if (this.edition == ConsolidatorEdition.DEVELOPER) {
					throw new SyncLitePropsException("Feature Not Supported : Multiple destinations are not supported in developer edition.");
				}
			}*/
		} else {
			throw new SyncLitePropsException("num-destinations not specified in configuration file");
		}

		//Parse dstTypes first and then rest all

		this.dstType = new DstType[numDestinations + 1];
		this.dstTypeName = new String[numDestinations + 1];
		for (int dstIndex = 1 ; dstIndex <= numDestinations ; ++dstIndex) {
			propValue = properties.get("dst-type-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstType[dstIndex] = DstType.valueOf(properties.get("dst-type-" + dstIndex));
					if (this.dstType[dstIndex] == null) {
						throw new SyncLitePropsException("Unsupported dst-type-" + dstIndex + " specified : " + propValue);
					}
					
					if (this.edition == ConsolidatorEdition.DEVELOPER) {
						if ((this.dstType[dstIndex] == DstType.POSTGRESQL) || (this.dstType[dstIndex] == DstType.MYSQL) || (this.dstType[dstIndex] == DstType.SQLITE) || (this.dstType[dstIndex] == DstType.DUCKDB) || (this.dstType[dstIndex] == DstType.MONGODB) || (this.dstType[dstIndex] == DstType.APACHE_ICEBERG)) {
							//Allowed in Developer Edition
						} else {
							throw new SyncLitePropsException("Feature Not Supported : Destination " + this.dstType[dstIndex] + " is not supported in developer edition.");
						}
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid dst-type-" + dstIndex + " specified : " + propValue);
				}
			} else {
				throw new SyncLitePropsException("dst-type-" + dstIndex + " not specified in configuration file");
			}
		}

		this.dstDatabase = new String[numDestinations + 1];
		this.dstSchema = new String[numDestinations + 1];
		this.dstDeviceSchemaNamePolicy = new DstDeviceSchemaNamePolicy[numDestinations + 1];
		this.dstAlias = new String[numDestinations + 1];
		this.dstConnStr = new String[numDestinations + 1];
		this.dstConnectionTimeoutS = new Long[numDestinations + 1];
		this.dstUser = new String[numDestinations + 1];
		this.dstPassword = new String[numDestinations + 1];
		this.dstPGVectorExtensionEnabled  = new Boolean[numDestinations + 1];
		this.dstSparkConfigurations = new String[numDestinations + 1];
		this.dstInsertBatchSize = new Long[numDestinations + 1];
		this.dstUpdateBatchSize = new Long[numDestinations + 1];
		this.dstDeleteBatchSize = new Long[numDestinations + 1];
		this.dstObjectInitMode = new DstObjectInitMode[numDestinations + 1];
		this.failedOperRetryCount = new Long[numDestinations + 1];
		this.failedOperRetryIntervalMs = new Long[numDestinations + 1];
		this.dstSkipFailedLogFiles = new Boolean[numDestinations + 1];
		this.dstSetUnparsableValuesToNull = new Boolean[numDestinations + 1];
		this.dstQuoteObjectNames = new Boolean[numDestinations + 1];
		this.dstQuoteColumnNames = new Boolean[numDestinations + 1];
		this.dstUseCatalogScopeResolution = new Boolean[numDestinations + 1];
		this.dstUseSchemaScopeResolution = new Boolean[numDestinations + 1];
		this.dstDisableMetadataTable = new Boolean[numDestinations + 1];
		this.dstCreateTableSuffix = new String[numDestinations + 1];
		this.dstClickHouseEngine = new String[numDestinations + 1];
		this.dstMongoDBUseTransactions = new Boolean[numDestinations + 1];
		this.dstDataTypeMapping = new DstDataTypeMapping[numDestinations + 1];
		this.dstEnableFilterMapperRules = new Boolean[numDestinations + 1];
		this.dstAllowUnspecifiedTables = new Boolean[numDestinations + 1];
		this.dstAllowUnspecifiedColumns = new Boolean[numDestinations + 1];
		this.dstTableFilterMapperRules = new HashMap[numDestinations + 1];
		this.dstTableToSrcTableMap = new HashMap[numDestinations + 1];
		this.dstColumnFilterMapperRules = new HashMap[numDestinations + 1];		
		this.dstEnableValueMapper = new Boolean[numDestinations + 1];
		this.dstValueMappings = new HashMap[numDestinations + 1];
		this.dstEnableTriggers = new Boolean[numDestinations + 1];
		this.dstTriggers = new HashMap[numDestinations + 1];
		this.dstOperPredicateOpt = new Boolean[numDestinations + 1];
		this.dstIdempotentDataIngestion = new Boolean[numDestinations + 1];
		this.dstIdempotentDataIngestionMethod = new DstIdempotentDataIngestionMethod[numDestinations + 1];
		this.dstDataLakeDataFormat = new DstDataLakeDataFormat[numDestinations + 1];
		this.dstDataLakeObjectSwitchInterval = new Integer[numDestinations + 1];
		this.dstDataLakeObjectSwitchIntervalUnit = new ChronoUnit[numDestinations + 1];
		this.dstDataLakeLocalStorageDir = new Path[numDestinations + 1];
		this.dstDataLakePublishing = new Boolean[numDestinations + 1];
		this.dstDataLakeS3EndPoint = new String[numDestinations + 1];
		this.dstDataLakeS3BucketName = new String[numDestinations + 1];
		this.dstDataLakeS3AccessKey = new String[numDestinations + 1];
		this.dstDataLakeS3SecretKey = new String[numDestinations + 1];
		this.dstDataLakeMinioEndPoint = new String[numDestinations + 1];
		this.dstDataLakeMinioBucketName = new String[numDestinations + 1];
		this.dstDataLakeMinioAccessKey = new String[numDestinations + 1];
		this.dstDataLakeMinioSecretKey = new String[numDestinations + 1];
		this.dstDuckDBReaderPortNumber = new Integer[numDestinations + 1];

		for (int dstIndex = 1 ; dstIndex <= numDestinations ; ++dstIndex) {			

			SQLGenerator sqlGen = SQLGenerator.getInstance(dstIndex);
			propValue = properties.get("dst-database-" + dstIndex);
			if (propValue != null) {
				this.dstDatabase[dstIndex] = propValue;
			} else {
				if (sqlGen.isDatabaseAllowed()) {
					throw new SyncLitePropsException("dst-database-" + dstIndex + " not specified in configuration file");
				}
			}

			propValue = properties.get("dst-schema-" + dstIndex);
			if (propValue != null) {
				this.dstSchema[dstIndex] = propValue;
			} else {
				if (sqlGen.isSchemaAllowed()) {
					throw new SyncLitePropsException("dst-schema-" + dstIndex + " not specified in configuration file");
				}
			}

			propValue = properties.get("dst-device-schema-name-policy-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstDeviceSchemaNamePolicy[dstIndex] = DstDeviceSchemaNamePolicy.valueOf(propValue);
					if ( this.dstDeviceSchemaNamePolicy[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-device-schema-name-policy-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-device-schema-name-policy-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstDeviceSchemaNamePolicy[dstIndex] = DstDeviceSchemaNamePolicy.SYNCLITE_DEVICE_ID_AND_NAME;
			}

			propValue = properties.get("dst-alias-" + dstIndex);
			if (propValue != null) {
				this.dstAlias[dstIndex] = propValue;
			} else {
				this.dstAlias[dstIndex] = "DB-" + dstIndex;
			}

			if (this.dstType[dstIndex] == DstType.DUCKDB) {
				propValue = properties.get("dst-duckdb-reader-port-" + dstIndex);
				if (propValue != null) {
					try {
						this.dstDuckDBReaderPortNumber[dstIndex] = Integer.valueOf(propValue);
						if ( this.dstDuckDBReaderPortNumber[dstIndex] == null) {
							throw new SyncLitePropsException("Invalid value specified for dst-duckdb-reader-port-" + dstIndex + " in configuration file : " + propValue);
						}
					} catch (IllegalArgumentException e) {
						throw new SyncLitePropsException("Invalid value specified for dst-duckdb-reader-port-" + dstIndex + " in configuration file : " + propValue);
					}
				} else {
					throw new SyncLitePropsException("dst-duckdb-reader-port-" + dstIndex + " not specified in configuration file");
				}
			}

			propValue = properties.get("dst-connection-string-" + dstIndex);
			if (propValue != null) {
				this.dstConnStr[dstIndex] = propValue;
			} else {        	
				//Set default conn string for SQLITE and DUCKDB
				if (dstType[dstIndex] == DstType.SQLITE) {
					this.dstConnStr[dstIndex] = "jdbc:sqlite:" + deviceDataRoot.resolve("consolidated_db_" + dstIndex + ".sqlite") + "?journal_mode=WAL";
				} else if(dstType[dstIndex] == DstType.DUCKDB) {
					this.dstConnStr[dstIndex] = "jdbc:duckdb:" + deviceDataRoot.resolve("consolidated_db_" + dstIndex + ".duckdb");
				} else if((dstType[dstIndex] == DstType.APACHE_ICEBERG)) {
					this.dstConnStr[dstIndex] = "";
				} else {
					throw new SyncLitePropsException("dst-connection-string-" + dstIndex + " not specified in configuration file");
				}
			}

			propValue = properties.get("dst-connection-timeout-s-" + dstIndex);
			if (propValue != null) {
				this.dstConnectionTimeoutS[dstIndex] = Long.valueOf(propValue);
				if (this.dstConnectionTimeoutS[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-connection-timeout-s-" + dstIndex + " in configuration file : " + propValue);
				} else if (this.dstConnectionTimeoutS[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-connection-timeout-s-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstConnectionTimeoutS[dstIndex] = 30L;
			}


			propValue = properties.get("dst-postgresql-vector-extension-enabled-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstPGVectorExtensionEnabled[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstPGVectorExtensionEnabled[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-postgresql-vector-extension-enabled--" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-postgresql-vector-extension-enabled-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstPGVectorExtensionEnabled[dstIndex] = false;
			}

			propValue = properties.get("dst-user-" + dstIndex);
			if (propValue != null) {
				this.dstUser[dstIndex] = propValue;
			}

			propValue = properties.get("dst-password-" + dstIndex);
			if (propValue != null) {
				this.dstPassword[dstIndex] = propValue;
			}

			propValue = properties.get("dst-insert-batch-size-" + dstIndex);
			if (propValue != null) {
				this.dstInsertBatchSize[dstIndex] = Long.valueOf(propValue);
				if (this.dstInsertBatchSize[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-insert-batch-size-" + dstIndex + " in configuration file : " + propValue);
				} else if (this.dstInsertBatchSize[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-insert-batch-size-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				//throw new SyncLitePropsException("dst-insert-batch-size not specified in configuration file");
				this.dstInsertBatchSize[dstIndex] = 5000L;
			}

			propValue = properties.get("dst-update-batch-size-" + dstIndex);
			if (propValue != null) {
				this.dstUpdateBatchSize[dstIndex] = Long.valueOf(propValue);
				if (this.dstUpdateBatchSize[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-update-batch-size-" + dstIndex + " in configuration file : " + propValue);
				} else if (this.dstUpdateBatchSize[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-update-batch-size-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				//throw new SyncLitePropsException("dst-update-batch-size not specified in configuration file");
				this.dstUpdateBatchSize[dstIndex] = 5000L;
			}

			propValue = properties.get("dst-delete-batch-size-" + dstIndex);
			if (propValue != null) {
				this.dstDeleteBatchSize[dstIndex] = Long.valueOf(propValue);
				if (this.dstDeleteBatchSize[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-delete-batch-size-" + dstIndex + " in configuration file : " + propValue);
				} else if (this.dstDeleteBatchSize[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-delete-batch-size-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				//throw new SyncLitePropsException("dst-delete-batch-size not specified in configuration file");
				this.dstDeleteBatchSize[dstIndex] = 5000L;
			}


			propValue = properties.get("dst-object-init-mode-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstObjectInitMode[dstIndex] = DstObjectInitMode.valueOf(propValue);
					if ( this.dstObjectInitMode[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-object-init-mode-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-object-init-mode-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstObjectInitMode[dstIndex] = DstObjectInitMode.TRY_CREATE_APPEND_DATA;
			}

			propValue = properties.get("dst-txn-retry-count-" + dstIndex);
			if (propValue != null) {
				this.failedOperRetryCount[dstIndex] = Long.valueOf(propValue);
				if (this.failedOperRetryCount[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-txn-retry-count-" + dstIndex + " in configuration file");
				} else if (this.failedOperRetryCount[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-txn-retry-count-" + dstIndex + " in configuration file");
				}
			} else {
				this.failedOperRetryCount[dstIndex] = 10L;
			}

			propValue = properties.get("dst-txn-retry-interval-ms-" + dstIndex);
			if (propValue != null) {
				this.failedOperRetryIntervalMs[dstIndex] = Long.valueOf(propValue);
				if (this.failedOperRetryIntervalMs[dstIndex] == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-txn-retry-interval-" + dstIndex + " in configuration file");
				} else if (this.failedOperRetryIntervalMs[dstIndex] <= 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for dst-txn-retry-interval-" + dstIndex + " in configuration file");
				}
			} else {
				this.failedOperRetryIntervalMs[dstIndex] = 10000L;
			}

			propValue = properties.get("dst-data-type-mapping-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstDataTypeMapping[dstIndex] = DstDataTypeMapping.valueOf(propValue);
					if ( this.dstDataTypeMapping[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-data-type-mapping-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-data-type-mapping-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstDataTypeMapping[dstIndex] = DstDataTypeMapping.ALL_TEXT;
			}

			propValue = properties.get("dst-enable-filter-mapper-rules-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstEnableFilterMapperRules[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstEnableFilterMapperRules[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-enable-filter-mapper-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-enable-filter-mapper-" + dstIndex + " in configuration file : " + propValue);
				}				
				
				/*
				if (this.dstEnableFilterMapperRules[dstIndex]) {
					if (this.edition == ConsolidatorEdition.DEVELOPER) {
						throw new SyncLitePropsException("Feature Not Supported : Table/Column Filtering/Mapping is not supported in developer edition.");
					}
				}*/
			} else {
				this.dstEnableFilterMapperRules[dstIndex] = false;
			}

			if (this.dstEnableFilterMapperRules[dstIndex] == true) {
				propValue = properties.get("dst-allow-unspecified-tables-" + dstIndex);
				if (propValue != null) {
					try {
						this.dstAllowUnspecifiedTables[dstIndex] = Boolean.valueOf(propValue);
						if ( this.dstAllowUnspecifiedTables[dstIndex] == null) {
							throw new SyncLitePropsException("Invalid value specified for dst-allow-unspecified-tables-" + dstIndex + " in configuration file : " + propValue);
						}
					} catch (IllegalArgumentException e) {
						throw new SyncLitePropsException("Invalid value specified for dst-allow-unspecified-tables-" + dstIndex + " in configuration file : " + propValue);
					}
				} else {
					this.dstAllowUnspecifiedTables[dstIndex] = false;
				}

				propValue = properties.get("dst-allow-unspecified-columns-" + dstIndex);				
				if (propValue != null) {
					try {
						this.dstAllowUnspecifiedColumns[dstIndex] = Boolean.valueOf(propValue);
						if ( this.dstAllowUnspecifiedColumns[dstIndex] == null) {
							throw new SyncLitePropsException("Invalid value specified for dst-allow-unspecified-columns-" + dstIndex + " in configuration file : " + propValue);
						}
					} catch (IllegalArgumentException e) {
						throw new SyncLitePropsException("Invalid value specified for dst-allow-unspecified-columns-" + dstIndex + " in configuration file : " + propValue);
					}
				} else {
					this.dstAllowUnspecifiedColumns[dstIndex] = true;
				}

				//Parse the rules
				propValue = properties.get("dst-filter-mapper-rules-file-" + dstIndex);
				if (propValue != null) {
					Path filterMapperRulesFile = Path.of(propValue);
					if (!Files.exists(filterMapperRulesFile)) {
						throw new SyncLitePropsException("Specified dst-filter-mapper-rules-file-" + dstIndex + " : " + propValue + " does not exist"); 
					}
					if (!filterMapperRulesFile.toFile().canRead()) {
						throw new SyncLitePropsException("Specified dst-filter-mapper-rules-file-" + dstIndex + " : " + propValue + " does not have read access");
					}
					parseFilterMapperRulesFile(filterMapperRulesFile, dstIndex);
				} else {
					throw new SyncLitePropsException("dst-filter-mapper-rules-file-" + dstIndex + " must be specified in configuration file while dst-enable-filter-mapper-rules-" + dstIndex + " is set to true");
				}
			}

			propValue = properties.get("dst-enable-value-mapper-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstEnableValueMapper[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstEnableValueMapper[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-enable-value-mapper-" + dstIndex + " in configuration file : " + propValue);
					}
					/*
					if (this.dstEnableValueMapper[dstIndex]) {
						if (this.edition == ConsolidatorEdition.DEVELOPER) {
							throw new SyncLitePropsException("Feature Not Supported : Value Mapping is not supported in developer edition.");
						}
					}*/
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-enable-value-mapper-" + dstIndex + " in configuration file : " + propValue);
				}			

			} else {
				this.dstEnableValueMapper[dstIndex] = false;
			}

			if (this.dstEnableValueMapper[dstIndex] == true) {
				//Parse the rules
				propValue = properties.get("dst-value-mappings-file-" + dstIndex);
				if (propValue != null) {
					Path valueMappingsFile = Path.of(propValue);
					if (!Files.exists(valueMappingsFile)) {
						throw new SyncLitePropsException("Specified dst-value-mappings-file-" + dstIndex + " : " + propValue + " does not exist"); 
					}
					if (!valueMappingsFile.toFile().canRead()) {
						throw new SyncLitePropsException("Specified dst-value-mappings-file-" + dstIndex + " : " + propValue + " does not have read access");
					}

					parseValueMappingsFile(valueMappingsFile, dstIndex);
				} else {
					throw new SyncLitePropsException("dst-value-mappings-file-" + dstIndex + " must be specified in configuration file while dst-enable-value-mapper-" + dstIndex + " is set to true");
				}
			}

			propValue = properties.get("dst-enable-triggers-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstEnableTriggers[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstEnableTriggers[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-enable-triggers-" + dstIndex + " in configuration file : " + propValue);
					}
					/*
					if (this.dstEnableTriggers[dstIndex]) {
						if (this.edition == ConsolidatorEdition.DEVELOPER) {
							throw new SyncLitePropsException("Feature Not Supported : Triggers not supported in developer edition.");
						}
					} */
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-enable-triggers-" + dstIndex + " in configuration file : " + propValue);
				}			

			} else {
				this.dstEnableTriggers[dstIndex] = false;
			}

			if (this.dstEnableTriggers[dstIndex] == true) {
				//Parse the rules
				propValue = properties.get("dst-triggers-file-" + dstIndex);
				if (propValue != null) {
					Path triggersFile = Path.of(propValue);
					if (!Files.exists(triggersFile)) {
						throw new SyncLitePropsException("Specified dst-tiggers-file-" + dstIndex + " : " + propValue + " does not exist"); 
					}
					if (!triggersFile.toFile().canRead()) {
						throw new SyncLitePropsException("Specified dst-triggers-file-" + dstIndex + " : " + propValue + " does not have read access");
					}

					parseTriggersFile(triggersFile, dstIndex);
				} else {
					throw new SyncLitePropsException("dst-triggers-file-" + dstIndex + " must be specified in configuration file while dst-enable-triggers-" + dstIndex + " is set to true");
				}
			}

			
			propValue = properties.get("dst-oper-predicate-optimization-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstOperPredicateOpt[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstOperPredicateOpt[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-oper-predicate-optimization-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-oper-predicate-optimization-" + dstIndex + " in configuration file : " + propValue);
				}
			} else {
				this.dstOperPredicateOpt[dstIndex] = true;
			}

			propValue = properties.get("dst-idempotent-data-ingestion-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstIdempotentDataIngestion[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstIdempotentDataIngestion[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-idempotent-data-ingestion-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-idempotent-data-ingestion-" + dstIndex + " in configuration file" + propValue);
				}
			} else {
				this.dstIdempotentDataIngestion[dstIndex] = false;
			}

			propValue = properties.get("dst-idempotent-data-ingestion-method-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstIdempotentDataIngestionMethod[dstIndex] = DstIdempotentDataIngestionMethod.valueOf(propValue);
					if (this.dstIdempotentDataIngestionMethod[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-idempotent-data-ingestion-method-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-idempotent-data-ingestion-method-" + dstIndex + " in configuration file" + propValue);
				}
			} else {
				this.dstIdempotentDataIngestionMethod[dstIndex] = DstIdempotentDataIngestionMethod.NATIVE_UPSERT;
			}

			propValue = properties.get("dst-skip-failed-log-files-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstSkipFailedLogFiles[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstSkipFailedLogFiles[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-skip-failed-log-files-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-skip-failed-log-files-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstSkipFailedLogFiles[dstIndex] = false;
			}

			propValue = properties.get("dst-set-unparsable-values-to-null-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstSetUnparsableValuesToNull[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstSetUnparsableValuesToNull[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-set-unparsable-values-to-null-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-set-unparsable-values-to-null-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstSetUnparsableValuesToNull[dstIndex] = false;
			}

			propValue = properties.get("dst-quote-object-names-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstQuoteObjectNames[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstQuoteObjectNames[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-quote-object-names-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-quote-object-names-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstQuoteObjectNames[dstIndex] = false;
			}

			propValue = properties.get("dst-quote-column-names-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstQuoteColumnNames[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstQuoteColumnNames[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-quote-column-names-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-quote-column-names-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstQuoteColumnNames[dstIndex] = false;
			}

			propValue = properties.get("dst-use-catalog-scope-resolution-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstUseCatalogScopeResolution[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstUseCatalogScopeResolution[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-use-catalog-scope-resolution-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-use-catalog-scope-resolution-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstUseCatalogScopeResolution[dstIndex] = true;
			}

			propValue = properties.get("dst-use-schema-scope-resolution-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstUseSchemaScopeResolution[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstUseSchemaScopeResolution[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-use-schema-scope-resolution-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-use-schema-scope-resolution-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstUseSchemaScopeResolution[dstIndex] = true;
			}

			propValue = properties.get("dst-disable-metadata-table-" + dstIndex);
			if (propValue != null) {
				try {
					this.dstDisableMetadataTable[dstIndex] = Boolean.valueOf(propValue);
					if ( this.dstDisableMetadataTable[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for dst-disable-metadata-table-" + dstIndex + " in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for dst-disable-metadata-table-" + dstIndex + " in configuration file : " + propValue);
				}				
			} else {
				this.dstDisableMetadataTable[dstIndex] = false;
			}

			propValue = properties.get("dst-create-table-suffix-" + dstIndex);
			if (propValue != null) {
				this.dstCreateTableSuffix[dstIndex] = propValue;
			} else {
				this.dstCreateTableSuffix[dstIndex] = "";
			}

			//Dst Specific config validation
			switch (dstType[dstIndex]) {
				case MONGODB:
					propValue = properties.get("dst-mongodb-use-transactions-" + dstIndex);
					if (propValue != null) {
						try {
							this.dstMongoDBUseTransactions[dstIndex] = Boolean.valueOf(propValue);
							if ( this.dstMongoDBUseTransactions[dstIndex] == null) {
								throw new SyncLitePropsException("Invalid value specified for dst-mongodb-use-transactions-" + dstIndex + " in configuration file : " + propValue);
							}
						} catch (IllegalArgumentException e) {
							throw new SyncLitePropsException("Invalid value specified for dst-mongodb-use-transactions-" + dstIndex + " in configuration file : " + propValue);
						}
					} else {
						this.dstMongoDBUseTransactions[dstIndex] = false;
					}
	
					if (this.dstMongoDBUseTransactions[dstIndex] == false) {
						this.dstDisableMetadataTable[dstIndex] = true;
					}
					break;
	
			case APACHE_ICEBERG:
				propValue = properties.get("dst-spark-configuration-file-" + dstIndex);
				if (propValue != null) {
					Path p  = Path.of(propValue);
					if (!Files.exists(p)) {
						throw new SyncLitePropsException("Specified dst-spark-configuration-file-" + dstIndex + " does not exist : " + propValue);
					}
					if (!p.toFile().canRead()) {
						throw new SyncLitePropsException("No read permission on specified dst-spark-configuration-file-" + dstIndex);
					}
					try {
						this.dstSparkConfigurations[dstIndex] = Files.readString(p);
					} catch (IOException e) {
						throw new SyncLitePropsException("Failed to read contents of specified dst-spark-configuration-file-" + dstIndex + " : " + e.getMessage(), e);
					}
				} else {
					throw new SyncLitePropsException("dst-spark-configuration-file-" + dstIndex + " not specified in configuration file.");
				}
				
				//this.dstDisableMetadataTable[dstIndex] = true;
				break;
			}


			propValue = properties.get("dst-type-name-" + dstIndex);
			if (propValue != null) {
				dstTypeName[dstIndex] = propValue;
			} else {
				dstTypeName[dstIndex] = getDstName(dstType[dstIndex], dstDataLakeDataFormat[dstIndex]);
			}
		}

		//Device mapping configs

		propValue = properties.get("map-devices-to-dst-pattern-type");
		if (propValue != null) {
			try {
				this.mapDevicesToDstPatternType = DevicePatternType.valueOf(propValue);
			} catch (Exception e) {
				throw new SyncLitePropsException("Invalid value " + propValue + " specified for map-devices-to-dst-pattern-type in configuration file");
			}
		} else {
			if (numDestinations > 1) {
				throw new SyncLitePropsException("map-devices-to-dst-pattern-type not specified in configuration file");
			} else {
				this.mapDevicesToDstPatternType = DevicePatternType.DEVICE_NAME_PATTERN;
			}
		}

		this.mapDevicesToDstPattern = new Pattern[numDestinations + 1];		
		this.mapDevicesToDstPattern = new Pattern[numDestinations + 1];
		for (int dstIndex=1 ; dstIndex <= numDestinations ; ++ dstIndex) {
			propValue = properties.get("map-devices-to-dst-pattern-" + dstIndex);
			if (propValue != null) {
				try {
					this.mapDevicesToDstPattern[dstIndex] = Pattern.compile(propValue);
					if (this.mapDevicesToDstPattern[dstIndex] == null) {
						throw new SyncLitePropsException("Invalid value specified for map-devices-to-dst-pattern-" + dstIndex + " in configuration file");
					}
				} catch (PatternSyntaxException e) {
					throw new SyncLitePropsException("Invalid value specified for map-devices-to-dst-pattern-" + dstIndex + " in configuration file", e);			
				}
			} else {				
				if (numDestinations > 1 ) {
					throw new SyncLitePropsException("map-devices-to-dst-pattern-" + dstIndex + " not specified in configuration file");
				} else {
					this.mapDevicesToDstPattern[dstIndex] = Pattern.compile(".*");
				}
			}			
		}

		propValue = properties.get("default-dst-index-for-unmapped-devices");
		if (propValue != null) {
			try {
				this.defaultDstIndexForUnmappedDevices = Integer.valueOf(propValue);

				if (this.defaultDstIndexForUnmappedDevices == null) {
					throw new SyncLitePropsException("Invalid value " + propValue + " specified for default-dst-index-for-unmapped-devices in configuration file");
				}				
				if ((this.defaultDstIndexForUnmappedDevices < 1) || (this.defaultDstIndexForUnmappedDevices > numDestinations)) {
					throw new SyncLitePropsException("Invalid value " + propValue + " specified for default-dst-index-for-unmapped-devices in configuration file, value must be between 1 and " + numDestinations);
				}
			} catch (Exception e) {
				throw new SyncLitePropsException("Invalid value " + propValue + " specified for default-dst-index-for-unmapped-devices in configuration file");
			}
		} else {
			if (numDestinations > 1) {
				throw new SyncLitePropsException("Invalid value " + propValue + " specified for default-dst-index-for-unmapped-devices in configuration file");
			} else {
				this.defaultDstIndexForUnmappedDevices = 1;
			}
		}


		propValue = properties.get("dst-sync-mode");
		if (propValue != null) {
			try {
				this.dstSyncMode = DstSyncMode.valueOf(propValue);
				if (this.dstSyncMode == null) {
					throw new SyncLitePropsException("Invalid value specified for dst-sync-mode in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for dst-sync-mode in configuration file : " + propValue);
			}
		} else {
			throw new SyncLitePropsException("dst-sync-mode not specified in configuration file");
		}


		propValue = properties.get("syncer-role");
		if (propValue != null) {
			try {
				this.syncerRole = SyncerRole.valueOf(propValue);
				if (this.syncerRole == null) {
					throw new SyncLitePropsException("Invalid value specified for syncer-role in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for syncer-role in configuration file : " + propValue);
			}
		} else {
			throw new SyncLitePropsException("syncer-role not specified in configuration file");
		}


		propValue = properties.get("device-stage-type");
		if (propValue != null) {
			this.deviceStageType = DeviceStageType.valueOf(propValue);
			if (this.deviceStageType == null) {
				throw new SyncLitePropsException("Invalid value specified for device-stage-type in configuration file");
			}
		} else {
			throw new SyncLitePropsException("device-stage-type not specified in configuration file");
		}

		propValue = properties.get("device-upload-root");
		if (propValue != null) {
			this.deviceUploadRoot = Path.of(propValue);
			if (this.deviceUploadRoot == null) {
				throw new SyncLitePropsException("Invalid value specified for device-upload-root in configuration file");
			}
			if (!Files.exists(this.deviceUploadRoot)) {
				throw new SyncLitePropsException("Specified device-upload-root path does not exist : " + this.deviceUploadRoot);
			}
			if (!this.deviceUploadRoot.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified device-upload-root path");
			}
			if (!this.deviceUploadRoot.toFile().canWrite()) {
				throw new SyncLitePropsException("No write permission on specified device-upload-root path");
			}
		} else {
			if ((this.deviceStageType == DeviceStageType.FS) || (this.deviceStageType == DeviceStageType.LOCAL_SFTP) || (this.deviceStageType == DeviceStageType.MS_ONEDRIVE) || (this.deviceStageType == DeviceStageType.GOOGLE_DRIVE) || (this.deviceStageType == DeviceStageType.LOCAL_MINIO))
			{
				throw new SyncLitePropsException("device-upload-root not specified in configuration file");
			} else if (this.deviceStageType == DeviceStageType.KAFKA){
				//For Kafka, files are downloaded and staged in the device root directory itself.
				deviceUploadRoot = deviceDataRoot;
			} else {
				//Set empty path as default
				deviceUploadRoot = Path.of("");
			}
		}
		//Make sure deviceUploadRoot is set to "" for remote targets : S3, REMOTE_MINIO        
		if ((this.deviceStageType == DeviceStageType.REMOTE_MINIO) || (this.deviceStageType == DeviceStageType.REMOTE_SFTP) || (this.deviceStageType == DeviceStageType.S3)) {
			deviceUploadRoot = Path.of("");
		}

		if (this.deviceStageType == DeviceStageType.REMOTE_SFTP) {
			propValue = properties.get("stage-sftp-host");
			if (propValue != null) {
				this.stageSFTPHost = propValue;
			} else {
				throw new SyncLitePropsException("stage-sftp-host not specified in configuration file");
			}			

			propValue = properties.get("stage-sftp-port");
			if (propValue != null) {
				try {
					this.stageSFTPPort= Integer.valueOf(propValue);
				} catch (NumberFormatException e) {
					throw new SyncLitePropsException("Invalid value specified for stage-sftp-port in configuration file. Please specify a valid numeric value");
				}
			} else {
				throw new SyncLitePropsException("stage-sftp-port not specified in configuration file");
			}			

			propValue = properties.get("stage-sftp-user");
			if (propValue != null) {
				this.stageSFTPUser= propValue;
			} else {
				throw new SyncLitePropsException("stage-sftp-user not specified in configuration file");
			}			
			propValue = properties.get("stage-sftp-password");
			if (propValue != null) {
				this.stageSFTPPassword= propValue;
			} else {
				throw new SyncLitePropsException("stage-sftp-password not specified in configuration file");
			}			

			propValue = properties.get("stage-sftp-data-directory");
			if (propValue != null) {
				this.stageSFTPDataDirectory = propValue;
			} else {
				throw new SyncLitePropsException("stage-sftp-data-directory not specified in configuration file");
			}				

			if (enableDeviceCommandHandler) {
				propValue = properties.get("stage-sftp-command-directory");
				if (propValue != null) {
					this.stageSFTPCommandDirectory = propValue;
				} else {
					throw new SyncLitePropsException("stage-sftp-command-directory not specified in configuration file");
				}
			}
		}

		if ((this.deviceStageType == DeviceStageType.LOCAL_MINIO) || (this.deviceStageType == DeviceStageType.REMOTE_MINIO)) {
			propValue = properties.get("stage-minio-endpoint");
			if (propValue != null) {
				this.stageMinioEndPoint = propValue;
			} else {
				throw new SyncLitePropsException("stage-minio-endpoint not specified in configuration file");
			}

			propValue = properties.get("stage-minio-data-bucket-name");
			if (propValue != null) {
				this.stageMinioDataBucketName = propValue;
			} else {
				throw new SyncLitePropsException("stage-minio-data-bucket-name not specified in configuration file");
			}

			if (enableDeviceCommandHandler) {
				propValue = properties.get("stage-minio-command-bucket-name");
				if (propValue != null) {
					this.stageMinioCommandBucketName = propValue;
				} else {
					throw new SyncLitePropsException("stage-minio-command-bucket-name not specified in configuration file while device command handler is enabled");
				}
			}

			propValue = properties.get("stage-minio-access-key");
			if (propValue != null) {
				this.stageMinioAccessKey = propValue;
			} else {
				throw new SyncLitePropsException("stage-minio-access-key not specified in configuration file");
			}

			propValue = properties.get("stage-minio-secret-key");
			if (propValue != null) {
				this.stageMinioSecretKey = propValue;
			} else {
				throw new SyncLitePropsException("stage-minio-secret-key not specified in configuration file");
			}
		}        

		if (this.deviceStageType == DeviceStageType.S3) {
			propValue = properties.get("stage-s3-endpoint");
			if (propValue != null) {
				this.stageS3EndPoint = propValue;
			} else {
				throw new SyncLitePropsException("stage-s3-endpoint not specified in configuration file");
			}

			propValue = properties.get("stage-s3-data-bucket-name");
			if (propValue != null) {
				this.stageS3DataBucketName = propValue;
			} else {
				throw new SyncLitePropsException("stage-s3-data-bucket-name not specified in configuration file");
			}

			if (enableDeviceCommandHandler) {
				propValue = properties.get("stage-s3-command-bucket-name");
				if (propValue != null) {
					this.stageS3CommandBucketName = propValue;
				} else {
					throw new SyncLitePropsException("stage-s3-command-bucket-name not specified in configuration file while device command handler is enabled");
				}
			}

			propValue = properties.get("stage-s3-access-key");
			if (propValue != null) {
				this.stageS3AccessKey = propValue;
			} else {
				throw new SyncLitePropsException("stage-s3-access-key not specified in configuration file");
			}

			propValue = properties.get("stage-s3-secret-key");
			if (propValue != null) {
				this.stageS3SecretKey = propValue;
			} else {
				throw new SyncLitePropsException("stage-s3-secret-key not specified in configuration file");
			}

			propValue = properties.get("throttle-stage-request-rate");
			if (propValue != null) {
				try {
					this.throttleStageRequestRate = Boolean.valueOf(propValue);
					if ( this.throttleStageRequestRate == null) {
						throw new SyncLitePropsException("Invalid value specified for throttle-stage-request-rate in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for throttle-stage-request-rate in configuration file : " + propValue);
				}
			} else {
				this.throttleStageRequestRate = true;
			}

			propValue = properties.get("max-stage-requests-per-minute");
			if (propValue != null) {
				try {
					this.maxStageRequestsPerMinute = Long.valueOf(propValue);
					if (this.maxStageRequestsPerMinute == null) {
						throw new SyncLitePropsException("Invalid value specified for max-stage-requests-per-minute in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for max-stage-requests-per-minute in configuration file : " + propValue);
				}
			} else {
				this.maxStageRequestsPerMinute = 2L; 
			}

		}

		if (this.deviceStageType == DeviceStageType.KAFKA) {
			propValue = properties.get("stage-kafka-consumer-properties-file");
			if (propValue != null) {
				this.stageKafkaConsumerPropertiesFile = Path.of(propValue);
				if (!Files.exists(this.stageKafkaConsumerPropertiesFile)) {
					throw new SyncLitePropsException("Specified stage-kafka-consumer-properties-file does not exist : " + stageKafkaConsumerPropertiesFile);
				}
				if (!this.stageKafkaConsumerPropertiesFile.toFile().canRead()) {
					throw new SyncLitePropsException("No read permission on specified stage-kafka-consumer-properties-file");
				}
			} else {
				throw new SyncLitePropsException("stage-kafka-consumer-properties-file not specified in configuration file");
			}

			if (enableDeviceCommandHandler) {	
				propValue = properties.get("stage-kafka-producer-properties-file");
				if (propValue != null) {
					this.stageKafkaProducerPropertiesFile = Path.of(propValue);
					if (!Files.exists(this.stageKafkaProducerPropertiesFile)) {
						throw new SyncLitePropsException("Specified stage-kafka-producer-properties-file does not exist : " + stageKafkaProducerPropertiesFile);
					}
					if (!this.stageKafkaProducerPropertiesFile.toFile().canRead()) {
						throw new SyncLitePropsException("No read permission on specified stage-kafka-producer-properties-file");
					}
				} else {
					throw new SyncLitePropsException("stage-kafka-producer-properties-file not specified in configuration file while device command handler is enabled");
				}
			}			
		}

		propValue = properties.get("device-encryption-enabled");
		if (propValue != null) {
			try {
				this.deviceEncryptionEnabled = Boolean.valueOf(propValue);
				if ( this.deviceEncryptionEnabled == null) {
					throw new SyncLitePropsException("Invalid value specified for device-encryption-enabled in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for device-encryption-enabled in configuration file : " + propValue);
			}
			
			if (this.deviceEncryptionEnabled == true) {
				if (this.edition == ConsolidatorEdition.DEVELOPER) {
					throw new SyncLitePropsException("Feature Not Supported : Device Encryption is not supported in developer edition");
				}
			} 
		} else {
			this.deviceEncryptionEnabled = false;
		}

		propValue = properties.get("enable-device-command-handler");
		if (propValue != null) {
			try {
				this.enableDeviceCommandHandler = Boolean.valueOf(propValue);
				if ( this.enableDeviceCommandHandler == null) {
					throw new SyncLitePropsException("Invalid value specified for enable-device-command-handler in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for enable-device-command-handler in configuration file : " + propValue);
			}
			/*
			if (this.enableDeviceCommandHandler == true) {
				if (this.edition == ConsolidatorEdition.DEVELOPER) {
					throw new SyncLitePropsException("Feature Not Supported : Device Command Handler is not supported in developer edition");
				}
			}
			*/
		} else {
			this.enableDeviceCommandHandler = false;
		}

		propValue = properties.get("device-command-timeout-s");
		if (propValue != null) {
			try {
				this.deviceCommandTimeoutS = Long.valueOf(propValue);
				if (this.deviceCommandTimeoutS == null) {
					throw new SyncLitePropsException("Invalid value specified for device-command-timeout-s in configuration file : " + propValue);
				}
				if (this.deviceCommandTimeoutS <= 0) {
					throw new SyncLitePropsException("Invalid value specified for device-command-timeout-s in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for device-command-timeout-s in configuration file : " + propValue);
			}
		} else {
			this.deviceCommandTimeoutS = 60L;
		}


		propValue = properties.get("enable-request-processor");
		if (propValue != null) {
			try {
				this.enableRequestProcessor = Boolean.valueOf(propValue);
				if ( this.enableRequestProcessor == null) {
					throw new SyncLitePropsException("Invalid value specified for enable-request-processor in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for enable-request-processor in configuration file : " + propValue);
			}
		} else {
			this.enableRequestProcessor = false;
		}

		if (this.enableRequestProcessor) {
			propValue = properties.get("request-processor-port");
			if (propValue != null) {
				try {
					this.requestProcessorPort = Integer.valueOf(propValue);
					if ( this.requestProcessorPort == null) {
						throw new SyncLitePropsException("Invalid value specified for request-processor-port in configuration file : " + propValue);
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for request-processor-port in configuration file : " + propValue);
				}
			} else {
				throw new SyncLitePropsException("No value specified for request-processor-port in configuration file while device command handler is enabled : " + propValue);
			}
		}

		propValue = properties.get("device-command");
		if (propValue != null) {
			this.deviceCommand = propValue;
			if (! this.deviceCommand.matches("[a-zA-Z0-9]+")) {
				throw new SyncLitePropsException("Invalid value specified for device-command in configuration file, it should contain alphanumeirc characters only : " + propValue);
			}
		} else {
			this.deviceCommand = null;
		}

		propValue = properties.get("device-command-details");
		if (propValue != null) {
			this.deviceCommandDetails = propValue;
		} else {
			this.deviceCommandDetails= "";
		}

		if (deviceEncryptionEnabled) {
			propValue = properties.get("device-decryption-key-file");
			if (propValue != null) {
				this.deviceDecryptionKeyFile = Path.of(propValue);
				if (this.deviceDecryptionKeyFile == null) {
					throw new SyncLitePropsException("Invalid value specified for device-decryption-key-file in configuration file");
				}
				if (!Files.exists(this.deviceDecryptionKeyFile)) {
					throw new SyncLitePropsException("Specified device-decryption-key-file does not exist : " + deviceDecryptionKeyFile);
				}
				if (!this.deviceDecryptionKeyFile.toFile().canRead()) {
					throw new SyncLitePropsException("No read permission on specified device-decryption-key-file path");
				}

				//validate if it is a valid private key

				try {
					PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Files.readAllBytes(deviceDecryptionKeyFile));
					KeyFactory keyFactory = KeyFactory.getInstance("RSA");
					PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
				} catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
					throw new SyncLitePropsException("Failed to load private key from the specified device decryption key file with exception :", e);
				}

			} else {
				throw new SyncLitePropsException("device-decryption-key-file not specified in configuration file");
			}
		}

		propValue = properties.get("stage-oper-retry-count");
		if (propValue != null) {
			this.failedStageOperRetryCount = Long.valueOf(propValue);
			if (this.failedStageOperRetryCount == null) {
				throw new SyncLitePropsException("Invalid value specified for stage-oper-retry-count in configuration file");
			} else if (this.failedStageOperRetryCount <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for stage-oper-retry-count in configuration file");
			}
		} else {
			this.failedStageOperRetryCount = 10L;
		}

		propValue = properties.get("stage-oper-retry-interval-ms");
		if (propValue != null) {
			this.failedStageOperRetryIntervalMs = Long.valueOf(propValue);
			if (this.failedStageOperRetryIntervalMs == null) {
				throw new SyncLitePropsException("Invalid value specified for stage-oper-retry-interval in configuration file");
			} else if (this.failedStageOperRetryIntervalMs <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for stage-oper-retry-interval in configuration file");
			}
		} else {
			this.failedStageOperRetryIntervalMs = 10000L;
		}

		propValue = properties.get("license-file");
		if (propValue != null) {
			this.licenseFilePath= Path.of(propValue);
			if (this.licenseFilePath == null) {
				throw new SyncLitePropsException("Invalid value specified for license-file in configuration file");
			}
			if (!Files.exists(this.licenseFilePath)) {
				throw new SyncLitePropsException("Specified license-file does not exist : " + licenseFilePath);
			}
			if (!this.licenseFilePath.toFile().canRead()) {
				throw new SyncLitePropsException("No read permission on specified license-file path");
			}
		} else {
			//throw new SyncLitePropsException("license-file not specified in configuration file");
		}

		if (this.enableDeviceCommandHandler) {
			propValue = properties.get("device-command-root");
			if (propValue != null) {
				this.deviceCommandRoot = Path.of(propValue);
				if (this.deviceCommandRoot == null) {
					throw new SyncLitePropsException("Invalid value specified for device-command-root in configuration file");
				}
				if (!Files.exists(this.deviceCommandRoot)) {
					throw new SyncLitePropsException("Specified device-command-root path does not exist : " + this.deviceCommandRoot);
				}
				if (!this.deviceCommandRoot.toFile().canRead()) {
					throw new SyncLitePropsException("No read permission on specified device-command-root path");
				}
				if (!this.deviceCommandRoot.toFile().canWrite()) {
					throw new SyncLitePropsException("No write permission on specified device-command-root path");
				}
			} else {
				if ((this.deviceStageType == DeviceStageType.FS) || (this.deviceStageType == DeviceStageType.LOCAL_SFTP) || (this.deviceStageType == DeviceStageType.MS_ONEDRIVE) || (this.deviceStageType == DeviceStageType.GOOGLE_DRIVE) || (this.deviceStageType == DeviceStageType.LOCAL_MINIO))
				{
					throw new SyncLitePropsException("device-command-root not specified in configuration file while device command handler is enabled");
				} else if (this.deviceStageType == DeviceStageType.KAFKA){
					//For Kafka, files are downloaded and staged in the device root directory itself.
					deviceCommandRoot = deviceDataRoot;
				} else {
					//Set empty path as default
					deviceCommandRoot = Path.of("");
				}
			}
		}

		propValue = properties.get("num-device-processors");
		if (propValue != null) {
			this.numDeviceProcessors = Integer.valueOf(propValue);
			if (this.numDeviceProcessors == null) {
				throw new SyncLitePropsException("Invalid value specified for num-device-processors in configuration file");
			} else if (this.numDeviceProcessors <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for num-device-processors in configuration file");
			}
		} else {
			//TODO: Get some default based on num cores on the machine
			numDeviceProcessors = 1;
		}

		propValue = properties.get("failed-device-retry-interval-s");
		if (propValue != null) {
			this.failedDeviceRetryIntervalS = Integer.valueOf(propValue);
			if (this.failedDeviceRetryIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for failed-device-retry-interval in configuration file");
			} else if (this.failedDeviceRetryIntervalS <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for failed-device-retry-interval in configuration file");
			}
		} else {
			//TODO: Get some default based on num cores on the machine
			failedDeviceRetryIntervalS = 30;
		}

		propValue = properties.get("device-scanner-interval-s");
		if (propValue != null) {
			this.deviceScannerIntervalS = Long.valueOf(propValue);
			if (this.deviceScannerIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for device-scanner-interval in configuration file");
			} else if (this.deviceScannerIntervalS <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for device-scanner-interval in configuration file");
			}
		} else {
			this.deviceScannerIntervalS = 30L;
		}


		propValue = properties.get("device-scheduler-type");
		if (propValue != null) {
			try {
				this.deviceSchedulerType = DeviceSchedulerType.valueOf(propValue); 
				if (this.deviceSchedulerType == null) {
					throw new SyncLitePropsException("Invalid value specified for device-scheduler-type in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for device-scheduler-type in configuration file : " + propValue);
			}
		} else {
			this.deviceSchedulerType = DeviceSchedulerType.EVENT_BASED;
		}

		propValue = properties.get("device-polling-interval-ms");
		if (propValue != null) {
			this.devicePollingIntervalMs = Long.valueOf(propValue);
			if (this.devicePollingIntervalMs == null) {
				throw new SyncLitePropsException("Invalid value specified for device-polling-interval-ms in configuration file");
			} else if (this.devicePollingIntervalMs < 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for device-polling-interval-ms in configuration file");
			}
		} else {
			if (this.deviceSchedulerType == DeviceSchedulerType.EVENT_BASED) {
				this.devicePollingIntervalMs = 30000L;
			} else {
				//2 secs default for POLLING and STATIC
				this.devicePollingIntervalMs = 2000L;
			}
		}

		propValue = properties.get("device-trace-level");
		if (propValue != null) {
			this.traceLevel= TraceLevel.valueOf(propValue);
			if (this.traceLevel == null) {
				throw new SyncLitePropsException("Invalid value specified for device-trace-level in configuration file");
			}
		} else {
			traceLevel = TraceLevel.DEBUG;
		}

		propValue = properties.get("device-snapshot-interval-s");
		if (propValue != null) {
			this.deviceSnapshotIntervalS = Long.valueOf(propValue);
			if (this.deviceSnapshotIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for device-snapshot-interval in configuration file");
			} else if (this.deviceSnapshotIntervalS <= 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for device-snapshot-interval in configuration file");
			}
		} else {
			this.deviceSnapshotIntervalS=0L;
		}

		boolean deviceFilteringSpecified = false;
		propValue = properties.get("include-device-ids");
		if (propValue != null) {
			String included[]= propValue.split(",");
			this.includeDeviceIDs = Collections.newSetFromMap(new ConcurrentHashMap<>());
			for (int i=0; i < included.length; ++i) {
				this.includeDeviceIDs.add(included[i]);
			}
			if (this.includeDeviceIDs == null) {
				throw new SyncLitePropsException("Invalid value specified for include-device-ids in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			includeDeviceIDs = null;
		}

		propValue = properties.get("include-device-names");
		if (propValue != null) {
			String included[]= propValue.split(",");
			this.includeDeviceNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
			for (int i=0; i < included.length; ++i) {
				this.includeDeviceNames.add(included[i]);
			}
			if (this.includeDeviceNames == null) {
				throw new SyncLitePropsException("Invalid value specified for include-device-names in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			includeDeviceNames = null;
		}

		propValue = properties.get("exclude-device-ids");
		if (propValue != null) {
			String excluded[] = propValue.split(",");
			this.excludeDeviceIDs = Collections.newSetFromMap(new ConcurrentHashMap<>());
			for (int i=0; i < excluded.length; ++i) {
				this.excludeDeviceIDs.add(excluded[i]);
			}
			if (this.excludeDeviceIDs == null) {
				throw new SyncLitePropsException("Invalid value specified for exclude-device-ids in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			excludeDeviceIDs = null;
		}

		propValue = properties.get("exclude-device-names");
		if (propValue != null) {
			String excluded[] = propValue.split(",");
			this.excludeDeviceNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
			for (int i=0; i < excluded.length; ++i) {
				this.excludeDeviceNames.add(excluded[i]);
			}

			if (this.excludeDeviceNames == null) {
				throw new SyncLitePropsException("Invalid value specified for exclude-device-names in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			excludeDeviceNames = null;
		}

		propValue = properties.get("device-id-pattern");
		if (propValue != null) {
			if ((includeDeviceIDs != null) || (excludeDeviceIDs != null)) {
				throw new SyncLitePropsException("Cannot specify both include-device_ids/exclude-device-ids and device-id-pattern.");
			}
			this.includeDeviceIDPattern= Pattern.compile(propValue);
			if (this.includeDeviceIDPattern == null) {
				throw new SyncLitePropsException("Invalid value specified for device-id-pattern in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			includeDeviceIDPattern = null;
		}

		propValue = properties.get("device-name-pattern");
		if (propValue != null) {
			if ((includeDeviceNames != null) || (excludeDeviceNames != null)) {
				throw new SyncLitePropsException("Cannot specify both include-device_names/exclude-device-names and device-name-pattern.");
			}
			this.includeDeviceNamePattern= Pattern.compile(propValue);
			if (this.includeDeviceNamePattern == null) {
				throw new SyncLitePropsException("Invalid value specified for device-name-pattern in configuration file");
			}
			deviceFilteringSpecified = true;
		} else {
			includeDeviceNamePattern = null;
		}

		/*
		if (deviceFilteringSpecified) {
			if (this.edition == ConsolidatorEdition.DEVELOPER) {
				throw new SyncLitePropsException("Feature Not Supported : Device Filtering (and Distributed Data Consolidation) is not supported in developer edition.");
			}
		} */

		propValue = properties.get("enable-replicas-for-telemetry-devices");
		if (propValue != null) {
			try {
				this.enableReplicasForTelemetryDevices = Boolean.valueOf(propValue);
				if ( this.enableReplicasForTelemetryDevices == null) {
					throw new SyncLitePropsException("Invalid value specified for enable-replicas-for-telemetry-devices in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for enable-replicas-for-telemetry-devices in configuration file : " + propValue);
			}
		} else {
			this.enableReplicasForTelemetryDevices = false;
		}
		
		propValue = properties.get("disable-replicas-for-appender-devices");
		if (propValue != null) {
			try {
				this.disableReplicasForAppenderDevices = Boolean.valueOf(propValue);
				if ( this.disableReplicasForAppenderDevices == null) {
					throw new SyncLitePropsException("Invalid value specified for disable-replicas-for-appender-devices in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for disable-replicas-for-appender-devices in configuration file : " + propValue);
			}
		} else {
			this.disableReplicasForAppenderDevices = false;
		}

		propValue = properties.get("skip-bad-txn-files");
		if (propValue != null) {
			try {
				this.skipBadTxnFiles = Boolean.valueOf(propValue);
				if ( this.skipBadTxnFiles == null) {
					throw new SyncLitePropsException("Invalid value specified for skip-bad-txn-files in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for skip-bad-txn-files in configuration file : " + propValue);
			}
		} else {
			this.skipBadTxnFiles = false;
		}

		propValue = properties.get("gui-dashboard");
		if (propValue != null) {
			try {
				this.guiDashboard = Boolean.valueOf(propValue);
				if ( this.guiDashboard == null) {
					throw new SyncLitePropsException("Invalid value specified for gui-dashboard in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for gui-dashboard in configuration file : " + propValue);

			}
		} else {
			this.guiDashboard = true;
		}

		propValue = properties.get("update-statistics-interval-s");
		if (propValue != null) {
			this.updateStatisticsIntervalS = Long.valueOf(propValue);
			if (this.updateStatisticsIntervalS == null) {
				throw new SyncLitePropsException("Invalid value specified for update-statistics-interval-s in configuration file");
			} else if (this.updateStatisticsIntervalS < 0) {
				throw new SyncLitePropsException("Please specify a positive numeric value for update-statistics-interval-s in configuration file");
			}
		} else {
			this.updateStatisticsIntervalS = 1L;
		}

		propValue = properties.get("enable-prometheus-statistics-publisher");
		if (propValue != null) {
			try {
				this.enablePrometheusStatisticsPublisher = Boolean.valueOf(propValue);
				if ( this.enablePrometheusStatisticsPublisher == null) {
					throw new SyncLitePropsException("Invalid value specified for enable-prometheus-statistics-publisher in configuration file : " + propValue);
				}
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for enable-prometheus-statistics-publisher in configuration file : " + propValue);

			}
		} else {
			this.enablePrometheusStatisticsPublisher = false;
		}

		if (this.enablePrometheusStatisticsPublisher) {
			propValue = properties.get("prometheus-push-gateway-url");
			if (propValue != null) {
				if (propValue.isBlank()) {
					throw new SyncLitePropsException("Blank value specified for prometheus-push-gateway-url in configuration file : " + propValue);
				}			
				try {
					this.prometheusPushGatewayURL = new URL(propValue);
				} catch (Exception e) {
					throw new SyncLitePropsException("Invalid prometheus-push-gateway-url specified : " + propValue, e);
				}
			} else {
				throw new SyncLitePropsException("prometheus-push-gateway-url not specified while enable-prometheus-statistics-publisher is true, in configuration file : " + propValue);
			}


			propValue = properties.get("prometheus-statistics-publisher-interval-s");
			if (propValue != null) {
				this.prometheusStatisticsPublisherIntervalS = Long.valueOf(propValue);
				if (this.prometheusStatisticsPublisherIntervalS == null) {
					throw new SyncLitePropsException("Invalid value specified for prometheus-statistics-publisher-interval-s in configuration file");
				} else if (this.prometheusStatisticsPublisherIntervalS < 0) {
					throw new SyncLitePropsException("Please specify a positive numeric value for prometheus-statistics-publisher-interval-s in configuration file");
				}
			} else {
				this.prometheusStatisticsPublisherIntervalS = 60L;
			}

		}

		propValue = properties.get("max-num-devices");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.deviceCountLimit = Long.MAX_VALUE;
			} else {
				this.deviceCountLimit = Long.valueOf(propValue);
				if (this.deviceCountLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for max-num-devices in license file");
				} else if (this.deviceCountLimit <= 0) {
					throw new SyncLitePropsException("Invalid value specified for max-num-devices in license file");
				}
			}
		} else {
			throw new SyncLitePropsException("max-num-devices not specified in license file");
		}

		propValue = properties.get("per-device-operation-count-limit");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.perDeviceOperationCountLimit = Long.MAX_VALUE;
			} else {
				this.perDeviceOperationCountLimit = Long.valueOf(propValue);
				if (this.perDeviceOperationCountLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for per-device-operation-count-limit in license file");
				} else if (this.perDeviceOperationCountLimit <= 0) {
					throw new SyncLitePropsException("Invalid value specified for per-device-operation-count-limit in license file");
				}
			}
		} else {
			this.perDeviceOperationCountLimit = Long.MAX_VALUE;
		}

		propValue = properties.get("per-device-processed-log-size-limit");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.perDeviceProcessedLogSizeLimit = Long.MAX_VALUE;
			} else {
				this.perDeviceProcessedLogSizeLimit = Long.valueOf(propValue);
				if (this.perDeviceProcessedLogSizeLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for per-device-processed-log-size-limit in license file");
				} else if (this.perDeviceProcessedLogSizeLimit <= 0) {
					throw new SyncLitePropsException("Invalid value specified for per-device-processed-log-size-limit in license file");
				}
			}
		} else {
			this.perDeviceProcessedLogSizeLimit = Long.MAX_VALUE;
		}

		if ((this.perDeviceOperationCountLimit < Long.MAX_VALUE) || (this.perDeviceProcessedLogSizeLimit < Long.MAX_VALUE)) {
			this.checkPerDeviceLimits = true;
		} else {
			this.checkPerDeviceLimits = false;
		}

		propValue = properties.get("total-operation-count-limit");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.totalOperationCountLimit = Long.MAX_VALUE;
			} else {
				this.totalOperationCountLimit = Long.valueOf(propValue);
				if (this.totalOperationCountLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for total-operation-count-limit in license file");
				} else if (this.totalOperationCountLimit <= 0) {
					throw new SyncLitePropsException("Invalid value specified for total-operation-count-limit in license file");
				}
			}
		} else {
			this.totalOperationCountLimit = Long.MAX_VALUE;
		}

		propValue = properties.get("total-transaction-count-limit");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.totalTransactionCountLimit = Long.MAX_VALUE;
			} else {
				this.totalTransactionCountLimit = Long.valueOf(propValue);
				if (this.totalTransactionCountLimit == null) {
					throw new SyncLitePropsException("Invalid value specified for total-transaction-count-limit in license file");
				} else if (this.totalTransactionCountLimit <= 0) {
					throw new SyncLitePropsException("Invalid value specified for total-transaction-count-limit in license file");
				}
			}
		} else {
			this.totalTransactionCountLimit = Long.MAX_VALUE;
		}


		propValue = properties.get("total-processed-log-size-limit");
		if (propValue != null) {
			if (propValue.equals("UNLIMITED")) {
				this.totalProcessedLogSizeLimit = Long.MAX_VALUE;
			} else {
				try {
					this.totalProcessedLogSizeLimit = getBytes(propValue);
					if (this.totalProcessedLogSizeLimit == -1) {
						throw new SyncLitePropsException("Invalid value specified for total-processed-log-size-limit in license file");
					} else if (this.totalProcessedLogSizeLimit <= 0) {
						throw new SyncLitePropsException("Invalid value specified for total-processed-log-size-limit in license file");
					}
				} catch (IllegalArgumentException e) {
					throw new SyncLitePropsException("Invalid value specified for total-processed-log-size-limit in license file");
				}
			}
		} else {
			this.totalProcessedLogSizeLimit = Long.MAX_VALUE;
		}

		if ((this.totalOperationCountLimit < Long.MAX_VALUE) || (this.totalProcessedLogSizeLimit < Long.MAX_VALUE) || (this.totalTransactionCountLimit < Long.MAX_VALUE)) {
			this.checkGlobalLimits = true;
		} else {
			this.checkGlobalLimits = false;
		}

		propValue = properties.get("allowed-destinations");
		this.allowedDestinations = new HashSet<DstType>();
		if (propValue != null) {
			String allowedDsts[]= propValue.split(",");
			DstType allowedDst;
			for (String allowedDstName : allowedDsts) {
				try {
					allowedDst = DstType.valueOf(allowedDstName);
					if (allowedDst == null) {
						//throw new SyncLitePropsException("Invalid value specified in allowed-destinations in license file : " + allowedDstName);
						//Ignore unsupported dst type
					}
					allowedDestinations.add(allowedDst);
				} catch (IllegalArgumentException e) {
					//throw new SyncLitePropsException("Invalid value specified in allowed-destinations in license file : " + allowedDstName);
					//Ignore unsupported dst type from here.
				}
			}
		} else {
			throw new SyncLitePropsException("allowed-destinations not specified in license file");
		}

		if (!allowedDestinations.contains(DstType.ALL)) {
			for (int dstIndex = 1 ; dstIndex <= numDestinations; ++dstIndex) {
				if (!allowedDestinations.contains(dstType[dstIndex])) {
					throw new SyncLitePropsException("Specified destinaton " + dstType[dstIndex] + " not allowed with your license");
				}				
			}
		}        

		//Set scheduler to POLLING based on stage type
		if ((this.deviceStageType == DeviceStageType.REMOTE_MINIO) || (this.deviceStageType == DeviceStageType.REMOTE_SFTP) || (this.deviceStageType == DeviceStageType.KAFKA) || (this.deviceStageType == DeviceStageType.S3)) {
			this.deviceSchedulerType = DeviceSchedulerType.POLLING;
		}
	}

	private void parseValueMappingsFile(Path valueMappingsFile, int dstIndex) throws SyncLitePropsException {
		this.dstValueMappings[dstIndex] = new HashMap<String, HashMap<String, HashMap<String, String>>>();

		try {
			String valueMappingsStr = Files.readString(valueMappingsFile);
			// Parse and populate the HashMap
			JSONObject jsonObject = new JSONObject(valueMappingsStr);
			JSONArray tablesArray = jsonObject.getJSONArray("tables");

			for (int i = 0; i < tablesArray.length(); i++) {
				JSONObject tableObject = tablesArray.getJSONObject(i);
				String srcTableName = tableObject.getString("src_table_name");
				if (!isAllowedTable(dstIndex, srcTableName)) {
					continue;
				}
				HashMap<String, HashMap<String, String>> tableData = new HashMap<>();
				JSONArray columnsArray = tableObject.getJSONArray("columns");

				for (int j = 0; j < columnsArray.length(); j++) {
					JSONObject columnObject = columnsArray.getJSONObject(j);
					String srcColumnName = columnObject.getString("src_column_name");
					if (!isAllowedColumn(dstIndex, srcTableName, srcColumnName)) {
						continue;
					}
					HashMap<String, String> valueMappings = new HashMap<>();
					JSONObject valueMappingsObject = columnObject.getJSONObject("value_mappings");

					for (String key : valueMappingsObject.keySet()) {
						valueMappings.put(key, valueMappingsObject.getString(key));
					}

					tableData.put(srcColumnName.toUpperCase(), valueMappings);
				}
				dstValueMappings[dstIndex].put(srcTableName.toUpperCase(), tableData);
			}
		} catch (Exception e) {
			throw new SyncLitePropsException("Failed to parse value mappings JSON file: " + valueMappingsFile, e);
		}
	}

	private void parseTriggersFile(Path triggersFile, int dstIndex) throws SyncLitePropsException {
		this.dstTriggers[dstIndex] = new HashMap<String, List<String>>();

		try {
			String triggersStr = Files.readString(triggersFile);
			// Parse and populate the HashMap
			JSONObject jsonObject = new JSONObject(triggersStr);
			JSONArray tablesArray = jsonObject.getJSONArray("tables");

			for (int i = 0; i < tablesArray.length(); i++) {
				JSONObject tableObject = tablesArray.getJSONObject(i);
				String dstTableName = tableObject.getString("dst_table_name");
				List<String> tableData = new ArrayList<>();
				JSONArray triggersArray = tableObject.getJSONArray("trigger_statements");

				for (int j = 0; j < triggersArray.length(); j++) {
					String triggerStmt = triggersArray.getString(j);
					tableData.add(triggerStmt);
				}
				this.dstTriggers[dstIndex].put(dstTableName.toUpperCase(), tableData);
			}
		} catch (Exception e) {
			throw new SyncLitePropsException("Failed to parse triggers JSON file: " + triggersFile, e);
		}
	}
	
	private void parseFilterMapperRulesFile(Path filterMapperRulesFile, int dstIndex) throws SyncLitePropsException {		
		this.dstTableFilterMapperRules[dstIndex] = new HashMap<String, String>();
		this.dstTableToSrcTableMap[dstIndex] = new HashMap<String, String>();
		this.dstColumnFilterMapperRules[dstIndex] = new HashMap<String, HashMap<String, String>>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filterMapperRulesFile.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					throw new SyncLitePropsException("Invalid line in config file " + filterMapperRulesFile + " : " + line);
				}

				String key = tokens[0].trim().toUpperCase();
				String val = line.substring(line.indexOf("=") + 1, line.length()).trim();

				String keyTokens[] = key.split("\\.");
				if (keyTokens.length == 1) {
					//Table rule
					this.dstTableFilterMapperRules[dstIndex].put(key, val);
					this.dstTableToSrcTableMap[dstIndex].put(val.toUpperCase(), key);
				} else if(keyTokens.length == 2) {
					HashMap<String, String> colRules = this.dstColumnFilterMapperRules[dstIndex].get(keyTokens[0]);
					if (colRules == null) {
						colRules = new HashMap<String, String>();
						colRules.put(keyTokens[1].toUpperCase(), val);
						this.dstColumnFilterMapperRules[dstIndex].put(keyTokens[0], colRules);
					} else {
						colRules.put(keyTokens[1].toUpperCase(), val);
					}
				} else {
					throw new SyncLitePropsException("Invalid table/column name specified in : " + filterMapperRulesFile + " : " + key + " in rule : " + line);
				}
				line = reader.readLine();
			}
			//Always allow synclite_metadata table
			this.dstTableFilterMapperRules[dstIndex].put(SyncLiteConsolidatorInfo.getSyncLiteMetadataTableName().toUpperCase(), "true");
		} catch (IOException e) {
			throw new SyncLitePropsException("Failed to load configuration file : " + filterMapperRulesFile + " : ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new SyncLitePropsException("Failed to close configuration file : " + filterMapperRulesFile + ": " , e);
				}
			}
		}
	}


	private long getBytes(String size) throws IllegalArgumentException{
		long bytes = -1;

		size = size.trim().toUpperCase();

		if (size.matches("\\d+(\\.\\d+)?\\s*(B|BYTE(S)?)?")) {
			bytes = (long) parseBytes(size);
		} else if (size.matches("\\d+(\\.\\d+)?\\s*KB")) {
			bytes = (long) (parseBytes(size) * 1024);
		} else if (size.matches("\\d+(\\.\\d+)?\\s*MB")) {
			bytes = (long) (parseBytes(size) * 1024 * 1024);
		} else if (size.matches("\\d+(\\.\\d+)?\\s*GB")) {
			bytes = (long) (parseBytes(size) * 1024 * 1024 * 1024);
		} else if (size.matches("\\d+(\\.\\d+)?\\s*TB")) {
			bytes = (long) (parseBytes(size) * 1024 * 1024 * 1024 * 1024);
		}
		return bytes;
	}

	private double parseBytes(String size) throws IllegalArgumentException {
		String[] parts = size.split("\\s+");
		double value = Double.parseDouble(parts[0]);
		return value;
	}

	private final String getDstName(DstType dType, DstDataLakeDataFormat dstDataLakeFormat) {
		switch (dType) {
		case DUCKDB:
			return "DuckDB";
		case MONGODB:
			return "MongoDB";		
		case MYSQL:
			return "MySQL";
		case POSTGRESQL:
			return "PostgreSQL";
		case SQLITE:
			return "SQLite";
		default:
			return dType.toString();
		}
	}

	public final boolean isAllowedDevice(DeviceIdentifier deviceIdentifier) {
		if (excludeDeviceIDs !=  null) {
			if (excludeDeviceIDs.contains(deviceIdentifier.uuid)) {
				return false;
			}
		}
		if ((excludeDeviceNames != null) && (!deviceIdentifier.name.equals(""))) {
			if (excludeDeviceNames.contains(deviceIdentifier.name)) {
				return false;
			}
		}

		if (includeDeviceIDPattern != null) {
			if (!includeDeviceIDPattern.matcher(deviceIdentifier.uuid).matches()) {
				return false;
			}
		}

		if ((includeDeviceNamePattern != null) && (!deviceIdentifier.name.equals(""))) {
			if (!includeDeviceNamePattern.matcher(deviceIdentifier.name).matches()) {
				return false;
			}
		}

		if (includeDeviceIDs != null) {
			if (includeDeviceIDs.contains(deviceIdentifier.uuid)) {
				return true;
			} else {
				return false;
			}
		}

		if ((includeDeviceNames != null) && (!deviceIdentifier.name.equals(""))) {
			if (includeDeviceNames.contains(deviceIdentifier.name)) {
				return true;
			} else {
				return false;
			}
		}

		syncLiteOperRetryCount = 10L;
		syncLiteOperRetryIntervalMs = 5000L;

		return true;
	}


	private void validateAndProcessManageDevicesProperties() throws SyncLitePropsException {
		manageDevicesNameList = properties.get("manage-devices-name-list");

		String propValue = properties.get("manage-devices-name-pattern");
		if (propValue != null) {
			this.manageDevicesNamePattern= Pattern.compile(propValue);
			if (this.manageDevicesNamePattern == null) {
				throw new SyncLitePropsException("Invalid value specified for manage-devices-name-pattern in configuration file");
			}
		} else {
			manageDevicesNamePattern = null;
		}

		manageDevicesIDList = properties.get("manage-devices-id-list");

		propValue = properties.get("manage-devices-id-pattern");
		if (propValue != null) {
			if (manageDevicesNamePattern != null) {
				throw new SyncLitePropsException("Both manage-devices-name-pattern and manage-devices-id-pattern specified in the configuration file. Only one of the two is allowed to be specified.");
			}
			this.manageDevicesIDPattern= Pattern.compile(propValue);
			if (this.manageDevicesIDPattern == null) {
				throw new SyncLitePropsException("Invalid value specified for manage-devices-id-pattern in configuration file");
			}
		} else {
			manageDevicesIDPattern = null;
		}

		if ((manageDevicesNameList == null) && (manageDevicesNamePattern == null) && (manageDevicesIDPattern == null) && (manageDevicesIDList == null)) {
			throw new SyncLitePropsException("None of the four : manage-devices-name-list, manage-devices-name-pattern, manage-devices-id-list and manage-devices-id-pattern specified in the configuration file. One of them must be specified.");
		}

		propValue = properties.get("remove-devices-from-dst");
		if (propValue != null) {
			try {
				this.removeDevicesFromDst = Boolean.valueOf(propValue);
				if (this.removeDevicesFromDst == null) {
					throw new SyncLitePropsException("Invalid value specified for remove-devices-from-dst in the configuration file : " + propValue);
				} 
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for remove-devices-from-dst in the configuration file : " + propValue);
			}
		} else {
			this.removeDevicesFromDst = false;
		}

		propValue = properties.get("manage-devices-operation-type");
		if (propValue != null) {
			try {
				this.manageDevicesOperationType = ManageDevicesOperationType.valueOf(propValue);
				if (this.manageDevicesOperationType == null) {
					throw new SyncLitePropsException("Invalid value specified for manage-devices-operation-type in the configuration file : " + propValue);
				} 
			} catch (IllegalArgumentException e) {
				throw new SyncLitePropsException("Invalid value specified for manage-devices-operation-type in the configuration file : " + propValue);
			}
		} else {
			throw new SyncLitePropsException("manage-devices-operation-type not specified in in the manage devices configuration file");
		}

		this.deviceCommand = properties.get("device-command");
	}

	private static final class InstanceHolder {
		private static ConfLoader INSTANCE = new ConfLoader();
	}

	public static ConfLoader getInstance() {
		return InstanceHolder.INSTANCE;
	}

	public boolean tableHasValueMappings(int dstIndex, String srcTableName) {
		if (this.dstEnableValueMapper[dstIndex] == false) {
			return false;			
		} else if (this.dstValueMappings[dstIndex] == null) {
			return false;
		} else if (this.dstValueMappings[dstIndex].get(srcTableName.toUpperCase()) == null) {
			return false;
		}
		return true;
	}

	public boolean tableHasFilterMapperRules(int dstIndex, String tableName) {
		if (dstColumnFilterMapperRules == null) {
			return false;
		}
		if (dstColumnFilterMapperRules[dstIndex] == null) {
			return false;
		}
		if (dstColumnFilterMapperRules[dstIndex].get(tableName.toUpperCase()) == null) {
			return false;
		}
		return true;
	}

	public boolean tableHasTriggers(int dstIndex, String dstTableName) {
		if (this.dstEnableTriggers[dstIndex] == false) {
			return false;			
		} else if (this.dstTriggers[dstIndex] == null) {
			return false;
		} else if (this.dstTriggers[dstIndex].get(dstTableName.toUpperCase()) == null) {
			return false;
		} else if (this.dstTriggers[dstIndex].get(dstTableName.toUpperCase()).size() == 0) {
			return false;
		}
		return true;
	}

	public List<String> getTriggers(int dstIndex, String dstTableName) {
		if (this.dstEnableTriggers[dstIndex] == false) {
			return null;			
		} else if (this.dstTriggers[dstIndex] == null) {
			return null;
		} else if (this.dstTriggers[dstIndex].get(dstTableName.toUpperCase()) == null) {
			return null;
		} else if (this.dstTriggers[dstIndex].get(dstTableName.toUpperCase()).size() == 0) {
			return null;
		}
		return this.dstTriggers[dstIndex].get(dstTableName.toUpperCase());		
	}
	
	public boolean triggersEnabled(int dstIndex) {
		if (this.dstEnableTriggers[dstIndex] == false) {
			return false;			
		} else if (this.dstTriggers[dstIndex] == null) {
			return false;
		} 
		return true;		
	}
}
