package com.synclite.consolidator.stage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

class S3StageManagerTest {
    private S3StageManager manager;
    private final List<String> deletedKeys = new ArrayList<>();
    private S3Client client;
    private Object previousRetries;
    private Object previousInterval;
    private Object previousEncryption;
    private boolean failDelete;

    @BeforeEach
    void configureClient() throws Exception {
        previousRetries = setField(ConfLoader.class, ConfLoader.getInstance(), "failedStageOperRetryCount", 1L);
        previousInterval = setField(ConfLoader.class, ConfLoader.getInstance(), "failedStageOperRetryIntervalMs", 0L);
        previousEncryption = setField(ConfLoader.class, ConfLoader.getInstance(), "deviceEncryptionEnabled", false);
        manager = new S3StageManager();
        client = (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[] { S3Client.class },
                (proxy, method, arguments) -> invoke(method.getName(), arguments));
        setField(S3StageManager.class, manager, "s3Client", client);
        setField(S3StageManager.class, manager, "bucketName", "archive");
        manager.tracer = Logger.getLogger(S3StageManagerTest.class);
    }

    @AfterEach
    void restoreConfiguration() throws Exception {
        setField(ConfLoader.class, ConfLoader.getInstance(), "failedStageOperRetryCount", previousRetries);
        setField(ConfLoader.class, ConfLoader.getInstance(), "failedStageOperRetryIntervalMs", previousInterval);
        setField(ConfLoader.class, ConfLoader.getInstance(), "deviceEncryptionEnabled", previousEncryption);
        if (client != null) client.close();
    }

    @Test
    void deletesAllPagesFromConfiguredBucket() throws Exception {
        manager.deleteContainer(Path.of("device"));
        assertTrue(deletedKeys.containsAll(List.of("device/first", "device/second", "device/")));
        assertTrue(deletedKeys.stream().allMatch(key -> key.startsWith("device/")));
    }

    @Test
    void partialDeletionErrorsRemainFailures() {
        failDelete = true;
        assertThrows(SyncLiteStageException.class, () -> manager.deleteContainer(Path.of("device")));
        assertFalse(deletedKeys.contains("device/"));
    }

    @Test
    void preservesDownloadTimestampsAndMissingObjectSemantics(@TempDir Path directory) throws Exception {
        Path output = directory.resolve("download");
        assertEquals(Instant.parse("2026-11-03T00:00:00Z").toEpochMilli(),
                manager.doDownloadObject(Path.of("device", "first"), output, SyncLiteObjectType.DATA));
        assertArrayEquals(new byte[] { 1, 2, 3 }, Files.readAllBytes(output));
        assertEquals(0, manager.doDownloadObject(Path.of("device", "missing"), output, SyncLiteObjectType.DATA));
        assertTrue(manager.containerExists(Path.of("device"), SyncLiteObjectType.DATA_CONTAINER));
        assertFalse(manager.containerExists(Path.of("missing"), SyncLiteObjectType.DATA_CONTAINER));
    }

    @SuppressWarnings("unchecked")
    private Object invoke(String method, Object[] arguments) {
        switch (method) {
            case "listObjectsV2Paginator": {
                ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
                ((Consumer<ListObjectsV2Request.Builder>) arguments[0]).accept(builder);
                assertEquals("archive", builder.build().bucket());
                assertEquals("device/", builder.build().prefix());
                return new ListObjectsV2Iterable(client, builder.build());
            }
            case "listObjectsV2": {
                ListObjectsV2Request request = (ListObjectsV2Request) arguments[0];
                boolean secondPage = request.continuationToken() != null;
                return ListObjectsV2Response.builder().isTruncated(!secondPage)
                        .nextContinuationToken(secondPage ? null : "page-2")
                        .contents(S3Object.builder().key(secondPage ? "device/second" : "device/first").build()).build();
            }
            case "deleteObjects": {
                DeleteObjectsRequest.Builder builder = DeleteObjectsRequest.builder();
                ((Consumer<DeleteObjectsRequest.Builder>) arguments[0]).accept(builder);
                DeleteObjectsRequest request = builder.build();
                assertEquals("archive", request.bucket());
                if (failDelete) {
                    return DeleteObjectsResponse.builder().errors(S3Error.builder().key("device/first").code("AccessDenied").build()).build();
                }
                request.delete().objects().forEach(object -> deletedKeys.add(object.key()));
                return DeleteObjectsResponse.builder().build();
            }
            case "deleteObject": {
                DeleteObjectRequest.Builder builder = DeleteObjectRequest.builder();
                ((Consumer<DeleteObjectRequest.Builder>) arguments[0]).accept(builder);
                assertEquals("archive", builder.build().bucket());
                deletedKeys.add(builder.build().key());
                return DeleteObjectResponse.builder().build();
            }
            case "headObject": {
                HeadObjectRequest.Builder builder = HeadObjectRequest.builder();
                ((Consumer<HeadObjectRequest.Builder>) arguments[0]).accept(builder);
                assertEquals("archive", builder.build().bucket());
                if (builder.build().key().startsWith("missing")) throw missingObject();
                return HeadObjectResponse.builder().build();
            }
            case "getObject": {
                GetObjectRequest.Builder builder = GetObjectRequest.builder();
                ((Consumer<GetObjectRequest.Builder>) arguments[0]).accept(builder);
                assertEquals("archive", builder.build().bucket());
                if (builder.build().key().endsWith("missing")) throw missingObject();
                return new ResponseInputStream<>(GetObjectResponse.builder().lastModified(Instant.parse("2026-11-03T00:00:00Z")).build(),
                        new ByteArrayInputStream(new byte[] { 1, 2, 3 }));
            }
            case "close": return null;
            default: throw new UnsupportedOperationException(method);
        }
    }

    private S3Exception missingObject() {
        return (S3Exception) S3Exception.builder().statusCode(404)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build()).build();
    }

    private Object setField(Class<?> owner, Object target, String name, Object value) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        Object previous = field.get(target);
        field.set(target, value);
        return previous;
    }
}