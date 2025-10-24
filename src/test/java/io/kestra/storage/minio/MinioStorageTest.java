package io.kestra.storage.minio;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinioStorageTest extends StorageTestSuite {

    @Inject
    StorageInterface storage;

    @BeforeEach
    void init() throws Exception {
        String bucket = ((MinioStorage) storage).getBucket();
        if (!((MinioStorage) storage).minioClient().bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
            ((MinioStorage) storage).minioClient().makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        }
    }

    @Test
    void checkVhostOff() throws Exception {
        ByteArrayOutputStream traceStream = new ByteArrayOutputStream();
        ((MinioStorage) storage).minioClient().traceOn(traceStream);
        try {
            storage.list(MAIN_TENANT, null, URI.create("/"));
            assertThat(traceStream.toString(), containsString("Host: " + ((MinioStorage) storage).getEndpoint()));
        } finally {
            ((MinioStorage) storage).minioClient().traceOff();
        }
    }

    @Test
    void putLongObjectName() throws URISyntaxException, IOException {
        String longObjectName = "/" + RandomStringUtils.insecure().nextAlphanumeric(260).toLowerCase();

        URI put = storageInterface.put(
            IdUtils.create(),
            null,
            new URI(longObjectName),
            new ByteArrayInputStream("Hello World".getBytes())
        );

        assertThat(put.getPath(), not(longObjectName));
        String suffix = put.getPath().substring(7); // we remove the random 5 char + '-'
        assertTrue(longObjectName.endsWith(suffix));
    }
}
