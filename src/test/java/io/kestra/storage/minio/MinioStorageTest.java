package io.kestra.storage.minio;

import io.kestra.core.storage.StorageTestSuite;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

class MinioStorageTest extends StorageTestSuite {
    @Inject
    MinioClient client;

    @Inject
    MinioConfig config;

    @Inject
    MinioStorage minioStorage;

    @BeforeEach
    void init() throws Exception {
        if (!client.bucketExists(BucketExistsArgs.builder().bucket(config.getBucket()).build())) {
            client.makeBucket(MakeBucketArgs.builder().bucket(config.getBucket()).build());
        }
    }

    @Test
    void checkVhostOff() throws Exception {
        ByteArrayOutputStream traceStream = new ByteArrayOutputStream();
        client.traceOn(traceStream);
        try {
            minioStorage.list(null, URI.create("/"));

            assertThat(traceStream.toString(), containsString("Host: " + config.endpoint));
        } finally {
            client.traceOff();
        }
    }
}
