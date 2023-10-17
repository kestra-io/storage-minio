package io.kestra.storage.minio;

import io.kestra.core.storage.StorageTestSuite;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;

@MicronautTest
class MinioStorageTest extends StorageTestSuite {
    @Inject
    MinioClientFactory clientFactory;

    @Inject
    MinioConfig config;

    @BeforeEach
    void init() throws Exception {
        MinioClient client = clientFactory.of(this.config);

        if (!client.bucketExists(BucketExistsArgs.builder().bucket(config.getBucket()).build())) {
            client.makeBucket(MakeBucketArgs.builder().bucket(config.getBucket()).build());
        }
    }
}
