package io.kestra.storage.minio;

import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Property;
import io.kestra.core.junit.annotations.KestraTest;
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

@KestraTest
@Property(name = "kestra.storage.minio.vhost", value = "true")
class MinioStorageVhostTest {

    @Inject
    StorageInterface storage;
    @BeforeEach
    void init() throws Exception {
        MinioClient minioClient = ((MinioStorage) storage).miniClient();
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(((MinioStorage) storage).getBucket()).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(((MinioStorage) storage).getBucket()).build());
        }
    }

    @Test
    void checkVhostOn() throws Exception {
        ByteArrayOutputStream traceStream = new ByteArrayOutputStream();
        MinioClient minioClient = ((MinioStorage) storage).miniClient();
        minioClient.traceOn(traceStream);
        try {
            storage.list(null, URI.create("/"));
            assertThat(traceStream.toString(), containsString("Host: " + ((MinioStorage) storage).getBucket() + "." + ((MinioStorage) storage).getEndpoint()));
        } finally {
            minioClient.traceOff();
        }
    }
}
