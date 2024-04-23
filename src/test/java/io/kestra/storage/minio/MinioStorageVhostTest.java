package io.kestra.storage.minio;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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

@MicronautTest
@Property(name = "kestra.storage.minio.vhost", value = "true")
class MinioStorageVhostTest {
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
    void checkVhostOn() throws Exception {
        ByteArrayOutputStream traceStream = new ByteArrayOutputStream();
        client.traceOn(traceStream);
        try {
            minioStorage.list(null, URI.create("/"));

            assertThat(traceStream.toString(), containsString("Host: " + config.bucket + "." + config.endpoint));
        } finally {
            client.traceOff();
        }
    }
}
