package io.kestra.storage.minio;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.ReadableBytes;
import lombok.Getter;

import jakarta.inject.Singleton;

@Singleton
@Getter
@ConfigurationProperties("kestra.storage.minio")
public class MinioConfig {
    String endpoint;

    int port;

    String accessKey;

    String secretKey;

    String region;

    boolean secure;

    String bucket;

    @ReadableBytes
    long partSize = 1024*1024*5;
}
