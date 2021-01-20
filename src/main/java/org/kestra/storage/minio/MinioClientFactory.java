package org.kestra.storage.minio;

import io.micronaut.context.annotation.Factory;
import io.minio.MinioClient;

import javax.inject.Singleton;

@Factory
@MinioStorageEnabled
public class MinioClientFactory {
    @Singleton
    public MinioClient of(MinioConfig config) {
        MinioClient client;
        
        try {
            client = MinioClient.builder()
                .endpoint(config.getEndpoint(), config.getPort(), config.isSecure())
                .credentials(config.getAccessKey(), config.getSecretKey())
                .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        return client;
    }
}
