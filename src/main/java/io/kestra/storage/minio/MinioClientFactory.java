package io.kestra.storage.minio;

import io.micronaut.context.annotation.Factory;
import io.minio.MinioClient;

import jakarta.inject.Singleton;

@Factory
@MinioStorageEnabled
public class MinioClientFactory {
    @Singleton
    public MinioClient of(MinioConfig config) {
        try {
            MinioClient.Builder bdr;
            bdr = MinioClient.builder()
                    .endpoint(config.getEndpoint(), config.getPort(), config.isSecure());
            if (config.region != null) {
                bdr.region(config.getRegion());
            }
            if (config.accessKey != null && config.secretKey != null) {
                bdr.credentials(config.getAccessKey(), config.getSecretKey());
            }

            MinioClient build = bdr.build();
            if (config.vhost) {
                build.enableVirtualStyleEndpoint();
            }

            return build;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
