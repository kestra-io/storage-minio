package io.kestra.storage.minio;

import io.minio.MinioClient;

public class MinioClientFactory {

    public static MinioClient of(final MinioConfig config) {
        try {
            MinioClient.Builder bdr;
            bdr = MinioClient.builder()
                    .endpoint(config.getEndpoint(), config.getPort(), config.isSecure());
            if (config.getRegion() != null) {
                bdr.region(config.getRegion());
            }
            if (config.getAccessKey() != null && config.getSecretKey() != null) {
                bdr.credentials(config.getAccessKey(), config.getSecretKey());
            }

            MinioClient build = bdr.build();
            if (config.isVhost()) {
                build.enableVirtualStyleEndpoint();
            }

            return build;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
