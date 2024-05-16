package io.kestra.storage.minio;

import io.minio.MinioClient;
import io.minio.credentials.AwsConfigProvider;
import io.minio.credentials.AwsEnvironmentProvider;
import io.minio.credentials.ChainedProvider;
import io.minio.credentials.IamAwsProvider;
import io.minio.credentials.MinioEnvironmentProvider;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import org.jetbrains.annotations.NotNull;

public class MinioClientFactory {

    public static MinioClient of(final MinioConfig config) {
        try {
            MinioClient.Builder bdr;
            bdr = MinioClient.builder()
                .endpoint(config.getEndpoint(), config.getPort(), config.isSecure());

            if (config.getRegion() != null) {
                bdr.region(config.getRegion());
            }
            
            bdr.credentialsProvider(getCredentialProvider(config));

            MinioClient build = bdr.build();

            if (config.isVhost()) {
                build.enableVirtualStyleEndpoint();
            }

            return build;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    public static Provider getCredentialProvider(final MinioConfig config) {
        Provider provider;
        if (config.getAccessKey() != null && config.getSecretKey() != null) {
            provider = new StaticProvider(config.getAccessKey(), config.getSecretKey(), null);
        } else {
            // Otherwise, by default configure the credential chains.
            provider = new ChainedProvider(
                new AwsEnvironmentProvider(),
                new MinioEnvironmentProvider(),
                new AwsConfigProvider(null, null),
                new IamAwsProvider(null, null)
            );
        }
        return provider;
    }
}
