package io.kestra.storage.minio;


import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.storage.minio.domains.ProxyConfiguration;
import io.kestra.storage.minio.domains.SslOptions;
import io.kestra.storage.minio.internal.BytesSize;

public interface MinioConfig {

    @PluginProperty
    String getEndpoint();

    @PluginProperty
    int getPort();

    @PluginProperty
    String getAccessKey();

    @PluginProperty
    String getSecretKey();

    @PluginProperty
    String getRegion();

    @PluginProperty
    boolean isSecure();

    @PluginProperty
    String getBucket();

    @PluginProperty
    boolean isVhost();

    @PluginProperty
    BytesSize getPartSize();

    @PluginProperty
    ProxyConfiguration getProxyConfiguration();

    @PluginProperty
    String getCaPem();

    @PluginProperty
    String getClientPem();

    @PluginProperty
    SslOptions getSslOptions();
}
