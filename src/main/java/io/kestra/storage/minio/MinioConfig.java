package io.kestra.storage.minio;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.storage.minio.domains.ProxyConfiguration;
import io.kestra.storage.minio.domains.SslOptions;
import io.kestra.storage.minio.internal.BytesSize;

public interface MinioConfig {

    @PluginProperty(group = "connection")
    String getEndpoint();

    @PluginProperty(group = "connection")
    int getPort();

    @PluginProperty(group = "connection")
    String getAccessKey();

    @PluginProperty(group = "connection")
    String getSecretKey();

    @PluginProperty(group = "connection")
    String getRegion();

    @PluginProperty
    boolean isSecure();

    @PluginProperty(group = "connection")
    String getBucket();

    @PluginProperty
    boolean isVhost();

    @PluginProperty(group = "advanced")
    BytesSize getPartSize();

    @PluginProperty(group = "advanced")
    ProxyConfiguration getProxyConfiguration();

    @PluginProperty(group = "advanced")
    String getCaPem();

    @PluginProperty(group = "advanced")
    String getClientPem();

    @PluginProperty(group = "connection")
    SslOptions getSslOptions();
}
