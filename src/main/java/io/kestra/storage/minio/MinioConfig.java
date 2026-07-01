package io.kestra.storage.minio;

import java.time.Duration;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.storage.minio.domains.ProxyConfiguration;
import io.kestra.storage.minio.domains.SslOptions;
import io.kestra.storage.minio.internal.BytesSize;

import jakarta.annotation.Nullable;

public interface MinioConfig {

    @PluginProperty(group = "connection")
    String getEndpoint();

    @PluginProperty(group = "connection")
    int getPort();

    @PluginProperty(group = "connection", secret = true)
    String getAccessKey();

    @PluginProperty(group = "connection", secret = true)
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

    /**
     * Maximum time to wait while establishing an HTTP connection to MinIO.
     * Set to {@code PT0S} (zero) to disable the timeout (infinite wait).
     * When {@code null}, OkHttp's default of 10 seconds is used.
     */
    @Nullable
    @PluginProperty(group = "advanced")
    Duration getHttpConnectTimeout();

    /**
     * Maximum time to wait for data between consecutive TCP packets during an HTTP read from MinIO.
     * Set to {@code PT0S} (zero) to disable the timeout (infinite wait), which avoids
     * {@code SocketException} on long-running {@code listObjects} streams over large buckets.
     * When {@code null}, OkHttp's default of 10 seconds is used.
     */
    @Nullable
    @PluginProperty(group = "advanced")
    Duration getHttpReadTimeout();

    /**
     * Maximum time to wait for data between consecutive TCP packets during an HTTP write to MinIO.
     * Set to {@code PT0S} (zero) to disable the timeout (infinite wait).
     * When {@code null}, OkHttp's default of 10 seconds is used.
     */
    @Nullable
    @PluginProperty(group = "advanced")
    Duration getHttpWriteTimeout();

    /**
     * Keep-alive duration for idle HTTP connections in the OkHttp connection pool.
     * Connections idle longer than this are evicted before reuse, preventing
     * {@code SocketException: Socket closed} when network infrastructure (Kubernetes,
     * Istio, load balancers) closes TCP connections after a shorter idle period.
     * Defaults to {@code PT30S} (30 seconds) when {@code null}.
     */
    @Nullable
    @PluginProperty(group = "advanced")
    Duration getHttpConnectionKeepAlive();
}
