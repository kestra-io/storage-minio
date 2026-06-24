package io.kestra.storage.minio;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import org.apache.hc.core5.ssl.SSLContexts;
import org.jetbrains.annotations.NotNull;

import io.kestra.storage.minio.domains.ProxyConfiguration;

import io.minio.MinioClient;
import io.minio.credentials.AwsConfigProvider;
import io.minio.credentials.AwsEnvironmentProvider;
import io.minio.credentials.ChainedProvider;
import io.minio.credentials.IamAwsProvider;
import io.minio.credentials.MinioEnvironmentProvider;
import io.minio.credentials.Provider;
import io.minio.credentials.StaticProvider;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

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

            bdr.httpClient(buildHttpClientWithTls(config));

            MinioClient build = bdr.build();

            if (config.isVhost()) {
                build.enableVirtualStyleEndpoint();
            }

            return build;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static OkHttpClient createHttpClient(MinioConfig config) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        ProxyConfiguration proxyConf = config.getProxyConfiguration();
        if (proxyConf != null && proxyConf.getType() != Proxy.Type.DIRECT) {
            Proxy proxy = new Proxy(
                proxyConf.getType(),
                new InetSocketAddress(proxyConf.getAddress(), proxyConf.getPort())
            );
            builder.proxy(proxy);

            if (proxyConf.getUsername() != null && proxyConf.getPassword() != null) {
                builder.proxyAuthenticator((route, response) ->
                {
                    String credential = okhttp3.Credentials.basic(proxyConf.getUsername(), proxyConf.getPassword());
                    return response.request().newBuilder()
                        .header("Proxy-Authorization", credential)
                        .build();
                });
            }
        }

        if (config.getHttpConnectTimeout() != null) {
            builder.connectTimeout(config.getHttpConnectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }
        if (config.getHttpReadTimeout() != null) {
            builder.readTimeout(config.getHttpReadTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }
        if (config.getHttpWriteTimeout() != null) {
            builder.writeTimeout(config.getHttpWriteTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }

        // Default 30s is intentionally shorter than OkHttp's 5-min default to stay below
        // typical k8s/Istio idle-connection kill timers (~10–60s), preventing stale-connection SocketException.
        var keepAlive = config.getHttpConnectionKeepAlive();
        long keepAliveMs = keepAlive != null ? keepAlive.toMillis() : 30_000L;
        builder.connectionPool(new ConnectionPool(5, keepAliveMs, TimeUnit.MILLISECONDS));

        return builder.build();
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

    private static OkHttpClient buildHttpClientWithTls(MinioConfig config) throws Exception {
        OkHttpClient.Builder builder = createHttpClient(config).newBuilder();

        if (config.getSslOptions() != null && config.getSslOptions().getInsecureTrustAllCertificates().equals(Boolean.TRUE)) {
            SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(null, (chain, authType) -> true)
                .build();

            builder.sslSocketFactory(sslContext.getSocketFactory(), CustomTrustManager.INSTANCE);
            builder.hostnameVerifier((h, s) -> true);

        } else if (config.getClientPem() != null || config.getCaPem() != null) {
            OkHttpClient tlsClient = MinioHttpClientUtils.withPemCertificate(
                config.getClientPem() != null
                    ? new ByteArrayInputStream(config.getClientPem().getBytes(StandardCharsets.UTF_8))
                    : null,
                config.getCaPem() != null
                    ? new ByteArrayInputStream(config.getCaPem().getBytes(StandardCharsets.UTF_8))
                    : null
            );

            builder.sslSocketFactory(tlsClient.sslSocketFactory(), CustomTrustManager.INSTANCE);
        }

        return builder.build();
    }

    private static class CustomTrustManager implements X509TrustManager {
        static final CustomTrustManager INSTANCE = new CustomTrustManager();

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
