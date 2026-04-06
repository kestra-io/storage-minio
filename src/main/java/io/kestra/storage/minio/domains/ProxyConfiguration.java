package io.kestra.storage.minio.domains;

import java.net.Proxy;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import io.kestra.core.models.annotations.PluginProperty;

@Getter
@Builder(toBuilder = true)
@Jacksonized
public class ProxyConfiguration {
    @Builder.Default
    private Proxy.Type type = Proxy.Type.DIRECT;

    @Schema(title = "The address of the proxy server.")
    @PluginProperty(group = "advanced")
    private String address;

    @Schema(title = "The port of the proxy server.")
    @PluginProperty(group = "connection")
    private Integer port;

    @Schema(title = "The username for proxy authentication.")
    @PluginProperty(group = "connection")
    private String username;

    @Schema(title = "The password for proxy authentication.")
    @PluginProperty(group = "connection")
    private String password;
}
