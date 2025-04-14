package io.kestra.storage.minio.domains;

import io.swagger.v3.oas.annotations.media.Schema;
import java.net.Proxy;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder(toBuilder = true)
@Jacksonized
public class ProxyConfiguration {
    @Builder.Default
    private Proxy.Type type = Proxy.Type.DIRECT;

    @Schema(title = "The address of the proxy server.")
    private String address;

    @Schema(title = "The port of the proxy server.")
    private Integer port;

    @Schema(title = "The username for proxy authentication.")
    private String username;

    @Schema(title = "The password for proxy authentication.")
    private String password;
}
