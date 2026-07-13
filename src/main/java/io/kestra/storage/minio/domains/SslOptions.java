package io.kestra.storage.minio.domains;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SslOptions {
    @Schema(
        title = "Whether to disable checking of the remote SSL certificate.",
        description = "WARNING: Enabling this option completely disables TLS certificate and hostname verification for all MinIO connections, " +
            "exposing credentials and data in transit to man-in-the-middle attacks (CWE-295, CWE-297). " +
            "This must NEVER be used in production environments. " +
            "Only use this for local development or testing with self-signed certificates. " +
            "For production use with self-signed certificates, configure a CA trust store via the caPem option instead. " +
            "A WARN-level log message is emitted at runtime when this option is active."
    )
    @PluginProperty(group = "advanced")
    private Boolean insecureTrustAllCertificates;
}
