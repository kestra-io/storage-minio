package io.kestra.storage.minio.domains;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SslOptions {
    @Schema(
        title = "Whether to disable checking of the remote SSL certificate.",
        description = "Only applies if no trust store is configured. Note: This makes the SSL connection insecure and should only be used for testing. If you are using a self-signed certificate, set up a trust store instead."
    )
    private Boolean insecureTrustAllCertificates;
}
