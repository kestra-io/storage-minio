package io.kestra.storage.minio;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.utils.IdUtils;
import io.kestra.storage.minio.domains.SslOptions;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class MinioStorageTlsTest {
    private static String caPem;
    private static String clientPem;
    private static final String ACCESS_KEY = "accessKey";
    private static final String SECRET_KEY = "secretKey";
    private static final String BUCKET = "tls-bucket";

    @BeforeEach
    void loadCerts() throws Exception {
        Path certDir = Path.of("src/test/resources/mtls");
        caPem = Files.readString(certDir.resolve("ca-cert.pem"));
        clientPem = Files.readString(certDir.resolve("client-cert-key.pem"));
    }

    @Test
    void shouldUploadAndReadWithTls() throws Exception {
        MinioStorage storage = MinioStorage.builder()
            .endpoint("localhost")
            .port(9443)
            .accessKey(ACCESS_KEY)
            .secretKey(SECRET_KEY)
            .bucket(BUCKET)
            .secure(true)
            .caPem(caPem)
            .clientPem(clientPem)
            .sslOptions(SslOptions.builder().insecureTrustAllCertificates(false).build())
            .build();

        storage.init();

        if (!storage.minioClient().bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
            storage.minioClient().makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }

        String fileContent = "hello secure world";
        URI uri = new URI("/" + IdUtils.create() + ".txt");

        storage.put(MAIN_TENANT, null, uri, new ByteArrayInputStream(fileContent.getBytes()));

        try (var in = storage.get(MAIN_TENANT, null, uri)) {
            byte[] result = in.readAllBytes();
            assertThat(new String(result), is(fileContent));
        }

        storage.close();
    }
}
