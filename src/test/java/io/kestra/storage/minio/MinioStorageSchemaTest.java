package io.kestra.storage.minio;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
class MinioStorageSchemaTest {
    @Inject
    private JsonSchemaGenerator jsonSchemaGenerator;

    @SuppressWarnings("unchecked")
    @Test
    void schema() {
        var schema = jsonSchemaGenerator.properties(StorageInterface.class, MinioStorage.class);

        var properties = (Map<String, Object>) schema.get("properties");
        assertThat((List<String>) schema.get("required"), containsInAnyOrder("endpoint", "bucket"));
        assertThat(properties, not(hasKey("minioClient")));
        assertThat(((Map<String, Object>) properties.get("clientPem")).get("$secret"), is(true));
    }
}
