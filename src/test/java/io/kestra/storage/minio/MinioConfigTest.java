package io.kestra.storage.minio;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.storage.minio.internal.BytesSize;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class MinioConfigTest {

    private static final Map<String, Object> DEFAULT_CONFIG = Map.of(
        "accessKey", "test-access-key",
        "secretKey", "test-secret-key",
        "endpoint","localhost",
        "bucket","unittest",
        "port", 9000
    );

    @Test
    void shouldDeserializeGivenByteSizeInLong() {
        HashMap<String, Object> config = new HashMap<>(DEFAULT_CONFIG);
        config.put("partSize", 1024);
        MinioStorage storage = JacksonMapper.toMap(config, MinioStorage.class);
        Assertions.assertEquals(new BytesSize(1024), storage.getPartSize());
    }

    @Test
    void shouldDeserializeGivenByteSizeInString() {
        HashMap<String, Object> config = new HashMap<>(DEFAULT_CONFIG);
        config.put("partSize", "50MB");
        MinioStorage storage = JacksonMapper.toMap(config, MinioStorage.class);
        Assertions.assertEquals(new BytesSize("50MB"), storage.getPartSize());
        Assertions.assertEquals(new BytesSize(1024 * 1024 * 50), storage.getPartSize());
    }

    @Test
    void shouldThrowWhenDeserializingGivenInvalidByteSize() {
        HashMap<String, Object> config = new HashMap<>(DEFAULT_CONFIG);
        config.put("partSize", "50TB"); // not supported
        Assertions.assertThrows(IllegalArgumentException.class, () -> JacksonMapper.toMap(config, MinioStorage.class));
    }
}