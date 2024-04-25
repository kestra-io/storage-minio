package io.kestra.storage.minio;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface MinioConfig {

    @JsonProperty
    String getEndpoint();

    @JsonProperty
    int getPort();

    @JsonProperty
    String getAccessKey();

    @JsonProperty
    String getSecretKey();

    @JsonProperty
    String getRegion();

    @JsonProperty
    boolean isSecure();

    @JsonProperty
    String getBucket();

    @JsonProperty
    boolean isVhost();

    @JsonProperty
    long getPartSize();
}
