package io.kestra.storage.minio;

import io.kestra.core.models.annotations.PluginProperty;

public interface MinioConfig {
    
    @PluginProperty
    String getEndpoint();
    
    @PluginProperty
    int getPort();
    
    @PluginProperty
    String getAccessKey();
    
    @PluginProperty
    String getSecretKey();
    
    @PluginProperty
    String getRegion();
    
    @PluginProperty
    boolean isSecure();
    
    @PluginProperty
    String getBucket();
    
    @PluginProperty
    boolean isVhost();
    
    @PluginProperty
    long getPartSize();
}
