package io.kestra.storage.minio;

import io.kestra.core.storages.FileAttributes;
import io.minio.StatObjectResponse;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
public class MinioFileAttributes implements FileAttributes {
    String fileName;
    StatObjectResponse stat;
    boolean isDirectory;
    Map<String, String> metadata;

    @Builder
    public MinioFileAttributes(String fileName, StatObjectResponse stat, boolean isDirectory) {
        this.fileName = fileName;
        this.stat = stat;
        this.isDirectory = isDirectory;

        this.metadata = MetadataUtils.toRetrievedMetadata(stat.userMetadata());
    }

    @Override
    public long getLastModifiedTime() {
        return stat.lastModified().toInstant().toEpochMilli();
    }

    @Override
    public long getCreationTime() {
        return stat.lastModified().toInstant().toEpochMilli();
    }

    @Override
    public FileType getType() {
        return isDirectory ? FileAttributes.FileType.Directory : FileAttributes.FileType.File;
    }

    @Override
    public long getSize() {
        return stat.size();
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }
}
