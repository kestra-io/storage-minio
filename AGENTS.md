# Kestra Minio Storage

## What

- Implements the storage backend under `io.kestra.storage.minio`.
- Includes classes such as `MinioFileAttributes`, `MinioConfig`, `MetadataUtils`, `MinioClientFactory`.

## Why

- This repository implements a Kestra storage backend for storage plugin for S3 and S3-compatible services such as Minio.
- It stores namespace files and internal execution artifacts outside local disk.

## How

### Architecture

Single-module plugin.

Infrastructure dependencies (Docker Compose services):

- `mitmproxy`
- `versity`
- `versity-data`
- `versity-data-tls`
- `versity-tls`

### Project Structure

```
storage-minio/
├── src/main/java/io/kestra/storage/minio/internal/
├── src/test/java/io/kestra/storage/minio/internal/
├── build.gradle
└── README.md
```

## Local rules

- Keep the scope on Kestra internal storage behavior, not workflow task semantics.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
