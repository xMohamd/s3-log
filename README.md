# s3-log

A durable, distributed, and highly available Write-Ahead Log (WAL) implementation using Amazon S3 as the storage backend.

## Features

- Append-only log with strictly sequential offsets
- Data integrity verification using SHA-256 checksums
- Support for reading by offset
- Last record retrieval

## Requirements

- Go 1.23 or later
- S3-compatible storage (AWS S3 or MinIO for local development)
- Docker for running MinIO

## Installation

To install the package, use the following command:

```bash
go get github.com/xMohamd/s3-log
```

## Testing

1. Start MinIO using Docker:

```bash
mkdir -p ~/minio/data

docker run -d \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   quay.io/minio/minio server /data --console-address ":9001"
```

2. Run the tests:

```bash
go test 
```
