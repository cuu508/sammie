README
======

Sammie synchronizes contents of a S3 bucket to the local filesystem. It
expects all keys in the bucket to follow a specific pattern:

    /{directoryname}/{objectname}

Build a static binary:

    CGO_ENABLED=0 go build

Use:

```
export ENDPOINT=s3.sbg.perf.cloud.ovh.net
export REGION=sbg
export ACCESS_KEY=fixme
export SECRET_KEY=fixme
export BUCKET=bucket-name
export DST=/target/directory/
./sammie
```