SageMaker Processing Job with ProcessingInputs and HDFS management

## Prerequisites

Upload data in your bucket

```
aws s3 cp ./data/* s3://<BUCKET_NAME>/<SUFFIX>/ --recursive
```