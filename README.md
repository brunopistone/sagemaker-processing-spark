# Deminstified Sagemaker Processing Job with Spark

## Prerequisites

### 1. Generate syntetic data

```
python utils/syntetic_data_energy.py
```

```
python utils/syntetic_data_weather.py
```

### 2. Upload data in your bucket

```
aws s3 sync ./data/output/ s3://<BUCKET_NAME>/electricity-forecasting/data/input/
```

---

## HDFSManager

This example is providing an utility class [HDFSManager](./code/services/HDFSManager.py) for managing data from and to HDFS.

### Copy files from local file system to HDFS

```
hdfs_manager.copy_full_to_hdfs(<LOCAL_PATH>)
```

### Copy files from HDFS to local file system

```
hdfs_manager.copy_from_hdfs(<LOCAL_FILE_SYSTEM>, <HDFS_FILE_NAME>)
```

### Read file from HDFS into a Spark DataFrame

```
df = hdfs_manager.load_df(<SPARK_CONTEXT>, <FILE_NAME>)
```

### Save Spark DataFrame to HDFS

```
hdfs_manager.save_df(df, <FILE_NAME>)
```

---

## Build Docker Image

### Create ECR Repository

```
aws ecr create-repository --repository-name <REPOSITORY_NAME>
```

* REPOSITORY_NAME: ECR Repository name

Example:

```
aws ecr create-repository --repository-name sagemaker-spark-custom-container
```

### Login to Spark Image repository

```
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <SPARK_IMAGE_URI>
```

* REGION: AWS Region
* SPARK_IMAGE_URI: Public ECR URI for the Spark image

Please refer to the official [AWS GitHub repository](https://github.com/aws/sagemaker-spark-container/blob/master/available_images.md)

Example:

```
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin  571004829621.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-spark-processing:3.3-cpu-py39-v1.2
```

### Build custom image

```
docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG} -f Dockerfile .
```

Example:

```
docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/sagemaker-spark-custom-container:latest -f Dockerfile .
```

* ACCOUNT_ID: AWS Account ID
* REGION: AWS Region
* REPOSITORY_NAME: ECR Repository name
* IMAGE_TAG: Tag associated to the image

### Push Image to ECR

```
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG} | docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG}
```

```
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG}
```

* ACCOUNT_ID: AWS Account ID
* REGION: AWS Region
* REPOSITORY_NAME: ECR Repository name
* IMAGE_TAG: Tag associated to the image

Example:

```
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/sagemaker-spark-custom-container:latest
```

```
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/sagemaker-spark-custom-container:latest
```
