# Deminstified Sagemaker Processing Job with Spark

## Prerequisites

Upload data in your bucket

```
aws s3 cp ./data/* s3://<BUCKET_NAME>/<SUFFIX>/ --recursive
```

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
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 571004829621.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-spark-processing:3.1-cpu
```

### Build custom image

```
docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG} -f Dockerfile .
```

* ACCOUNT_ID: AWS Account ID
* REGION: AWS Region
* REPOSITORY_NAME: ECR Repository name
* IMAGE_TAG: Tag associated to the image

### Push Image to ECR

```
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME} | docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG}
```

* ACCOUNT_ID: AWS Account ID
* REGION: AWS Region
* REPOSITORY_NAME: ECR Repository name
* IMAGE_TAG: Tag associated to the image
