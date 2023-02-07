FROM 571004829621.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-spark-processing:3.1-cpu

RUN pip3 install pandas pyarrow
