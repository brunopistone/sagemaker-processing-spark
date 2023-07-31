FROM 571004829621.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-spark-processing:3.3-cpu-py39-v1.2

RUN pip3 install \
    pandas==1.4.2 \
    pyarrow
