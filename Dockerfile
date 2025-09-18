FROM 173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.0

RUN pip3 install \
    pandas==1.4.2 \
    pyarrow
