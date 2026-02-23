# FROM apache/spark:3.5.1

# USER root

# # Install Python and pip
# RUN apt-get update && apt-get install -y \
#     python3 \
#     python3-pip \
#     curl \
#     wget \
#     && rm -rf /var/lib/apt/lists/*

# # Install Python packages
# # Install Python packages
# ENV PIP_TRUSTED_HOST="pypi.org files.pythonhosted.org pypi.python.org"
# ENV PIP_NO_CACHE_DIR=1
# RUN pip3 install --no-cache-dir \
#     pyspark==3.5.3 \
#     pyarrow==14.0.2 \
#     pandas==2.0.3 \
#     boto3==1.34.0 \
#     pyiceberg[s3fs,pyarrow]==0.6.0 \
#     faker==22.6.0

# # ==========================================
# # JAR DEPENDENCIES
# # ==========================================

# ENV SPARK_JARS_DIR=/opt/spark/jars

# # Iceberg Spark Runtime
# RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
#     -O $SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# # Nessie Spark Extensions
# RUN wget -q https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar \
#     -O $SPARK_JARS_DIR/nessie-spark-extensions-3.5_2.12-0.77.1.jar

# # AWS S3 / MinIO support
# RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#     -O $SPARK_JARS_DIR/hadoop-aws-3.3.4.jar

# RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
#     -O $SPARK_JARS_DIR/aws-java-sdk-bundle-1.12.262.jar

# # Delta Lake
# RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
#     -O $SPARK_JARS_DIR/delta-spark_2.12-3.1.0.jar

# RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
#     -O $SPARK_JARS_DIR/delta-storage-3.1.0.jar

# # PostgreSQL JDBC
# RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
#     -O $SPARK_JARS_DIR/postgresql-42.7.1.jar

# # Spark SQL Kafka
# RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
#     -O $SPARK_JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.1.jar

# # ==========================================
# # SPARK CONFIGURATION
# # ==========================================

# COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# ENV SPARK_HOME=/opt/spark
# ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# ENV PYSPARK_PYTHON=python3
# ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# WORKDIR /opt/spark

FROM apache/spark:3.5.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# If you have a corporate CA cert, uncomment these lines and place the .crt file in ./spark/
# COPY your-company-ca.crt /usr/local/share/ca-certificates/your-company-ca.crt
# RUN update-ca-certificates

# Upgrade pip to latest
RUN pip3 install --upgrade pip \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --no-cache-dir

# Install Python packages
RUN pip3 install \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --no-cache-dir \
    pyspark==3.5.3 \
    pyarrow==14.0.2 \
    "pandas>=2.0.0,<2.1.0" \
    boto3==1.34.0 \
    "pyiceberg[s3fs,pyarrow]==0.6.0" \
    faker==22.6.0

# ==========================================
# JAR DEPENDENCIES
# ==========================================

ENV SPARK_JARS_DIR=/opt/spark/jars

# Iceberg Spark Runtime
# RUN wget -q --no-check-certificate \
#     https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
#     -O $SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -O $SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.5.2.jar
# Nessie Spark Extensions
# AFTER (correct - API v2)
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.90.4/nessie-spark-extensions-3.5_2.12-0.90.4.jar \
    -O $SPARK_JARS_DIR/nessie-spark-extensions-3.5_2.12-0.90.4.jar

# AWS S3 / MinIO support
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O $SPARK_JARS_DIR/hadoop-aws-3.3.4.jar

RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -O $SPARK_JARS_DIR/aws-java-sdk-bundle-1.12.262.jar

# Delta Lake
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -O $SPARK_JARS_DIR/delta-spark_2.12-3.1.0.jar

RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    -O $SPARK_JARS_DIR/delta-storage-3.1.0.jar

# PostgreSQL JDBC
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    -O $SPARK_JARS_DIR/postgresql-42.7.1.jar

# Spark SQL Kafka
RUN wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -O $SPARK_JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.1.jar

# ==========================================
# SPARK CONFIGURATION
# ==========================================

COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /opt/spark