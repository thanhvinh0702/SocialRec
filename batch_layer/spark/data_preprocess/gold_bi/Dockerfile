FROM docker.io/library/spark:4.0.1

USER root
RUN mkdir -p /opt/spark/app /tmp/.ivy2

# Tải các thư viện cần thiết để Spark đọc/ghi MinIO (S3)
RUN curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o /opt/spark/jars/hadoop-aws-3.4.1.jar
RUN curl -fL https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar -o /opt/spark/jars/aws-sdk-v2-bundle-2.24.6.jar
RUN curl -fL https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar -o /opt/spark/jars/wildfly-openssl-1.1.3.Final.jar
RUN curl -fL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.772/aws-java-sdk-bundle-1.12.772.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.772.jar

# Copy file code Star Schema của BẠN vào Image
COPY star_schema_gold.py /opt/spark/app/star_schema_gold.py

USER 185