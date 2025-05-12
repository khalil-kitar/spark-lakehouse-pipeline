FROM khalilkitar/spark-pipeline:3.5.1

# Passe en root pour gérer les droits
USER root

RUN mkdir -p /opt/spark/jars && \
    mkdir -p /opt/spark/conf && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    curl -L -o /opt/spark/jars/delta-storage-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    curl -L -o /opt/spark/jars/delta-core_2.12-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/3.0.0/delta-core_2.12-3.0.0.jar && \
    chmod 644 /opt/spark/jars/*.jar && \
    chown 185:185 /opt/spark/jars/*.jar && \
    echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" >> /opt/spark/conf/spark-defaults.conf

    

# Reviens à l’utilisateur Spark (UID utilisé par ton image)
USER 185

# Dossier de travail
WORKDIR /app

# Copie les scripts
COPY . /app

