# Use a imagem do Airflow como base
FROM apache/airflow:2.7.0-python3.9

# Copiar o arquivo requirements.txt
COPY requirements.txt /requirements.txt

# Instalar os pacotes Python a partir do requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

# Instalar Java e o cliente Hive (Beeline)
USER root

# Atualizar para instalar o OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    curl --retry 5 --retry-delay 10 -O https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz && \
    tar -zxvf apache-hive-2.3.9-bin.tar.gz -C /opt/ && \
    ln -s /opt/apache-hive-2.3.9-bin /opt/hive && \
    rm apache-hive-2.3.9-bin.tar.gz

# Instalar Hadoop
RUN curl -O https://archive.apache.org/dist/hadoop/core/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -zxvf hadoop-3.3.6.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-3.3.6 /opt/hadoop && \
    rm hadoop-3.3.6.tar.gz

# Instalar Scala 2.13 para compatibilidade com o Spark master
RUN curl -O https://downloads.lightbend.com/scala/2.13.8/scala-2.13.8.deb && \
    dpkg -i scala-2.13.8.deb && \
    rm scala-2.13.8.deb

# Adicionar Hive ao PATH
ENV PATH="$PATH:/opt/hive/bin"
ENV HIVE_HOME="/opt/hive"

# Variáveis de ambiente para Java
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV HADOOP_HOME="/opt/hadoop"
ENV HADOOP_PREFIX="/opt/hadoop"
ENV PATH="$PATH:$HADOOP_HOME/bin"

# Retorna ao usuário airflow
USER airflow