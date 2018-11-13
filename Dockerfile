FROM ubuntu:18.04

LABEL maintainer="Timothy Hnat <twhnat@memphis.edu>"
LABEL org.md2k.cerebralcortex.version='2.3.0'
LABEL description="Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data."

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.3.2
ENV HADOOP_VERSION 2.7

ENV SPARK_HOME  /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip
ENV JAVA_HOME   /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH        $JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON python3


RUN apt-get update \
  && apt-get install -yqq wget git python3-pip  openjdk-8-jre python3-setuptools libyaml-dev libev-dev liblapack-dev \
  && pip3 install --upgrade --force-reinstall pip==9.0.3 \
  && pip3 install cython


RUN cd /tmp && \
        wget -q http://apache.cs.utah.edu/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
        tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local && \
        rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark


COPY . /CerebralCortex

RUN cd CerebralCortex \
    && /usr/local/bin/pip3 install -r requirements.txt \
    && python3 setup.py install \
    && cd .. && rm -rf CerebralCortex

VOLUME /cc_data

WORKDIR /cc_app

ENTRYPOINT bash
