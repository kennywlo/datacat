# Copyright 2021 SLAC for the benefit of the SuperCDMS collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Kenny Lo <kennywlo@slac.stanford.edu>, 2021

FROM centos:7

MAINTAINER kennylo@slac.stanford.edu

ENV DCHOME=/opt/datacat
RUN mkdir -p $DCHOME

WORKDIR $DCHOME

RUN yum -y install wget && \
    wget https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm

RUN yum -y install epel-release \
        https://repo.ius.io/ius-release-el7.rpm \
        httpd mod_ssl mod_auth_kerb python-setuptools python-pip python36u-pip python36-devel \
        python36-mod_wsgi python36-m2crypto gmp-devel krb5-devel git openssl-devel multitail vim && \
    yum -y localinstall mysql57-community-release-el7-11.noarch.rpm && \
    yum -y install mysql-community-server \
        mysql-community-libs \
        java-1.8.0-openjdk-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN git clone https://gitlab.com/supercdms/slaclab-datacat.git /tmp/slaclab-datacat
WORKDIR /tmp/slaclab-datacat

RUN yum -y install maven && mvn package -DskipTests

# Setup Tomcat
RUN mkdir /opt/tomcat/
WORKDIR /opt/tomcat
RUN curl -O https://downloads.apache.org/tomcat/tomcat-8/v8.5.65/bin/apache-tomcat-8.5.65.tar.gz && \
    tar xvf apache-tomcat-8.5.65.tar.gz && \
    rm -f apache-tomcat-8.5.65.tar.gz && \
    mv apache-tomcat-8.5.65/* /opt/tomcat/. && \
    rm -f apache-tomcat-8.5.65.tar.gz && \
    rm -rf apache-tomcat-8.5.65

RUN mv /tmp/slaclab-datacat/webapp/target/org-srs-webapps-datacat-0.6-DEPENDENCY.war /opt/tomcat/webapps

RUN rm -rf /tmp/slaclab-datacat && \
    chmod +x /opt/tomcat/bin/catalina.sh && \
    ln -fs /usr/bin/python3 /usr/bin/python

EXPOSE 8080 8443

ENV PATH $PATH:$DCHOME/bin

# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-openjdk
RUN export JAVA_HOME

CMD ["/usr/sbin/init"]
CMD ["systemctl start mysqld", "run"]
CMD ["/opt/tomcat/bin/catalina.sh", "run"]

