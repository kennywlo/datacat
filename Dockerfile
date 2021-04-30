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

RUN yum -y install epel-release && \
    yum -y install https://repo.ius.io/ius-release-el7.rpm && \
    yum -y install httpd mod_ssl mod_auth_kerb python-setuptools python-pip python36u-pip python36-devel python36-mod_wsgi python36-m2crypto gmp-devel krb5-devel git openssl-devel multitail vim && \
    yum -y localinstall mysql57-community-release-el7-9.noarch.rpm \
    yum -y install mysql-community-server \
    yum -y install mysql-community-libs \
    yum -y install java-1.8.0-openjdk-devel \
    yum clean all && \
    rm -rf /var/cache/yum

RUN git clone https://gitlab.com/supercdms/slaclab-datacat.git /tmp/slaclab-datacat

ENV DCHOME=/opt/datacat

RUN mkdir -p $DCHOME

WORKDIR $DCHOME

RUN mkdir -p \
    bin \
    etc \
    lib/datacat \
    tools

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade setuptools && \
    rm -rf /usr/lib/python3.6/site-packages/ipaddress* && \
    ln -s $DCHOME/lib/datacat /usr/local/lib/python3.6/site-packages/datacat

RUN mkdir /opt/tomcat/
WORKDIR /opt/tomcat
RUN curl -O https://downloads.apache.org/tomcat/tomcat-8/v8.5.65/src/apache-tomcat-8.5.65-src.tar.gz && \
    tar xvf apache-tomcat-8.5.65-src.tar.gz && \
    mv apache-tomcat-8.5.65-src/* /opt/tomcat/.

WORKDIR /opt/tomcat/webapps
RUN curl -O -L https://github.com/AKSarav/SampleWebApp/raw/master/dist/SampleWebApp.war

RUN rm -r /tmp/slaclab-datacat && \
    ln -fs /usr/bin/python3 /usr/bin/python

EXPOSE 8080 8443

ENV PATH $PATH:$DCHOME/bin

CMD ["/usr/sbin/init"]
CMD ["systemctl start mysqld", "run"]
CMD ["/opt/tomcat/bin/catalina.sh", "run"]

