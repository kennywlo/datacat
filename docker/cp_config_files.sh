#!/usr/bin/env bash
# Author: Kenny Lo <kennylo@slac.stanford.edu>, 2021

# move the files for Tomcat
/bin/cp /opt/datacat/webapp/target/org-srs-webapps-datacat-0.6.war /opt/datacat/docker/tomcat/datacat.war
/bin/cp -f ~/.m2/repository/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar \
/opt/datacat/docker/tomcat/mysql-connector-java-8.0.16.jar

# to keep container alive for testing purpose
tail -F /dev/null
