#!/bin/bash
# Author: Kenny Lo <kennylo@slac.stanford.edu>, 2021

# copy out the files needed in Tomcat
/bin/cp -f /opt/datacat/docker/tomcat/server.xml /tmp/server.xml
/bin/cp -f /opt/datacat/docker/tomcat/java.security /tmp/java.security
/bin/cp -f /opt/datacat/docker/tomcat/keystore.jks /tmp/keystore.jks
/bin/mv /opt/datacat/webapp/target/org-srs-webapps-datacat-0.6.war /opt/datacat/webapp/target/datacat.war
/bin/cp -f /opt/datacat/webapp/target/datacat.war /tmp/datacat.war
/bin/cp -f ~/.m2/repository/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar /tmp/mysql-connector-java-8.0.16.jar

# to keep container alive for testing purpose
tail -F /dev/null