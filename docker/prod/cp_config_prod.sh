#!/bin/bash
# Author: Kenny Lo <kennylo@slac.stanford.edu>, 2021

# copy out the files needed in Tomcat
/bin/cp -f /opt/datacat/docker/tomcat/server_prod.xml /tmp/server_prod.xml
/bin/cp -f /opt/datacat/docker/tomcat/java.security /tmp/java.security
/bin/mv /opt/datacat/webapp/target/org-srs-webapps-datacat-0.6-DEPENDENCY.war /opt/datacat/webapp/target/datacat.war
/bin/cp -f /opt/datacat/webapp/target/datacat.war /tmp/datacat.war

# to keep container alive for testing purpose
tail -F /dev/null
