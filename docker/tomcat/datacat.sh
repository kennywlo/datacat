#!/usr/bin/env bash
# Author: Kenny Lo <kennylo@slac.stanford.edu>, 2022

# move the files for Tomcat to the right locations
dc_dir=/opt/datacat/docker/tomcat
/bin/cp $dc_dir/setenv.sh /usr/local/tomcat/bin/setenv.sh
/bin/cp $dc_dir/server.xml /usr/local/tomcat/conf/server.xml
/bin/cp $dc_dir/keystore.jks /usr/local/tomcat/conf/keystore.jks
/bin/cp $dc_dir/java.security /usr/local/openjdk-8/jre/lib/security/java.security
/bin/cp $dc_dir/datacat.war /usr/local/tomcat/webapps/datacat.war
/bin/cp $dc_dir/mysql-connector-java-8.0.16.jar /usr/local/tomcat/lib/mysql-connector-java-8.0.16.jar

# start Tomcat
/usr/local/tomcat/bin/catalina.sh "$@"