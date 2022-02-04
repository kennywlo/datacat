# Server certificate in keystore for development testing

Every 90 days, the keystore.jks file (used for https) in this directory with a self-signed certificate needs to be 
regenerated as follows:
    $JAVA_HOME/bin/keytool -genkey -alias tomcat -keyalg RSA  -keystore ./keystore.jks

For reference see [SSL/TLS Configuration How-To](https://tomcat.apache.org/tomcat-10.0-doc/ssl-howto.html)