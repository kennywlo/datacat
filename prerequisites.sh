#!/bin/sh
# This script will install Java8 and Maven.

#-----Java8 install script-----
echo -e "\n-----Java Installation-----"
sudo apt update
sudo apt install openjdk-8-jdk
echo -e "\n-----Java Installation Details-----"
java -version

echo -e "\n-----Javac Check-----"
javacCheck=$(ls /usr/bin | grep javac)

if [ "$javacCheck" = "javac" ];
then
    echo "Javac Installation Confirmed"
else
    echo "Javac Could Not Be Located"
fi

#-----Python3 install script-----
echo -e "\n-----Python Installation-----"
sudo apt update
sudo apt install python3.8

echo -e "\n-----Python Installation Details-----"
python3 --version


#-----Pip install script-----
echo -e "\n-----Pip Installation-----"
sudo apt update
sudo apt install python3-pip
echo -e "\n-----Pip Installation Details-----"
pip3 --version

#WORK IN PROGRESS
#-----Pytest install script-----
#echo -e "\n-----Pytest Installation-----"
#pip install -U pytest
#echo -e "\n-----Pytest Installation Details-----"
#pytest --version


#-----Maven install script-----
echo -e "\n-----Maven Installation-----"
sudo apt update
sudo apt install maven

echo -e "\n-----Maven Installation Details-----"
mvn -version
echo -e "\n"