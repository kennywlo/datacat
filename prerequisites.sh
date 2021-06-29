#!/bin/bash

shopt -s expand_aliases

# This script will install Java8, Python3, Pip, PyTest, and Maven.
# To run:
#         source ./prerequisites.sh
# After completion, to exit env:
#         deactivate

function toggle_os()
{
        if cat /etc/os-release | grep -nw rhel ; then
            echo "using a rhel marked system"
            #-----Repository Index Update Script-----
            echo -e "\n-----Repository Index Update-----"
            if yum -y makecache ; then
                echo "repository index updated using yum, moving forward"
            else
                echo "Error updating Index"
            fi
            alias pkg_tool='yum -y $@'
            var_java=" java-1.8.0-openjdk"
        elif cat /etc/os-release | grep debian ; then
            echo "using a debian marked system"
            #-----Repository Index Update Script-----
            echo -e "\n-----Repository Index Update-----"
            if sudo apt update ; then
                echo "repository index updated using apt, moving forward"
            else
                echo "Error updating Index"
            fi
            alias pkg_tool='sudo apt -y $@'
            var_java=" openjdk-8-jdk"
        else
          return 1
        fi
}

# switch to the right OS
if ! toggle_os ; then
  echo "ERROR: Unrecognized OS... Only rhel and debian systems are supported"
  echo "Now Exiting"
  exit 1
fi


echo "******************************"
echo "*****DOWNLOADING PACKAGES*****"
echo "******************************"
#-----Java8 install script-----
echo -e "\n-----Java Installation-----"
if pkg_tool install$var_java ; then
        echo "java8 installed"
else
        echo "java8 installation failed"
fi
#-----Python3 install script-----
echo -e "\n-----Python Installation-----"
if pkg_tool install python3 ; then
        echo "python3 installed"
else
        echo "python3 installation failed"
fi
#-----Pip install script-----
echo -e "\n-----Pip Installation-----"
if pkg_tool install python3-pip ; then
        echo "pip installed"
else
        echo "pip installation failed"
fi
#-----Install virtualenv----
echo -e "\n----virtualenv installation----"
if pkg_tool install python3-venv ; then
        echo "virtualenv installed"
else
        echo "virtualenv installation failed"
fi
#----Create env----
echo -e "\n----create env----"
if python3 -m venv env ; then
        echo "env created"
else
        echo "env creation failed"
fi
#----Activate env----
echo -e "\n----env activation----"
if source env/bin/activate ; then
        echo "env activated"
else
        echo "env activation failed"
fi
#----install wheel----
echo -e "\n----wheel installation----"
if pip3 install wheel ; then
        echo "wheel installed"
else
        echo "wheel installation failed"
fi
#-----Python Client Packages Installation script-----
echo -e "\n-----Python Client Packages Installation-----"
if pip3 install client/python/. ;then
        echo "Python Client Packages installed"
else
        echo "Python Client Packages installation failed"
fi
#-----Pytest install script-----
echo -e "\n-----Pytest Installation-----"
if pip3 install pytest ; then
        echo "pytest installed using pip3"
else
        echo "pytest installation failed"
fi
#-----Maven install script-----
echo -e "\n-----Maven Installation-----"
if pkg_tool install maven ; then
        echo "maven installed"
else
        echo "maven installation failed"
fi
echo ""
echo "*****************************"
echo "*****  PACKAGE DETAILS  *****"
echo "*****************************"
echo -e "\n-----Java Installation Details-----"
java -version
echo -e "\n-----Python Installation Details-----"
python3 --version
echo -e "\n-----Pip Installation Details-----"
pip3 --version
echo -e "\n-----Pytest Installation Details-----"
pytest --version
echo -e "\n-----Maven Installation Details-----"
mvn -version
