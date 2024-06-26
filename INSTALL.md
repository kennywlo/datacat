## Prerequisites

Within this document you will find instructions on how to set up your linux machine to be able to perform Maven Testing as well as
Python Client Testing.


(For instructions on running unit tests within the IntelliJ IDE, [**Click Here**](#unit-testing-within-intellij))

***

### Step 1) - Downloading Required Packages
Before being able to proceed forward with the tests found in [Test documentation](TEST.md), we must first install the following packages onto our machine.
* **For Maven Testing**
    * JDK 8 (Java8)
    * Maven
* **For Python Client Testing**
    * Python3
    *  Pip3
    * Pytest

You can download these packages through the terminal using command line methods, or you can use the provided bash script to download the packages (recommended).

#### Downloading through the included bash script:

Included in the datacat repo is the bash script `prerequisites.sh`, this script will help you download most of the packages you need. Run this script by using the following command line inside your linux terminal.
```
To run:
    source ./prerequisites.sh
After completion, to exit env:
    deactivate
```
Execution of the script will automatically download the following packages onto your machine.

* JDK 8
* Maven
* Python3
* Pip3
* Pytest


As you can see, this will install all required packages except for Pytest. Not to worry though, you can download pytest by navigating to the root of your directory from 
the terminal and running the following command,

``pip3 install pytest``

***Before moving on, take a second to verify that the bash script has properly installed the required packages by using the following commands.***

***Verifying Java8:***
```
-----Command-----
java -version

-----Should Return-----
openjdk version "1.8.0_282"
OpenJDK Runtime Environment (build 1.8.0_282-8u282-b08-0ubuntu1~20.04-b08)
OpenJDK 64-Bit Server VM (build 25.282-b08, mixed mode)
```
***Verifying Maven:***
```
-----Command-----
mvn -version

-----Should Return-----
Apache Maven 3.6.3
Maven home: /usr/share/maven
Java version: 1.8.0_282, vendor: Private Build, runtime: /usr/lib/jvm/java-8-openjdk-amd64/jre
Default locale: en, platform encoding: UTF-8
OS name: "linux", version: "5.4.72-microsoft-standard-wsl2", arch: "amd64", family: "unix"

```

***Verifying Python3:***
```
-----Command-----
python3 --version

-----Should Return-----
Python 3.8.5
```
***Verifying Pip:***
```
-----Command-----
pip3 --version

-----Should Return-----
pip 20.0.2 from /usr/lib/python3/dist-packages/pip (python 3.8)
```
***Verifying Pytest:***
```
-----Command-----
pytest --version

-----Should Return-----
pytest 6.2.2
```



### Step 2) - Finishing Up Maven Test Setup

Once we have installed and verified the required packages, we can finish setting up for Maven testing by setting up JAVA_HOME.

### **Setting up JAVA_HOME**

#### ***Step 1 ) Finding your java installation***

To set up JAVA_HOME, first open a simple text editor like notepad within your windows environment (not your linux
environment). Take the provided line of text and paste it there for later use.

``export JAVA_HOME=" "``

Now within your linux environment travel to where you installed java 8. Java 8 is usually installed at ``/usr/lib/jvm``
and we can verify that by entering the following commands into your linux terminal.

```
ls /usr/lib/jvm
```

If your installation of Java 8 is indeed here, all you need to do is copy your current address and paste it into the
empty quotes of provided text line mentioned earlier (The one in notepad).

You should end up with something like this:

``export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"``

We will be using this line of text in the next step so be sure not to lose it.

#### ***What if Java is installed in a different directory?***

If the above doesn't work for you here is an universal method to find where your JDK is installed.

On Ubuntu or Linux, we can use ``which javac`` to find out where JDK is installed.

```
$ which javac
/usr/bin/javac

$ ls -lsah /usr/bin/javac
/usr/bin/javac -> /etc/alternatives/javac

$ ls -lsah /etc/alternatives/javac
/etc/alternatives/javac -> /usr/lib/jvm/java-11-openjdk-amd64/bin/javac

$ cd /usr/lib/jvm/java-11-openjdk-amd64/bin

$ ./javac -version
javac 11.0.10

```

In the example above, the JDK is installed at ``/usr/lib/jvm/java-11-openjdk-amd64``. The example is using a different
version of JDK but the idea is the same.

#### ***Step 2 ) Setting JAVA_HOME path***

Enter the following command line into your terminal

```
nano ~/.bashrc
```

This will open a text editor that you will use to add the line of text we made in step 1. Paste the line of text to the
end of the opened file and save it. This will tell system to do ``export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"``
everytime you open a new bash instance.

***MacOS setup:***

To make things simple, instead of finding the location of Java you could simply paste the following line into your opened .bashrc file:

```
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```

Where we are utilizing a command to find the path to Java1.8 on your machine and save it to JAVA_HOME directly.

#### ***Step 3 ) Verifying the JAVA_HOME path***

You can test that everything was done properly by entering the following echo command into your terminal.

```
echo $JAVA_HOME

-----Should Return-----
/usr/lib/jvm/java-8-openjdk-amd64
```

If you get something like this, your JAVA_HOME path is now set. If your changes aren't
recognized please reset your linux environment as this tends to apply those changes.


**You are now ready to run the Maven and Python client tests as outlined in [TEST.md](TEST.md)**

***

### Unit Testing Within IntelliJ

Before we can begin testing within the IntelliJ IDE we must first set up our project and configurations as follows.

* **Java Tests**
    * Open IntelliJ and load the datacat repository.
    * Go to File -> Project Structure... Project Settings -> Project
        * Set "Project SDK" to your current installation of Java 8.
    * Go to Run -> Edit Configurations
        * Click on the plus (top left corner) to add a new configuration
        * Select JUnit
            * Name: (Can_be_anything)
            * Build and run
                * `Module not specified`: Set this to your installation of Java 8.
                * `-op <no module>`:  Set this to whatever module you will be testing in.
                * `-ea`: Replace the entire line with.. `-ea -Ddb.test.harness=hsqldb`.
                * `Class`: Leave as is.
                    * In the field to the right, enter the directory which contains the specific test you're running,
                      for example `org.srs.datacat.dao.sql.search.DatacatSearchTest`.

            * `Working directory`: Leave as is.
            * `Environment variables`: Leave as is
            * Click OK

    With this you should now be able to run and debug Java Tests from within the IntelliJ IDE. It is important to
  remember
    that you will need to create a new configuration for every Java test you wish to run.


* **Python Client Tests**
  * Go to File -> Project Structure -> Platform Settings -> SDKs
    * Press ``+`` -> choose Add Python SDK -> Click on System Interpreter -> Python Interpreter
  * Go to Run -> Edit Configuration
    * Press ``+`` -> Choose Python tests -> Pytest
  * Configuration on the right
    * Target -> Script Path -> choose your test module ->
    * Python interpreter -> Use specified interpreter -> choose interpreter you just set up
    * Working directory -> ``\slaclab-datacat\client\python\tests``

  With this you should now be able to run and debug the Python Client Tests from within the IntelliJ IDE.

* **Important Point to Remember** 
  * Before running the Python tests in IntelliJ, ensure that you have followed the instructions on ``DOCKER_SETUP.md``.
  * Open your Linux terminal and run ``docker ps`` to check whether the containers are running.
  * If not, then run the command ``docker-compose --file ./docker-compose.yml up -d`` to start the containers. 
  * You can check the status of datacat my visiting [http://localhost:8080/datacat/display/browser](http://localhost:8080/datacat/display/browser) as mentioned in ``docker-compose.yml``.
  * Your tests may not run properly if your containers have stopped.

* ***IntelliJ Configuration Notes***
  * If IntelliJ is unable to recognize common python modules such as `os` and `sys`, **do not** do as IntelliJ suggests and change the entire `slaclab-datacat/client` module's SDK. 
    * Instead, go to File -> Project Structure -> Facets, and add Python interpreter to the datacat-client module. 
    * Invalidating Caches may also help.
  * Before running the tests, ensure that your Python SDK has pytest package installed. To do this go to File -> Project Structure -> Platform Settings -> SDKs
    * Choose the Python SDK you setup and go to Packages.
    * If pytest is properly installed it should be listed.
    * If pytest is not listed then click on the ``+`` icon, search pytest and install the package. 