# Containerized Datacat Environment Setup

A containerized version of datacat provides the perfect environment for testing and developing new features without having to worry about making irreversible changes to the production version of datacat.
Within this document you will find instructions on how to set up a containerized version as well as how to use it for the first time.

### Step 1) - Downloading Docker Desktop
To begin setting up the containerized environment, begin by installing docker desktop using the following link:
https://www.docker.com/products/docker-desktop

### Step 2) - Run and Setup docker desktop on your machine
Open the newly downloaded docker desktop and leave it running in the background.

### Step 3) - Build the image using the provided Dockerfile:
Open a linux based terminal and build the required image using the following command line: 
`docker build -t slac-lab/datacat:INT_TESTING .`

*----- Make sure this command line is run at the root level of the /slaclab-datacat repo. -----*

### Step 4) - Start the containerized datacat
Now that we have built the image we can start the containerized version of datacat using the following command line.
`docker-compose --file ./docker-compose.yml up -d`

*----- Make sure this command line is run at the root level of the /slaclab-datacat repo. -----*

### Step 5) - Verify


To verify that the container is up and running visit the following link to view the browser version of datacat.
http://localhost:8080/datacat/display/browser

---

### Extra 1) - Starting and Stopping container:

Everytime you wish to use the newly created environment you will have to manually start the environment as mentioned in step 4.

To start the environment
* `docker-compose --file ./docker-compose.yml up -d`\

To shut down the environment
* `docker-compose --file ./docker-compose.yml down`

### Extra 2) - Frequently Encountered Startup Error: 
If you encounter this message after trying to start the docker using the instructions in step 4, please follow the instructions below.
```
Error response from daemon: OCI runtime create failed: container_linux.go:380: starting container process caused: process_linux.go:545: container init caused: rootfs_linux.go:76: mounting "/run/desktop/mnt/host/wsl/docker-desktop-bi
nd-mounts/Ubuntu-20.04/584eaaf8a37aa769bec4fe56a56f71922981d27d7693d7b1cc5bfcd0b568f295" to rootfs at "/usr/local/tomcat/conf/server.xml" caused: mount through procfd: not a directory: unknown: Are you trying to mount a directory on
to a file (or vice-versa)? Check if the specified host path exists and is the expected type
```

1) Delete the following directories/files from the `/tmp` directory on your HOST machine using the following command lines. 
   1) `rm -rf server.xml`
   2) `rm -rf java.security`
   3) `rm -rf datacat.war`
   4) `rm -rf mysql-connector-java-8.0.16.jar`
2) Manually copy the same files back over by using the following command lines from within a datacat command line instance.
    1) `/bin/cp -f /opt/datacat/docker/tomcat/server.xml /tmp/server.xml`
    2) `/bin/cp -f /opt/datacat/docker/tomcat/java.security /tmp/java.security`
    3) `/bin/cp -f /opt/datacat/webapp/target/datacat.war /tmp/datacat.war`
    4) `/bin/cp -f ~/.m2/repository/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar /tmp/mysql-connector-java-8.0.16.jar`
3) Restart your pc

