
Download the latest verions of YADAMU from [https://github.com/markddrake/YADAMU---Yet-Another-DAta-Migration-Utility/archive/master.zip](url)

This can be done using a Web Browser or a command line tool such as CURL:

```
curl -L https://github.com/markddrake/YADAMU---Yet-Another-DAta-Migration-Utility/archive/master.zip -oYADAMU.zip
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   159  100   159    0     0    159      0  0:00:01 --:--:--  0:00:01   509
100  880k    0  880k    0     0   880k      0 --:--:-- --:--:-- --:--:-- 4018k
C:\doc>
```

Unpack the zip file:
```
tar -xvf YADAMU.zip
x YADAMU---Yet-Another-DAta-Migration-Utility-master/
x YADAMU---Yet-Another-DAta-Migration-Utility-master/.gitignore
x YADAMU---Yet-Another-DAta-Migration-Utility-master/Dockerfile
x YADAMU---Yet-Another-DAta-Migration-Utility-master/Dockerfile.dockerignore
...
x YADAMU---Yet-Another-DAta-Migration-Utility-master/utilities/node/compareArrayContent.js
x YADAMU---Yet-Another-DAta-Migration-Utility-master/utilities/node/compareFileSizes.js
x YADAMU---Yet-Another-DAta-Migration-Utility-master/utilities/node/sorter.js
x YADAMU---Yet-Another-DAta-Migration-Utility-master/utilities/node/testSchemaMappings.js
```

Rename the YADAMU home directory (This makes life much easier)

```
rename YADAMU---Yet-Another-DAta-Migration-Utility-master YADAMU
```

Change the current directory to the YADAMU home directory
```
cd YADAMU
```

If working with a remote docker instance set the DOCKER_HOST variable {Make sure to provide values for user and docker_host approriate for your environment)
```
set DOCKER_HOST=ssh://user@docker_host
``` 

Verify that your docker environment is configured correctly. If it's configured correctly you will see something similar to this

```
docker info

Server:
 Containers: 18
  Running: 15
  Paused: 0
  Stopped: 3
 Images: 42
 Server Version: 19.03.13
 Storage Driver: overlay2
 ....
```

Build a Docker Image containing the software environment required to run YADAMU
```
docker build -t  yadamu/environment:latest . -f docker/dockerfiles/environment 
Sending build context to Docker daemon   3.53MB
Step 1/5 : FROM node:latest
 ---> ebcfbb59a4bd
Step 2/5 : WORKDIR /usr/src/YADAMU
 ---> Using cache
 ---> c022cdffacfa
Step 3/5 : ADD https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm .
Downloading [==================================================>]  54.08MB/54.08MB
 ---> Using cache
 ---> e1d25ec6777d
Step 4/5 : COPY src/package*.json ./
 ---> 106c166de1a9
Step 5/5 : RUN apt update  && apt-get -y install libaio1  && apt-get -y install alien  && alien -i ./oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm  && npm install --global electron  && npm install
 ---> Running in 52314e424a95
...
Removing intermediate container 52314e424a95
 ---> 195fea7069b1
Successfully built 195fea7069b1
Successfully tagged yadamu/environment:latest
SECURITY WARNING: You are building a Docker image from Windows against a non-Windows Docker host. All files and directories added to build context will have '-rwxr-xr-x' permissions. It is recommended to double check and reset permissions for sensitive files and directories.
```

This is a image is derived from the official Node Docker Image. The current version is based on the Alpine Distro and Node 15. The image also includes the Oracle instant client for Linux, which is required to use oracledb, the official node driver for the Oracle database. The build process uses NPM to pull in all of the other 3rd Party NPM packages required by YADAMU. You can see the list of packages and thier versions by checking out of the contents of package.json

Build the Yadamu Runtime Docker Image
```
docker build -t  yadamu/docker:latest . -f docker/dockerfiles/yadamuRuntime

C:\doc\YADAMU>docker build -t  yadamu/docker:latest . -f docker/dockerfiles/yadamuRuntime
yadamu@yadamu-db1's password:
Sending build context to Docker daemon   3.53MB
Step 1/10 : FROM yadamu/environment:latest
 ---> 195fea7069b1
Step 2/10 : WORKDIR /usr/src/YADAMU
 ---> Running in a5d613e7e882
Removing intermediate container a5d613e7e882
 ---> e4c4501e6f49
Step 3/10 : COPY src src
 ---> f9059b3bfd8b
Step 4/10 : COPY bin bin
 ---> b5a807d8d503
Step 5/10 : ENV YADAMU_HOME=/usr/src/YADAMU
 ---> Running in 9ae3d9f46576
Removing intermediate container 9ae3d9f46576
 ---> fee64f35e126
Step 6/10 : ENV YADAMU_BIN=$YADAMU_HOME/bin
 ---> Running in 185bd297c97c
Removing intermediate container 185bd297c97c
 ---> f08b5b50b5fd
Step 7/10 : ENV YADAMU_SRC=$YADAMU_HOME/src
 ---> Running in 2bbd9a20dd8c
Removing intermediate container 2bbd9a20dd8c
 ---> 58b3bedc18e2
Step 8/10 : ENV PATH=$PATH:$YADAMU_BIN
 ---> Running in 1fad141413ef
Removing intermediate container 1fad141413ef
 ---> 253ea5d387e4
Step 9/10 : RUN cd bin  && chmod +x $YADAMU_BIN/export.sh  &&   ln -s $YADAMU_BIN/export.sh yadamuExport  && chmod +x $YADAMU_BIN/import.sh  && ln -s $YADAMU_BIN/import.sh yadamuImport  && chmod +x $YADAMU_BIN/upload.sh  && ln -s $YADAMU_BIN/upload.sh yadamuUpload  && chmod +x $YADAMU_BIN/copy.sh  &&   ln -s $YADAMU_BIN/copy.sh yadamuCopy  && chmod +x $YADAMU_BIN/load.sh  && ln -s $YADAMU_BIN/load.sh yadamuLoad  && chmod +x $YADAMU_BIN/unload.sh  &&     ln -s $YADAMU_BIN/unload.sh yadamuUnload  && chmod +x $YADAMU_BIN/yadamu.sh  && ln -s $YADAMU_BIN/yadamu.sh yadamu
 ---> Running in 74f59f9a3274
Removing intermediate container 74f59f9a3274
 ---> 16db4a7b945e
Step 10/10 : ENTRYPOINT ["sleep","365d"]
 ---> Running in 7fdfb7f05b39
Removing intermediate container 7fdfb7f05b39
 ---> 574bb0f5a49e
Successfully built 574bb0f5a49e
Successfully tagged yadamu/docker:latest
SECURITY WARNING: You are building a Docker image from Windows against a non-Windows Docker host. All files and directories added to build context will have '-rwxr-xr-x' permissions. It is recommended to double check and reset permissions for sensitive files and directories.

```

Create a docker volume to manage your output:
```
docker volume create YADAMU-MNT-01
```

Start Runtime
```
docker run --name RUNTIME-01 --memory="4g" --network YADAMU-NET -d -vYADAMU_01_MNT:/usr/src/YADAMU/mnt  yadamu/docker:latest
```
Connect to the runtime. 
```
docker exec -it RUNTIME-01 /bin/bash
```

The docker environment supports the following commands

1. **yadamuExport**: Copy tables from a database schema to a single JSON dump file.
2. **yadamuImport**: Copy tables from a single JSON dump file to a database schema
3. **yadamuUnload**: Copy tables from a database schema to a set of JSON dump files. One file is generated for each table.
4. **yadamuLoad**: Copy a set of JSON dump files into a database schema
5. **yadamuCopy**: Copy a database schema directly between a source and target database.

Optionally if you want to be able to access the output directory from a windows development environment 

Create a Docker SMB Server
```
docker run --name YADAMU-SMB -m 512m -it -p 139:139 -p 445:445 -p 137:137/udp -p 138:138/udp -vYADAMU_01_MNT:/mount -d dperson/samba -p -s "work;/mount;yes;no;yes" -n
```
Map the shared volume to a windows drive (assuming you want to map to drive "Y" and your docker host is called "docker-host":
```
net use y: \\docker-host\work
```



    









