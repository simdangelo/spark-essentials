This repository contains the code I developed while following Daniel Ciocîrlan's online course **Spark Essentials with Scala**, step by step. The course provided comprehensive guidance on Spark using Scala API, and this code serves as a practical implementation of the concepts covered. 

The following are the topics covered in the course I wrote the code for:
* DataFrames Basics
* DataFrame API
* Data Sources (json, csv, parquet, DB)
* DataFrame Aggregations
* DataFrame Joins
* DataSets
* The Spark SQL Shell
* How Spark runs on a Cluster
* Deploying Spark Applications on a Cluster


# The official repository for the Rock the JVM Spark Essentials with Scala course

This repository contains the code we wrote during  [Rock the JVM's Spark Essentials with Scala](https://rockthejvm.com/course/spark-essentials) (Udemy version [here](https://udemy.com/spark-essentials)) Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

## How to install

- install [Docker](https://docker.com)
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- Windows users, you need to set up some Hadoop-related configs - use [this guide](/HadoopWindowsUserSetup.md)
- in a terminal window, navigate to the folder where you downloaded this repo and run `docker-compose up` to build and start the PostgreSQL container - we will interact with it from Spark
- in another terminal window, navigate to `spark-cluster/` 
- Linux/Mac users: build the Docker-based Spark cluster with
```
chmod +x build-images.sh
./build-images.sh
```
- Windows users: build the Docker-based Spark cluster with
```
build-images.bat
```
- when prompted to start the Spark cluster, go to the `spark-cluster` directory and run `docker-compose up --scale spark-worker=3` to spin up the Spark containers with 3 worker nodes

### How to start

Clone this repository and checkout the `start` tag by running the following in the repo folder:

```
git checkout start
```

### How to see the final code

Udemy students: checkout the `udemy` branch of the repo:
```
git checkout udemy
```

Rock the JVM students: checkout the master branch:
```
git checkout master
```

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
