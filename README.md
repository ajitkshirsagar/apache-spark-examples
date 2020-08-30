# apache-spark-examples

# PySpark Dockerfile
Dockerfile for running [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) on openjdk:8-alpine base image. Available @ [amksagar/pyspark](https://hub.docker.com/r/amksagar/pyspark/).

## Base image
* [openjdk:8-alpine](https://hub.docker.com/_/openjdk) 

## Installation
1. Install Docker.
2. Pull [amksagar/pyspark](https://hub.docker.com/r/amksagar/pyspark/).

## Build
Build and tag the image.
```
docker build -t pyspark --no-cache .
```

## Usage
**Run container** 
Run `pyspark` container instance in interactive mode to access cmd shell.
```
docker run -it --rm pyspark
```

**Run pyspark** 
Pyspark installed in working directory `/data`.
```
