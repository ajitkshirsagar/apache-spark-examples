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
bash-4.4# pyspark
Python 3.6.9 (default, Jul 19 2020, 03:46:11)
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
20/08/30 09:51:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.0.0
      /_/

Using Python version 3.6.9 (default, Jul 19 2020 03:46:11)
SparkSession available as 'spark'.
>>> df=spark.createDataFrame([(1,), (2,), (3,)], ['id'])
>>> df.show()
+---+
| id|
+---+
|  1|
|  2|
|  3|
+---+

>>>
```
