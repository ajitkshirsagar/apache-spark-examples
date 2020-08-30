import sys
import os

# Set the path for spark installation
# this is the path where you downloaded and uncompressed the Spark download
# Using forward slashes on windows, \\ should work too.
os.environ['SPARK_HOME'] = "C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/"
# # # # This is the HADOOP_HOME DIR for the local mode where winutil utility is installed
os.environ['HADOOP_HOME'] = "C:/winutil/"

# try the import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
sc = SparkContext("local[*]")

employeeData = [("Jack",1000.0),("Bob",2000.0),("Carl",7000.0),("Ajit",8000.0)]
employeeRDD = sc.parallelize(employeeData)
dummyEmployee = ("dummy",0.0)
max_employee_salary = employeeRDD.fold(dummyEmployee,lambda acc,employee : employee if acc[1] < employee[1] else acc)

print("Max employee salary: {}".format(max_employee_salary))

num_rdd = sc.parallelize([2,3],2)
num_squared = num_rdd.fold(1,lambda a,b: b ** a)

print("Numbers after fold operation on RDD: {}".format(num_squared))
