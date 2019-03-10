from pyspark import SparkContext, SparkConf,sql
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.sql import functions as F
from datetime import datetime
import time
import mysql.connector
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/adityaallamraju/hadoop-install/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'
import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/arpit.maheshwari/Downloads/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'

sys.path.append("/Users/arpit.maheshwari/Downloads/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar")
conf = SparkConf()\
    .setAppName("Ingest Data to Data zalora catalog")\
  #  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

sc = SparkContext(conf=conf)
sqlContext =SQLContext(sc)



cnx = mysql.connector.connect(user='guest', password='relational',
                              host='relational.fit.cvut.cz',
                              database='northwind')
cnx.close()

