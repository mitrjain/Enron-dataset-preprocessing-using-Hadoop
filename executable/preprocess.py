#!/usr/bin/env python

from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
import sys
import re
import email

sparkSessionName = sys.argv[1]
hdfInputCsv = sys.argv[2]
hdfsOutputCsvGrouped = sys.argv[3]
hdfsOutputCsvUnGrouped = sys.argv[4]

# creating sparksession; entry point to creating Dataframes and using pyspark functionality
print(sparkSessionName,": Establishing spark session")
spark = SparkSession.builder.appName(sparkSessionName).getOrCreate()
print(sparkSessionName,": Spark session created")
# define schema of our dataframe
schema = StructType().add("id",IntegerType(),False).add("message",StringType(),True)       

# reading structured data from csv file into the data frame
print(sparkSessionName,": Reading data from csv file - ",hdfInputCsv," - into spark dataframe")
df = spark.read.format("csv").option("header", True).option("quote", "\"").option("escape","\"").option("multiline",True).load(hdfInputCsv)
print(sparkSessionName,": Dataframe loaded with ",df.count()," emails")
df.show()

# creating a User Defined Function(UDF) to extract sender email id and raw email body from raw email message
def getData(message):
    global df
    e = email.message_from_string(message)
    sender = e.get('From')
    body = e.get_payload()
    return sender, body

schema = StructType([  
    StructField("from", StringType(), False),
    StructField("rawEmailBody", StringType(), False)  
])

getDataUDF = udf(getData,schema)

# extracting sender email id and raw email id from the 'message' column
# creating coressponding new columns in dataframe - 'from' and 'rawEmailBody'
print(sparkSessionName,": Extracting sender id and raw email body")
df=df.withColumn("extracted", getDataUDF(df.message)).select(col("id"),col("extracted.*"))
print(sparkSessionName,": Extraction complete")
df.show()


# creating a UDF to clean raw email body. 
def clean(rawEmailBody):
    #converting everything to lowercase
    x=rawEmailBody.lower()

    # removing headers within body in case its a chain of emails
    x =re.sub(r"(message\-id)|(mime\-version)|(content\-type)|(content\-transfer\-encoding)|(x\-from)|x(\-to)|(x\-cc)|(x\-bcc)|(x\-origin)|(x\-filename)|(x\-folder)"," ",x)

    #removing email ids
    x = re.sub(r"\w+(\.|-)*\w*@[^\s]*\.(com|de|uk|edu\.au|au|co)", "", x)

    #removing urls

    #removing currency symbols
    x = re.sub(r"\$\€\£"," ", x)

    # removing punctuations
    x = re.sub('[\\.\\,\\:\\-\\!\\?\\n\\r\\t,\\%\\#\\*\\|\\=\\(\\)\\\"\\>\\<\\/\\`\\&\\{\\}\\;\\+\\-\\[\\]\\_\\/\\\,]',' ',x)

    # removing single quotation marks
    x = re.sub(r"\'","", x)

    # removing digits
    x=re.sub(r'\d+',' ',x)

    #If anything other than letters or space is left we remove it
    x=re.sub(r"[^a-z\s]+"," ",x)

    # If spaces occuring more than two times replace that sequence with single space
    x = re.sub(r"\s{2,}"," ",x)

# Additional functional calls or regular expression operations to further clean the data 
# and hence extend the functionality of this email preprocessor must be placed below within this clean(). 
# Any additional operation/functional call must return a string back and stored in the variable 'x'.

    return x

cleanUDF = udf(lambda z: clean(z),StringType())

# Performing cleaning transformation on the dataframe.
print(sparkSessionName,": Cleaning raw email bodies")
df = df.withColumn("cleanEmailBody",cleanUDF(col("rawEmailBody")))
print(sparkSessionName,": Cleaning complete")

# Create a new dataframe by dropping the columns which are no longer required.
df2=df.drop("rawEmailBody","id")
df2.show()

# Create another new dataframe where we combine all the cleaned email bodies belonging to the same user(email id) into one single row.
import pyspark.sql.functions as f
df3=df2.groupby("from").agg((f.concat_ws(" ", f.collect_list(df2.cleanEmailBody)).alias("cleanEmailBodies")))
df3.show()


# Writing the newly created datframes to HDFS
print(sparkSessionName,": Writing the newly created datframes to HDFS")
df2.write.option("header",True).csv(hdfsOutputCsvGrouped)
df3.write.option("header",True).csv(hdfsOutputCsvUnGrouped)
print(sparkSessionName,": Writing complete")

