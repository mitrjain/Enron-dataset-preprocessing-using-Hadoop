# Enron-dataset-preprocessing-using-Spark

This repository contains the source code of the Email preprocessor used to preprocess/clean the *structured form* of raw Enron email dataset. 
The dataset is avaiable to download from [here](https://www.cs.cmu.edu/~enron/)

The raw dataset downloaded from the above website is in an unstructured form. This email preprocessor requires the input data to be in a structured from.

## Code organization/ Directory structure
All executable code is present in the /executable directory.
- `execute.sh` : Driver program - shell script used to run the email preprocessor
- `preprocess.py` : contains required code that utilizes the pyspark library to perform data preprocessing 

## Pre-requisites

### Input - Structured dataset(csv file)
The structured format here is a csv file, where each row coressponds to an email. Each row has two columns:
-  id
- message

Following is the format of a single row of the csv file:

`"{unique id of this message}","{raw email text}"`

*Note: Every doublequote(") occuring inside the raw email text must be escapaed by replacing the single doublequote (") with a double doublequote ("")*

You can write your own code to convert the unstructured dataset to a structured dataset or refer to [convertEnronToCsv](https://github.com/mitrjain/convertEnronToCsv)

## Running the Email preprocessor
### Understanding flow of operations
This application takes an input csv file and produces two output csv files. 
- The first output csv file consists of cleaned email bodies 

`CSV format: <from,cleanEmailBody>`

- The second outout csv file consists of cleaned email bodies grouped by the sender email id. All the email bodies belonging to the same user are concatenated into one single row. 

`CSV format: <from,cleanEmailBody>`

This application requires 4 input parameters:
- sparkSessionName: An identifier of your spark session
- hdfInputCsv: full hdfs path (including hdfs namenode address) of the structured data csv file.
- hdfsOutputCsvUnGrouped: full hdfs path (including hdfs namenode address) of the directory where the application should place the csv file containing un-grouped email bodies
- hdfsOutputCsvGrouped: full hdfs path (including hdfs namenode address) of the directory where the application should place the csv file containing email bodies grouped by sender email id.

*Note All hdfs based paths would look like "hdfs://{namenode-address}/{path_on_hdfs}"*

### Steps to run the application
- Navigate to the /executable direcotr
- execute the follwing command

`./execute.sh {sparkSessionName} {hdfInputCsv} {hdfsOutputCsvUnGrouped} {hdfsOutputCsvGrouped}`

## Extending the Email-preprocessor
The preprocessing/cleaning prowess of this application can be extended by placing the desired code inside (at the end) the clean() present in the /executable/preprocess.py file.





