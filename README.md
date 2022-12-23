# Enron-dataset-preprocessing-using-Spark

This repository contains the source code of the Email preprocessor used to preprocess/clean the *structured form* of raw Enron email dataset. 
The dataset is avaiable to download from [here](https://www.cs.cmu.edu/~enron/)

The raw dataset downloaded from the above website is in an unstructured form. This email preprocessor requires the input data to be in a structured from.

## Code organization/ Directory structure

## Pre-requisites

### Input - Structured dataset(csv file)
The structured format here is a csv file, where each row coressponds to an email. Each row has two columns:
-  id
- message

Following is the format of a single row of the csv file:

`"{unique id of this message}","{raw email text}"`

*Note: Every doublequote(") occuring inside the raw email text must be escapaed by replacing the single doublequote (") with a double doublequote ("")*

You can write your own code to convert the unstructured dataset to a structured dataset or refer to the 

### Hadoop streaming jar
In order to run python code in the hadoop ecosystem, we need to downlaod the (hadoop straming jar)[https://jar-download.com/artifacts/org.apache.hadoop/hadoop-streaming]. The appropriate version depends on the hadoop version installed in your environment (local/cloud).

## Running the Email preprocessor


## Extending the Email-preprocessor




