# hadoop-exercise
An exercise of hadoop

## Dependencies
- [Apache Hadoop Common](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common)
- [Apache Hadoop MapReduce Core](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core)

## Description
This is a simple twisted version of word count in MapReduce example code. It created an word count version of inverted index on several txt files with 380MB total size. 

The output file is in this format
    
    [word]: ([file name]:[count])+

The code is tested on GCP and sample output is in index folder
