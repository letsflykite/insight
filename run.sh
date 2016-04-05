#!/usr/bin/env bash

# example of the run script for running the average_degree calculation

# I'll execute my programs, with the input directory tweet_input and output the files in the directory tweet_output
#python ./src/average_degree.py ./tweet_input/tweets.txt ./tweet_output/output.txt
#javac -cp ".;./src/jackson-databind-2.7.3.jar;./src/jackson-core-2.7.3.jar;./src/jackson-annotations-2.7.3.jar" src/AverageDegree.java
javac -cp ".:./src/jackson-databind-2.7.3.jar:./src/jackson-core-2.7.3.jar:./src/jackson-annotations-2.7.3.jar" src/AverageDegree.java
#java -cp "./;./src;./src/jackson-databind-2.7.3.jar;./src/jackson-core-2.7.3.jar;./src/jackson-annotations-2.7.3.jar" AverageDegree ./tweet_input/tweets.txt ./tweet_output/output.txt
java -cp "./:./src:./src/jackson-databind-2.7.3.jar:./src/jackson-core-2.7.3.jar:./src/jackson-annotations-2.7.3.jar" AverageDegree ./tweet_input/tweets.txt ./tweet_output/output.txt

