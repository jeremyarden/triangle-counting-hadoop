# Hadoop Closed Triplet Counting

Count the number of triangles in a social network graph.

## Prerequisites

Input data should be a list of edges, denoted by

    <node> <node>

Create the input directory first, and put your input file in

    /user/<yourname>/input

The output will be in your HDFS directory

    /user/<yourname>/output

## How to Run

If you're using Google Cloud Dataproc with Hadoop clustered already set up,

Open bashrc.

    nano ~/.bashrc


Add this line at the bottom.

    export HADOOP_CLASSPATH=/usr/lib/jvm/java-8-openjdk-amd64/lib/tools.jar

Don't forget  to

    source ~/.bashrc

Run.

    ./run.sh

## Collaborator

Special thanks to Rei https://github.com/reinaldoignatius