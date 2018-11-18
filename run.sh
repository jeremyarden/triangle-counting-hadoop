#!/bin/bash

hdfs dfs -rm -r -f /user/wennyyustalim/output
hdfs dfs -rm -r -f /user/wennyyustalim/temp/first-mapreduce
hdfs dfs -rm -r -f /user/wennyyustalim/temp/second-mapreduce

rm triplet.jar
rm ClosedTriplet*.class

hadoop com.sun.tools.javac.Main ClosedTripletCount.java
jar cf triplet.jar ClosedTripletCount*.class

#hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input /user/wennyyustalim/output
#hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input2 /user/wennyyustalim/output2
hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input3 /user/wennyyustalim/output3
