#!/bin/bash

hdfs dfs -rm -r -f /user/wennyyustalim/output
hdfs dfs -rm -r -f /user/wennyyustalim/temp/first-mapreduce
rm triplet.jar
rm ClosedTriplet*.class

hadoop com.sun.tools.javac.Main ClosedTripletCount.java
jar cf triplet.jar ClosedTripletCount*.class

hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input /user/wennyyustalim/output