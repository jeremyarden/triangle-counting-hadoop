#!/bin/bash

#### Delete intermediate and output files ####

hdfs dfs -rm -r -f /user/wennyyustalim/output
# hdfs dfs -rm -r -f /user/wennyyustalim/output1
# hdfs dfs -rm -r -f /user/wennyyustalim/output2
# hdfs dfs -rm -r -f /user/wennyyustalim/output_tc1
# hdfs dfs -rm -r -f /user/wennyyustalim/output_tc2
# hdfs dfs -rm -r -f /user/wennyyustalim/output_tc3
hdfs dfs -rm -r -f /user/wennyyustalim/temp/first-mapreduce
hdfs dfs -rm -r -f /user/wennyyustalim/temp/second-mapreduce
rm triplet.jar
rm ClosedTriplet*.class

#### Compile ####
hadoop com.sun.tools.javac.Main ClosedTripletCount.java
jar cf triplet.jar ClosedTripletCount*.class

#### Execute ####
hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input /user/wennyyustalim/output
hdfs dfs -cat /user/wennyyustalim/output/*

# hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input2 /user/wennyyustalim/output2
# hdfs dfs -cat /user/wennyyustalim/output2/*

# hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input3 /user/wennyyustalim/output3
# hdfs dfs -cat /user/wennyyustalim/output3/*

# hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input_tc1 /user/wennyyustalim/output_tc1
# hdfs dfs -cat /user/wennyyustalim/output_tc1/*

# hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input_tc2 /user/wennyyustalim/output_tc2
# hdfs dfs -cat /user/wennyyustalim/output_tc2/*

# hadoop jar triplet.jar ClosedTripletCount /user/wennyyustalim/input_tc3 /user/wennyyustalim/output_tc3
# hdfs dfs -cat /user/wennyyustalim/output_tc3/*