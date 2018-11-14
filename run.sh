#!/bin/bash

hdfs dfs -rm -r -f /user/wennyyustalim/output

rm Triangle*.class
hadoop com.sun.tools.javac.Main TriangleCount.java
jar cf triangle.jar Triangle*.class

hadoop jar triangle.jar TriangleCount /user/wennyyustalim/input /user/wennyyustalim/output