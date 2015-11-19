# Apache-Pig---Median-UDF
An implementation of a median estimor for Apache Pig

This file contains an Apache Pig UDF for implementing the function MEDIAN. MEDIAN finds the true median of a bag of values of size < 1000. For larger bags, MEDIAN uses the algorithm described <a href="http://www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf">here</a> to estimate the median.





