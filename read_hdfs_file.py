#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Surendra Marupudi"

"""
  This script takes the HDFS file and one word as command line inputs and count the number lines contains the given word and print 
  the count at the end.

  How to run this script:
  spark-submit read_hdfs_file.py 'HDFS file path' 'wrod to search in the document given'
  Example: spark-submit  read_hdfs_file.py hdfs://localhost:54310/user/README.txt use
"""

from pyspark import SparkContext
import sys, getopt

" " " 
    Check number of command line arguments. If the number of arguments are not equla to 3 then output the error message and exit
    and if the number of arguments are 3 then read the command line arguments.
" " "

if len(sys.argv) != 3:
   print "Usage : read_hdfs_file.py input_file 'word to search'"
   sys.exit(2)
else :
   input = sys.argv[1]
   word = sys.argv[2]
   
#Initializing Spark context

sc = SparkContext("local", "HDFS Read App")

#Reading the input file from hdfs.
inputData = sc.textFile(input).cache()

#count the number of lines contain word given.
linesCount = inputData.filter(lambda line: word in line).count()

#printing the stas for give word.
print "Lines with %s: %i" % (word, linesCount)
