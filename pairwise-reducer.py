#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator):
    """
    Generator that yields lines from the mapper.
    """
    for line in file:
        yield line.rstrip().split(separator, 2)

def main(separator='\t'):
    """
    Consolidates output from the mapper and sums document pair similarity scores to
    calculate a final similarity score for each document pair. Input comes as key/
    value with key being a document pair and value being the product of term weights
    therein.
    
    Input:
    
    "docid1|docid2"        10
    "docid1|docid2"        5
    ...
    "docidx|docidy"        weight_product
    
    Output:
    
    "docid1|docid2"        15
    ...
    "docidx|docidy"        weight_sum
    """
    data = read_mapper_output(sys.stdin, separator)
    
    # Input from the mapper is sorted by key by map/reduce. This groups the
    # input by key and then consolidates the values.
    for docset, group in groupby(data, itemgetter(0)):
        totcount = 0
        for docset_inner, count in group:
            totcount += int(count)
        print "%s\t%s" % (docset, totcount)

if __name__ == "__main__":
    main()