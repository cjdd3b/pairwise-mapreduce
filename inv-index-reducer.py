#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator):
    """
    Generator to limit memory usage while reading input.
    """
    for line in file:
        yield line.rstrip().split(separator, 2)

def main(separator='\t'):
    """
    Consolidates input from mapper into an inverted index with term weights.
    More info here: http://en.wikipedia.org/wiki/Inverted_index
    
    Input:

    this     1       1
    document 1       1
    document 2       1
    ...
    word    docidx   count
    
    Output:
    
    this        {"1": 1}
    document    {"1": 1, "2", 1}
    ...
    term        {"docidx": count, "docidy": count1 ...}
    """
    data = read_mapper_output(sys.stdin, separator)
    
    # Input from the mapper is sorted by key by map/reduce. This groups the
    # input by key and then consolidates the values.
    for current_word, group in groupby(data, itemgetter(0)):
        # Creates a dict-like list of docids and term counts in string form, for
        # output by the reducer.
        fileList = []
        for current_word, fileName, count in group:
            fileList.append('"%s": %s' % (fileName, count))
        # Print inverted index to stdout
        print "%s\t{%s}" % (current_word, ','.join(fileList))

if __name__ == "__main__":
    main()