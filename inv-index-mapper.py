#!/usr/bin/python

import sys

def read_mapper_input(stdin):
    """
    Generator to limit memory usage while reading input.
    """
    for line in stdin:
        yield line.rstrip()

def main():
    """
    Takes input as a document ID and string of words and returns a list of term
    counts, which will be consolidated into a true inverted index in the reducer.
    
    Input:
    
    docid|This is some document text
    docid2|And this is another document
    
    Output:
    
    this     1       1
    document 1       1
    document 2       1
    ...
    word    docidx   count 
    """
    for line in read_mapper_input(sys.stdin):
        # Split document ID and document string
        docid = line.split('|')[0]
        document = line.split('|')[1]
        
        frequencies = {}
        # Crudely tokenize document into words and tally up word counts. This
        # works better if preprocessing strips punctuation, removes stopwords,
        # performs stemming, etc.
        for word in document.split():
            try:
                frequencies[word] += 1
            except KeyError:
                frequencies[word] = 1
        
        # Print word frequencies to stdout for ingestion by reducer.
        for word in frequencies:
            print '%s\t%s\t%s' % (word, docid, frequencies[word])

if __name__ == "__main__":
    main()