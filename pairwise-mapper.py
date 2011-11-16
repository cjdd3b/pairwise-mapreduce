#!/usr/bin/env python

import sys

def read_mapper_input(stdin):
    """
    Generator to limit memory usage while reading input.
    """
    for line in stdin:
        yield line.rstrip()

def combinations(iterable, r):
    """
    Implementation of itertools combinations method. Re-implemented here because
    of import issues in Amazon Elastic MapReduce. Was just easier to do this than
    bootstrap. More info here: http://docs.python.org/library/itertools.html#itertools.combinations
    
    Input/Output:
    
    combinations('ABCD', 2) --> AB AC AD BC BD CD
    combinations(range(4), 3) --> 012 013 023 123
    """
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = range(r)
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

def main():
    """
    Takes input from stdin. For my application, input was inverted index displayed.
    as key/values. Key was the index word, value was a string representing a
    dictionary of documents and weights:
    
    word        {"docid": weight}
    word2       {"docid1": weight1, "docid2": weight2, etc.}
    
    Output:
    
    "docid1|docid2"        10
    "docid1|docid2"        5
    ...
    "docidx|docidy"        weight_product
    """
    input = read_mapper_input(sys.stdin)
    for line in input:
        # Eval the dict in the input value. Input lines are tab-delimited between
        # key (the word) and value (the dict)
        worddict = eval(line.split('\t')[1])
        
        try:
            # Iterate over permutations of document pairs for a given word
            for c in combinations(worddict.keys(), 2):
                # Calculate the document/word weight, which in the paper example
                # is just the product of term frequency counts between documents.
                # This can be improved with an algorithm like tfidf, BM25, etc.
                number = worddict[c[0]] * worddict[c[1]]
                
                # Return output in the form of a document pair and weight for
                # a given word. These will later be combined in the reducer.
                print '"%s|%s"\t%d' % (c[0], c[1], number)
        except ValueError: # Pass if the word only appears in one document
            pass

if __name__ == "__main__":
    main()