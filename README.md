# Multi-Threading with Map-Reduce

App designed to find perfect powers and count the individual appearances within a set of files,
while using Multithreading in C and the Map-Reduce model.

# Implementation

I used the Map-Reduce model to parallelize the processing of input files. I dynamically split the files into multiple threads which parsed and checked which numbers greater than 0 are perfect powers (the Map operation), resulting in partial lists for each exponent. Then, I combined these partial lists (the Reduce operation) to obtain aggregated lists for each exponent. For each list obtained, I counted the unique values in parallel and wrote the results to some output files.

Kindly check the task file in order to understand the assignment better.

# How to Run
Usage: ./tema1 <mappers_count> <reducers_count> <in_file> <br>
Example: ./tema1 3 5 test0/test.txt

