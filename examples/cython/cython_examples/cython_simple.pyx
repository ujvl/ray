#!python
# cython: profile=True
# cython: linetrace=True
# cython: embedsignature=True, binding=True
# distutils: language = c++
# cython: language_level = 3
# distutils: define_macros=CYTHON_TRACE_NOGIL=1

from libcpp.unordered_map cimport unordered_map
from libcpp.pair cimport pair
from libcpp.string cimport string as c_string
from libcpp.unordered_set cimport unordered_set
from libcpp.list cimport list
from libcpp.vector cimport vector
from collections import defaultdict

from cython.operator import dereference, postincrement

import yep
import time

def simple_blahfunc(x, y, z):
    return x + y + z


# Cython code directly callable from Python
def fib(n):
    if n < 2:
        return n
    return fib(n-2) + fib(n-1)


# Typed Cython code
def fib_int(int n):
    if n < 2:
        return n
    return fib_int(n-2) + fib_int(n-1)


# Cython-Python code
cpdef fib_cpdef(int n):
    if n < 2:
        return n
    return fib_cpdef(n-2) + fib_cpdef(n-1)


# C code
def fib_cdef(int n):
    return fib_in_c(n)


cdef int fib_in_c(int n):
    if n < 2:
        return n
    return fib_in_c(n-2) + fib_in_c(n-1)


# Simple class
class simple_class(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

#def cython_process_batch(batch, num_reducers):
#    timestamp = batch[0][0]
#    timestamped = set(batch[0][1].split(b' '))
#
#    counts = defaultdict(int)
#    for _, row in batch:
#        for word in row.split(b' '):
#            counts[word] += 1
#
#    keyed_counts = {
#            key: [] for key in range(num_reducers)
#            }
#    for word, count in counts.items():
#        keyed_counts[hash(word) % num_reducers].append((timestamp if word in timestamped else 0, (word, count)))
#    return keyed_counts

def cython_process_batch(list[pair[float, c_string]] batch, int num_reducers):

    cdef:
        unordered_map[c_string, int] counts
        unordered_set[c_string] timestamped
        #list[c_string] line
        c_string row
        size_t start = 0
        size_t end = 0
        c_string word
    #for w in batch[0][1].split(b' '):
    #    timestamped.insert(w)

    #for _, row in batch:
    #    end = row.find(b' ');
    #    while (end != row.size()):
    #        word = row.substr(start, end)
    #        start = end + 1
    #        end = row.find(b' ', start)
    #        counts[word] += 1
        #for w in row.split(b' '):
        #    counts[w] += 1

    #keyed_counts = {
    #        key: [] for key in range(num_reducers)
    #        }
    cdef:
        #unordered_map[c_string, int].iterator it = counts.begin()
        unordered_map[int, list[pair[float, pair[c_string, int]]]] keyed_counts
        int count
        int h
        int i
        float timestamp

    #while it != counts.end():
    #    word = dereference(it).first
    #    count = dereference(it).second
    #    h = hash(word)
    #    keyed_counts[h % num_reducers][word] = (timestamp if timestamped.count(word) == 1 else 0, count)
    #    postincrement(it)

    #timestamp = batch[0][0]
    start = time.time()
    cdef:
        list[pair[float, c_string]].iterator it = batch.begin()
    while it != batch.end():
        timestamp = dereference(it).first
        row = dereference(it).second
        end = row.find(b' ');
        while (end < row.size()):
            word = row.substr(start, end)
            #counts[word] += 1
            h = hash(word)
            h = h % num_reducers
            keyed_counts[h].push_back((timestamp, (word, 1)))

            start = end + 1
            end = row.find(b' ', start)
        postincrement(it)

    return time.time() - start
