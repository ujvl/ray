import time
from collections import defaultdict
import numpy as np

from cython_examples import cython_process_batch, cython_process_batch2
import yep


batch_size = 1000
sentence_length = 100

words = []
with open('words.txt', 'r') as f:
    for line in f.readlines():
        words.append(line.strip().encode('ascii'))
words = np.array(words)

num_reducers = 1
partition = {
        word: hash(word) % num_reducers for word in words
        }

batch = [(0, np.string_.join(b' ', np.random.choice(words, sentence_length))) for _ in range(batch_size)]
batch[0] = (time.time(), batch[0][1])

with open('out', 'wb+') as f:
    for _, row in batch:
        f.write(row)
        f.write(b'\n')

def process_batch(batch, num_reducers):
    keyed_counts = {
            key: [] for key in range(num_reducers)
            }
    for timestamp, row in batch:
        for word in row.split(b' '):
            keyed_counts[hash(word) % num_reducers].append((timestamp, (word, 1)))

    return keyed_counts


start = time.time()
D = process_batch(batch, num_reducers)
print("TIMESTAMP", D[0][0][0])
end = time.time()
print("Took", end - start)

start = time.time()
objects = []
# yep.start('/tmp/test.prof')
# for _ in range(50):
#     d = cython_process_batch(batch, num_reducers)
#     objects.append(d)
# yep.stop()
d = cython_process_batch(batch, num_reducers)
end = time.time()
print("CYTHON: Took", end - start, type(d))

import gc
gc.disable()
start = time.time()
d = cython_process_batch(batch, num_reducers)
print("CYTHON TIMESTAMP", d[0][0][0])
end = time.time()
print("CYTHON (no gc): Took", end - start, type(d))

start = time.time()
a, b = cython_process_batch2(batch, num_reducers)
end = time.time()
print("CYTHON2: Took", end - start, type(b))
