import time
from collections import defaultdict
import numpy as np

from cython_examples import cython_process_batch
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

def process_batch(batch, num_reducers):

    #[
    # (timestamp, "<word> <word> ... <word>"),
    # ...
    # ]


    timestamp = batch[0][0]
    timestamped = set(batch[0][1].split(b' '))

    counts = defaultdict(int)
    for _, row in batch:
        for word in row.split(b' '):
            counts[word] += 1

    keyed_counts = {
            key: [] for key in range(num_reducers)
            }
    for word, count in counts.items():
        keyed_counts[partition[word]].append((timestamp if word in timestamped else 0, (word, count)))
    #{
    #  reducer key: [
    #     (timestamp, (word, count)),
    #     ...
    #     ]
    # }

    return keyed_counts

start = time.time()
process_batch(batch, num_reducers)
end = time.time()
print("Took", end - start)

start = time.time()
objects = []
yep.start('/tmp/test.prof')
for _ in range(50):
    d = cython_process_batch(batch, num_reducers)
    objects.append(d)
yep.stop()
#d = cython_process_batch(batch, num_reducers)
end = time.time()
print("CYTHON: Took", end - start)
