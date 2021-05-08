from heapq import merge
import gzip
import glob, os
chunks = []
for filename in glob.glob('chunk_*.csv.gz'):
    chunks += [gzip.open(filename, 'rb')]

with gzip.open('sorted.csv.gz', 'w') as f_out:
    f_out.writelines(merge(*chunks, key=lambda k: int(k.split()[0])))