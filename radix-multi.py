import json
import logging
import math
import os
import sys
import time
import gzip
import glob
from heapq import merge

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def main(big_filepath):
    state_filepath = os.path.abspath(".radixsort.state.json")
    if not os.path.isfile(state_filepath):
        with open(state_filepath, "w") as fp:
            fp.write(json.dumps({"finished_stages": ["setup"]}))
    state = get_state(state_filepath)
    state["filepath"] = state_filepath
    if "target" not in state:
        state["target"] = os.path.abspath("out.txt")
    if "meta" not in state:
        state["meta"] = {}  # stage => time
    if "chunk_data" not in state["finished_stages"]:
        t0 = time.time()
        target_dir = "radixsort_tmp"
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        prefixes = get_prefixes()
        state = chunk_data(state, big_filepath, prefixes)
        t1 = time.time()
        state["meta"]["chunk_data"] = t1 - t0
        print("Time for Chunking: {:0.1f}s".format(t1 - t0))
        write_state(state["filepath"], state)
    if "sort_chunks" not in state["finished_stages"]:
        t0 = time.time()
        state = sort_chunks(state)
        t1 = time.time()
        state["meta"]["sort_chunks"] = t1 - t0
        print("Sort chunks: {:0.1f}s".format(t1 - t0))
        state["finished_stages"].append("sort_chunks")
        write_state(state["filepath"], state)
    if "merge_data" not in state["finished_stages"]:
        t0 = time.time()
        state = merge_chunks(state)
        t1 = time.time()
        state["meta"]["merge_data"] = t1 - t0
        print("Merged chunks: {:0.1f}s".format(t1 - t0))
        state["finished_stages"].append("merge_data")
        write_state(state["filepath"], state)
    print("Done!")


def get_state(state_filepath):
    with open(state_filepath) as fp:
        state = json.loads(fp.read())
    return state


def write_state(state_filepath, state):
    with open(state_filepath, "w") as fp:
        fp.write(json.dumps(state))


def get_range(big_filepath):
    min_val, max_val = None, None
    i = 0
    with gzip.open(big_filepath, 'rb') as fp:
        line = fp.readline()
        min_val = line.strip()
        max_val = line.strip()
        for line in fp:
            if i % 10000000 == 0:
                logger.info("    i={:,}".fromat(i))
            line = line.strip()
            min_val = min(min_val, line)
            max_val = max(max_val, line)
            i += 1
    return min_val, max_val, i


def get_prefixes():
    """Those are the characters the number starts with."""
    return [str(i) for i in range(1, 10)]


def chunk_data(state, big_filepath, prefixes):
    """
    Sort the numbers into files which are roughly 216MB big.
    Each file is defined by its prefix.
    This takes 305,29s on my machine.
    """
    prefix2file = {}
    chunks_to_sort = []
    for prefix in prefixes:
        chunk = os.path.abspath(f"radixsort_tmp/{prefix}.txt")
        chunks_to_sort.append(chunk)
        prefix2file[prefix] = open(chunk, "wt")
    logger.info("Generated files")
    logger.info("Start splitting...")
    with gzip.open(big_filepath, 'rt') as fp:
        for line in fp:
            try:
                prefix2file[line[:1]].writelines(line)
            except:
                print(line[:1])
    logger.info("Done splitting...")
    state["finished_stages"].append("chunk_data")
    state["chunks_to_sort"] = chunks_to_sort
    state["chunks_to_merge"] = chunks_to_sort[:]
    return state


def sort_chunks(state):
    from multiprocessing import Pool

    pool = Pool(processes=8)
    pool.map(sort_chunk, state["chunks_to_sort"])
    return state


def sort_chunk(chunk_path):
    """Reading, sorting, writing... takes about 8-10s per 216MB."""
    with open(chunk_path) as fp:
        lines = fp.readlines()
    lines = sorted(lines)
    with open(chunk_path, "w") as fp:
        fp.writelines(lines)


def merge_chunks(state):
    state["chunks_to_merge"] = sorted(state["chunks_to_merge"])
    state["written_lines"] = 0
    write_state(state["filepath"], state)
    with open(state["target"], "w") as fp:
        t0 = time.time()
        while state["chunks_to_merge"]:
            chunk_filepath = state["chunks_to_merge"].pop(0)
            chunks=[]
            for filename in glob.glob(chunk_filepath):
                print(filename)
                chunks += [open(filename, 'r')]
            print(chunks)
            with open('sorted.txt', 'w') as f_out:
                f_out.writelines(merge(*chunks, key=lambda k: int(k.split()[0])))
        t1 = time.time()
        print("Written {:}: {:0.1f}".format(chunk_filepath, t1 - t0))
        write_state(state["filepath"], state)
    return state


if __name__ == "__main__":
    main("hash_catid_count.csv.gz")