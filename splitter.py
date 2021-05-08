import glob
import gzip

path = "chunk_*.tsv"

chunksize = 1_000_000
fid = 1
lines = []

with gzip.open('hash_catid_count.csv.gz', 'rb') as f_in:
    f_out = gzip.open('chunk_{}.csv.gz'.format(fid), 'wb')
    for line_num, line in enumerate(f_in, 1):
        lines.append(line)
        if not line_num % chunksize:
            lines = sorted(lines, key=lambda k: int(k.split()[0]))
            f_out.writelines(lines)

            print('splitting', fid)
            f_out.close()
            lines = []
            fid += 1
            f_out = gzip.open('chunk_{}.csv.gz'.format(fid), 'wb')

    # last chunk
    if lines:
        print('splitting', fid)
        lines = sorted(lines, key=lambda k: int(k.split()[0]))
        f_out.writelines(lines)
        f_out.close()
        lines = []